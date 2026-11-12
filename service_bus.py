"""Receive ADO service-hook events through an identity-authenticated Azure queue."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from functools import partial
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, replace

import httpx

from src.security import SecurityPolicyError, positive_int

logger = logging.getLogger(__name__)
_MAX_STATUS_FAILURES = 5


class RuntimeStatusUnavailable(RuntimeError):
    """Release this receiver for recovery without settling its pending job."""


@dataclass(frozen=True)
class ServiceBusSettings:
    namespace: str
    queue: str
    max_body_bytes: int
    work_queue: str = "stlc-work-items"
    max_concurrent_work_items: int = 5

    @classmethod
    def from_env(cls) -> ServiceBusSettings | None:
        namespace = os.getenv("STLC_SERVICE_BUS_NAMESPACE", "").strip()
        queue = os.getenv("STLC_SERVICE_BUS_QUEUE", "").strip()
        if not namespace and not queue:
            return None
        if not re.fullmatch(r"[a-zA-Z0-9-]+\.servicebus\.windows\.net", namespace):
            raise SecurityPolicyError("STLC_SERVICE_BUS_NAMESPACE must be an Azure Service Bus hostname")
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._/-]*", queue):
            raise SecurityPolicyError("STLC_SERVICE_BUS_QUEUE must name a queue")
        work_queue = os.getenv("STLC_SERVICE_BUS_WORK_QUEUE", "stlc-work-items").strip()
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._/-]*", work_queue) or work_queue == queue:
            raise SecurityPolicyError("STLC_SERVICE_BUS_WORK_QUEUE must name a separate session queue")
        return cls(
            namespace,
            queue,
            min(
                positive_int("STLC_WEBHOOK_MAX_PAYLOAD_BYTES", 1048576),
                positive_int("ADO_WEBHOOK_MAX_PAYLOAD_BYTES", 1048576),
            ),
            work_queue,
            min(
                positive_int("STLC_SERVICE_BUS_MAX_CONCURRENT_WORK_ITEMS", 5),
                positive_int("STLC_RUNTIME_MAX_PENDING_RUNS", 32),
            ),
        )


def message_body(message, limit: int) -> bytes:
    """Bound the body before invoking the existing authenticated webhook parser."""
    from azure.servicebus.amqp import AmqpMessageBodyType

    if message.body_type != AmqpMessageBodyType.DATA:
        raise ValueError("unsupported_body_type")
    result = bytearray()
    for chunk in message.body:
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise ValueError("unsupported_body_type")
        if len(result) + len(chunk) > limit:
            raise ValueError("payload_too_large")
        result.extend(chunk)
    return bytes(result)


async def route_message(message, receiver, sender, settings: ServiceBusSettings) -> None:
    """Filter intake, then preserve arrival order in one session per canonical item."""
    from azure.servicebus import ServiceBusMessage
    from src.runtime import work_item_session_id
    from src.security import EventAuthorizationError
    from src.webhooks.azure_devops_webhook import (
        build_webhook_summary, get_webhook_config, work_item_reference_from_summary,
    )

    try:
        body = message_body(message, settings.max_body_bytes)
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("invalid_payload")
        summary, prompt = build_webhook_summary(payload, get_webhook_config())
        if not prompt:
            await receiver.complete_message(message)
            logger.info("Service Bus event ignored: no matching trigger")
            return
        item = work_item_reference_from_summary(summary)
        if item is None:
            raise ValueError("missing_work_item")
    except (ValueError, TypeError, AttributeError, EventAuthorizationError):
        await receiver.dead_letter_message(message, reason="WebhookRejected")
        logger.warning("Service Bus intake event rejected")
        return
    # ADO IDs are organization-wide. A project name versus its GUID in webhook
    # payloads must not create two broker sessions for the same native work item.
    session_id = work_item_session_id(replace(item, namespace=None))
    digest = hashlib.sha256(session_id.encode() + b"\0" + body).hexdigest()
    if not payload.get("id"):
        payload["id"] = "stlc-" + digest
        body = json.dumps(payload, separators=(",", ":")).encode()
    await sender.send_messages(ServiceBusMessage(
        body, session_id=session_id, message_id=digest, content_type="application/json",
    ))
    await receiver.complete_message(message)
    logger.info("Service Bus eligible event queued for its work item")


async def wait_for_run(reference: dict, *, on_status: Callable[[bool], None] | None = None) -> None:
    """Keep the broker session while an accepted job runs, including across retries."""
    from starlette.concurrency import run_in_threadpool
    from src.runtime import get_dispatcher

    try:
        handle = get_dispatcher().application.attach(reference)
    except Exception:
        if on_status:
            on_status(False)
        raise
    failures = 0
    while True:
        try:
            snapshot = await run_in_threadpool(handle.refresh)
        except Exception as exc:
            if on_status:
                on_status(False)
            failures += 1
            logger.warning("Queued runtime status unavailable: %s", type(exc).__name__)
            if failures >= _MAX_STATUS_FAILURES:
                raise RuntimeStatusUnavailable("Runtime status failed repeatedly; retaining the run reference") from exc
        else:
            failures = 0
            if on_status:
                on_status(True)
            if snapshot.status.terminal:
                logger.info("Queued runtime reached terminal status: %s", snapshot.status.value)
                return
        await asyncio.sleep(2)


async def deliver_message(message, receiver, client, secret: str, settings: ServiceBusSettings,
                          *, on_runtime_status: Callable[[bool], None] | None = None) -> int:
    """Hold the session until terminal runtime status; retain its run reference."""
    from src.webhooks.azure_devops_webhook import DEFAULT_PATH, SECRET_HEADER

    # Broker state survives ACA restarts. A redelivery must reattach its existing
    # job; a later event must also wait if the previous message expired into DLQ.
    wait = wait_for_run if on_runtime_status is None else partial(wait_for_run, on_status=on_runtime_status)
    stored = await receiver.session.get_state()
    previous = json.loads(bytes(stored)) if stored else None
    if previous:
        await wait(previous["run_ref"])
        if previous["sequence_number"] == message.sequence_number:
            await receiver.complete_message(message)
            await receiver.session.set_state(None)
            return 0
        await receiver.session.set_state(None)
    try:
        body = message_body(message, settings.max_body_bytes)
    except ValueError:
        await receiver.dead_letter_message(message, reason="InvalidWebhookBody")
        logger.warning("Service Bus event rejected: invalid body")
        return 0
    response = await client.post(
        DEFAULT_PATH,
        content=body,
        headers={SECRET_HEADER: secret, "Content-Type": "application/json"},
    )
    if 200 <= response.status_code < 300:
        reference = response.json().get("run_ref")
        if reference:
            await receiver.session.set_state(json.dumps({
                "sequence_number": message.sequence_number, "run_ref": reference,
            }))
            await wait(reference)
        await receiver.complete_message(message)
        if reference:
            await receiver.session.set_state(None)
        logger.info("Service Bus event accepted")
        return 0
    if response.status_code in {400, 403, 413, 422}:
        await receiver.dead_letter_message(message, reason="WebhookRejected")
        logger.warning("Service Bus event rejected: HTTP %s", response.status_code)
        return 0
    await receiver.abandon_message(message)
    logger.warning("Service Bus event will retry: HTTP %s", response.status_code)
    try:
        return max(5, min(60, int(response.headers.get("Retry-After", "5"))))
    except ValueError:
        return 5


async def consume_events(app, settings: ServiceBusSettings, secret: str) -> None:
    from azure.identity.aio import ManagedIdentityCredential
    from azure.servicebus import NEXT_AVAILABLE_SESSION, TransportType
    from azure.servicebus.aio import AutoLockRenewer, ServiceBusClient
    from azure.servicebus.exceptions import (
        MessageLockLostError, OperationTimeoutError, SessionCannotBeLockedError, SessionLockLostError,
    )

    router_ready = False
    workers_ready: set[int] = set()
    status_unavailable: set[int] = set()

    def update_readiness():
        app.state.service_bus_connected = (router_ready and not status_unavailable
                                           and len(workers_ready) == settings.max_concurrent_work_items)

    def runtime_status(number, available):
        if available:
            status_unavailable.discard(number)
        else:
            status_unavailable.add(number)
        update_readiness()

    async def renew_session(session):
        # Renew for the lifetime of the processing coroutine, with no job deadline.
        while True:
            await asyncio.sleep(15)
            await session.renew_lock()

    async def process_session(receiver, client, number):
        while messages := await receiver.receive_messages(max_message_count=1, max_wait_time=10):
            delay = await deliver_message(messages[0], receiver, client, secret, settings,
                                          on_runtime_status=partial(runtime_status, number))
            if delay:
                await asyncio.sleep(delay)

    async def work_items(service_bus, client, number):
        while True:
            try:
                async with service_bus.get_queue_receiver(
                    settings.work_queue, session_id=NEXT_AVAILABLE_SESSION,
                    max_wait_time=10, prefetch_count=0,
                ) as receiver:
                    workers_ready.add(number)
                    update_readiness()
                    processing = asyncio.create_task(process_session(receiver, client, number))
                    renewal = asyncio.create_task(renew_session(receiver.session))
                    try:
                        done, _ = await asyncio.wait((processing, renewal), return_when=asyncio.FIRST_COMPLETED)
                        for task in done:
                            task.result()
                    finally:
                        for task in (processing, renewal):
                            task.cancel()
                        await asyncio.gather(processing, renewal, return_exceptions=True)
            except (OperationTimeoutError, SessionCannotBeLockedError):
                # No available session is normal; the service accepted this receiver.
                workers_ready.add(number)
                update_readiness()
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                workers_ready.discard(number)
                update_readiness()
                logger.warning("Service Bus work-item receiver reconnecting: %s", type(exc).__name__)
                await asyncio.sleep(5)

    async def intake(service_bus):
        nonlocal router_ready
        while True:
            locked_message = None
            try:
                async with (
                    AutoLockRenewer(max_lock_renewal_duration=3600) as renewer,
                    service_bus.get_queue_receiver(settings.queue, max_wait_time=10, prefetch_count=0) as receiver,
                    service_bus.get_queue_sender(settings.work_queue) as sender,
                ):
                    router_ready = True
                    update_readiness()
                    logger.info("Service Bus intake receiver connected")
                    while True:
                        for message in await receiver.receive_messages(max_message_count=1, max_wait_time=10):
                            locked_message = message
                            renewer.register(receiver, message)
                            # Do not advance past this event on a temporary send failure.
                            while True:
                                try:
                                    await route_message(message, receiver, sender, settings)
                                    locked_message = None
                                    break
                                except (MessageLockLostError, SessionLockLostError):
                                    raise
                                except Exception as exc:
                                    router_ready = False
                                    update_readiness()
                                    logger.warning("Service Bus routing will retry: %s", type(exc).__name__)
                                    await asyncio.sleep(5)
                            router_ready = True
                            update_readiness()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                router_ready = False
                update_readiness()
                logger.warning("Service Bus intake reconnecting: %s", type(exc).__name__)
                # The SDK updates this timestamp after renewals. Wait for the
                # actual held lock, including queues configured above one minute.
                expiry = getattr(locked_message, "locked_until_utc", None)
                delay = max(5, (expiry - datetime.now(timezone.utc)).total_seconds() + 5) if expiry else 5
                await asyncio.sleep(delay)

    client_id = os.getenv("AZURE_CLIENT_ID", "").strip()
    async with (
        ManagedIdentityCredential(**({"client_id": client_id} if client_id else {})) as credential,
        ServiceBusClient(
            settings.namespace,
            credential,
            transport_type=TransportType.AmqpOverWebsocket,
            logging_enable=False,
        ) as service_bus,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://servicebus.internal",
        ) as client,
    ):
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(intake(service_bus))
                for number in range(settings.max_concurrent_work_items):
                    group.create_task(work_items(service_bus, client, number))
        finally:
            app.state.service_bus_connected = False


__all__ = ["ServiceBusSettings", "consume_events", "deliver_message"]
