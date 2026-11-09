"""CI-neutral Azure provisioning and release. Plans unless --apply is supplied.

Inputs and outputs contain configuration/identifiers only. Secret values are
created separately by the operator and consumed by workload identities.
"""
from __future__ import annotations

import argparse
import ast
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
EXTRA_ENV = {
    "ADO_AGENT_IDENTITY_IDS", "ADO_AGENT_IDENTITY_UNIQUE_NAMES", "ADO_AGENT_IDENTITY_DISPLAY_NAMES",
    "ADO_WEBHOOK_MENTION_HANDLE", "STLC_SERVICE_BUS_MAX_CONCURRENT_WORK_ITEMS",
    "ADO_WEBHOOK_MAX_PAYLOAD_BYTES",
    "STLC_WEBHOOK_PORT", "STLC_WEBHOOK_MAX_PAYLOAD_BYTES",
    "STLC_WEBHOOK_REQUESTS_PER_MINUTE", "STLC_WEBHOOK_REQUESTS_PER_SOURCE_PER_MINUTE",
    "LANGFUSE_TRACING_ENABLED",
}
DERIVED_ENV = {
    "STLC_RUNTIME_PROVIDER", "STLC_RUNTIME_AZURE_KEY_VAULT_URL",
    "STLC_RUNTIME_AZURE_SECRET_NAME", "STLC_RUNTIME_JOB_STORE",
    "STLC_RUNTIME_SESSION_GENERATION", "FOUNDRY_PROJECT_ENDPOINT", "FOUNDRY_AGENT_NAME",
    "STLC_SERVICE_BUS_NAMESPACE", "STLC_SERVICE_BUS_QUEUE", "STLC_SERVICE_BUS_WORK_QUEUE",
    "AZURE_CLIENT_ID", "PYTHON_DOTENV_DISABLED", "PLAYWRIGHT_BROWSERS_PATH",
}
INFRA_REQUIRED = {
    "prefix", "registryName", "keyVaultName", "foundryResourceGroup",
    "foundryAccountName", "foundryProjectName", "foundryLocation",
}


def allowed_environment() -> set[str]:
    # Read the application's reviewed allowlist without importing its runtime or
    # accidentally loading operator credentials/initializing telemetry.
    module = ast.parse((ROOT / "src/deploy_runtime.py").read_text())
    for node in module.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "DEPLOYMENT_ENV_NAMES" for t in node.targets
        ):
            return set(ast.literal_eval(node.value)) | EXTRA_ENV
    raise ValueError("Application deployment allowlist is missing")


def load_config(path: Path) -> dict:
    config = json.loads(path.read_text())
    fields = {"subscriptionId", "resourceGroup", "location", "infrastructure", "projectEndpoint", "agentName",
              "workerCpu", "workerMemory", "runtimeSecretName", "webhookSecretName", "sessionGeneration",
              "commonEnvironment", "workerEnvironment", "ingressEnvironment"}
    if not isinstance(config, dict) or set(config) - fields:
        raise ValueError("Unsupported configuration field; credentials belong in Key Vault")
    for key in ("subscriptionId", "resourceGroup", "location", "projectEndpoint",
                "agentName", "runtimeSecretName", "webhookSecretName", "sessionGeneration"):
        if not isinstance(config.get(key), str) or not config[key]:
            raise ValueError(f"Missing configuration field: {key}")
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", config["subscriptionId"]):
        raise ValueError("Invalid subscription identifier")
    if not re.fullmatch(r"https://[a-zA-Z0-9.-]+\.services\.ai\.azure\.com/api/projects/[a-zA-Z0-9_-]+", config["projectEndpoint"]):
        raise ValueError("Expected a Foundry project HTTPS endpoint without credentials")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", config["sessionGeneration"]):
        raise ValueError("Invalid cloud session generation")
    infra = config.get("infrastructure", {})
    optional_infra = {"vnetAddressPrefix", "subnetAddressPrefix", "serviceBusNamespaceName", "intakeLockDuration",
                      "foundryProjectDisplayName", "foundryProjectDescription", "serviceBusTags", "vnetTags",
                      "acaSubnetDelegationName"}
    if not INFRA_REQUIRED <= infra.keys() or set(infra) - INFRA_REQUIRED - optional_infra:
        raise ValueError("Invalid infrastructure parameter names")
    allowed = allowed_environment() - DERIVED_ENV
    for block in ("commonEnvironment", "workerEnvironment", "ingressEnvironment"):
        env = config.get(block, {})
        if not isinstance(env, dict) or any(k not in allowed or not isinstance(v, str) for k, v in env.items()):
            raise ValueError(f"Unsupported or secret environment variable in {block}")
    combined = {**config.get("commonEnvironment", {}), **config.get("workerEnvironment", {})}
    if combined.get("STLC_EVENT_PROVIDER") != "azure_devops":
        raise ValueError("This Service Bus release requires Azure DevOps events")
    mode = combined.get("ADO_AUTH_MODE", "entra")
    if mode not in {"entra", "pat"} or mode not in combined.get("STLC_ALLOWED_ADO_AUTH_MODES", "entra").split(","):
        raise ValueError("Explicit ADO authentication mode and policy must agree")
    return config


def az(config: dict, *args: str):
    result = subprocess.run(
        ["az", *args, "--subscription", config["subscriptionId"],
         "--only-show-errors", "--output", "json"],
        check=False, capture_output=True, text=True,
    )
    if result.returncode:
        # Provider errors can include reflected request bodies. Keep pipeline
        # diagnostics to the failed operation; inspect details in Azure Activity Log.
        raise RuntimeError("Azure CLI operation failed: " + " ".join(args[:3]))
    return json.loads(result.stdout) if result.stdout.strip() else None


def emit(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str) + "\n")


def provision(config: dict, output: Path, apply: bool):
    with tempfile.TemporaryDirectory(prefix="stlc-arm-") as scratch:
        parameters = Path(scratch) / "parameters.json"
        emit(parameters, {"parameters": {k: {"value": v} for k, v in
             {**config["infrastructure"], "location": config["location"]}.items()}})
        if apply:
            az(config, "group", "create", "--name", config["resourceGroup"], "--location", config["location"])
        result = az(config, "deployment", "group", "create" if apply else "what-if",
                    "--resource-group", config["resourceGroup"], "--name", "stlc-infrastructure",
                    "--template-file", str(HERE / "main.bicep"), "--parameters", "@" + str(parameters),
                    *([] if apply else ["--no-pretty-print"]))
    if apply:
        result = {k: v["value"] for k, v in result["properties"]["outputs"].items()}
    emit(output, result)
    print(json.dumps({"infrastructure": "applied" if apply else "planned", "output": str(output)}), flush=True)


def grant(config: dict, principal: str, role: str, scope: str):
    definitions = az(config, "role", "definition", "list", "--name", role)
    if len(definitions) != 1:
        raise ValueError("Cannot resolve role: " + role)
    role_id = definitions[0]["id"]
    assignments = az(config, "role", "assignment", "list", "--scope", scope)
    if any(a["principalId"].lower() == principal.lower() and
           a["roleDefinitionId"].lower() == role_id.lower() for a in assignments):
        return
    az(config, "role", "assignment", "create", "--assignee-object-id", principal,
       "--assignee-principal-type", "ServicePrincipal", "--role", role_id, "--scope", scope)


def release_environment(config: dict, infra: dict) -> tuple[dict, dict]:
    common = {**config.get("commonEnvironment", {}), "PYTHON_DOTENV_DISABLED": "1",
              "STLC_RUNTIME_PROVIDER": "azure", "STLC_RUNTIME_JOB_STORE": "session"}
    worker = {**common, **config.get("workerEnvironment", {}),
              "STLC_RUNTIME_AZURE_KEY_VAULT_URL": infra["vaultUrl"].rstrip("/"),
              "STLC_RUNTIME_AZURE_SECRET_NAME": config["runtimeSecretName"],
              "PLAYWRIGHT_BROWSERS_PATH": "/opt/ms-playwright"}
    ingress = {**common, **config.get("ingressEnvironment", {}),
               "STLC_TELEMETRY_ENABLED": "false", "LANGFUSE_TRACING_ENABLED": "false",
               "FOUNDRY_PROJECT_ENDPOINT": config["projectEndpoint"],
               "FOUNDRY_AGENT_NAME": config["agentName"],
               "AZURE_CLIENT_ID": infra["identityClientId"],
               "STLC_RUNTIME_SESSION_GENERATION": config["sessionGeneration"],
               "STLC_SERVICE_BUS_NAMESPACE": infra["busNamespace"],
               "STLC_SERVICE_BUS_QUEUE": "stlc-events", "STLC_SERVICE_BUS_WORK_QUEUE": "stlc-work-items"}
    return worker, ingress


def validate_images(images: dict, infra: dict):
    for role in ("worker", "webhook"):
        if not re.fullmatch(re.escape(infra["registryServer"]) + rf"/stlc-{role}@sha256:[0-9a-f]{{64}}", images.get(role, "")):
            raise ValueError("Expected an immutable image in the selected ACR: " + role)


def deploy_worker(config: dict, infra: dict, image: str, environment: dict) -> dict:
    # Never load the caller's .env or export operator telemetry. The worker
    # receives only the reviewed environment and retrieves its own secret bundle.
    os.environ.update(PYTHON_DOTENV_DISABLED="1", STLC_TELEMETRY_ENABLED="false", LANGFUSE_TRACING_ENABLED="false")
    sys.path.insert(0, str(ROOT))
    from azure.identity import AzureCliCredential
    from azure.ai.projects import AIProjectClient
    from andromeda.runtime import AzureHostedAgentsConfig, ManagedRuntime, RuntimeJobConfig
    from src.runtime import bind_application

    with AzureCliCredential() as credential, AIProjectClient(endpoint=config["projectEndpoint"], credential=credential) as project:
        runtime = ManagedRuntime("azure", options=AzureHostedAgentsConfig(
            project_endpoint=config["projectEndpoint"], agent_name=config["agentName"],
            cpu=config.get("workerCpu", "2"), memory=config.get("workerMemory", "4Gi"),
            credential=credential, project_client=project, wait_until_ready=False,
            enable_resilient_background=True,
        ), jobs=RuntimeJobConfig(store="session"))
        session = None
        try:
            app = bind_application(runtime, "azure")
            result = runtime.deploy(runtime.plan(app, image_uri=image, environment=environment))
            details = project.agents.get_version(agent_name=config["agentName"], agent_version=result.version)
            if not details.instance_identity or not details.instance_identity.principal_id:
                raise RuntimeError("Worker identity was not returned; stop before enabling intake")
            grant(config, details.instance_identity.principal_id, "Key Vault Secrets User",
                  infra["vaultId"] + "/secrets/" + config["runtimeSecretName"])
            deadline = time.monotonic() + 900
            while True:
                details = project.agents.get_version(agent_name=config["agentName"], agent_version=result.version)
                status = getattr(details.status, "value", str(details.status)).lower()
                if status == "active":
                    break
                if status == "failed" or time.monotonic() > deadline:
                    raise RuntimeError("Worker activation failed or timed out")
                time.sleep(5)
            session = "stlc-deployment-check-" + uuid4().hex
            # Missing prompt fails after secret loading and telemetry setup, and
            # before any model/integration call. This validates native job storage.
            handle = app.submit({}, session_id=session, idempotency_key=session)
            deadline = time.monotonic() + 300
            while True:
                snapshot = handle.refresh()
                if snapshot.status.terminal:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("Worker readiness job timed out")
                time.sleep(2)
            if snapshot.status.value != "failed" or snapshot.error_type != "KeyError":
                raise RuntimeError("Worker readiness job did not reach post-secret validation")
            project.agents.delete_session(agent_name=config["agentName"], session_id=session)
            session = None
            return {"agent": config["agentName"], "version": result.version, "image": image,
                    "principalId": details.instance_identity.principal_id, "nativeJobStoreReady": True}
        finally:
            if session:
                print(json.dumps({"readinessSessionToInspect": session}), flush=True)
            runtime.close()


def ingress_definition(config: dict, infra: dict, image: str, environment: dict) -> dict:
    return {
        "name": config["infrastructure"]["prefix"] + "-aca", "location": config["location"],
        "identity": {"type": "UserAssigned", "userAssignedIdentities": {infra["identityId"]: {}}},
        "properties": {
            "managedEnvironmentId": infra["environmentId"],
            "configuration": {
                "activeRevisionsMode": "Single",
                "ingress": {"external": False, "targetPort": 8080, "transport": "http", "allowInsecure": False},
                "registries": [{"server": infra["registryServer"], "identity": infra["identityId"]}],
                "secrets": [{"name": "ado-hook", "keyVaultUrl": infra["vaultUrl"].rstrip("/") + "/secrets/" + config["webhookSecretName"], "identity": infra["identityId"]}],
            },
            "template": {
                "revisionSuffix": "release-" + uuid4().hex[:12],
                "containers": [{"name": "ingress", "image": image, "resources": {"cpu": 0.5, "memory": "1Gi"},
                    "env": [{"name": k, "value": v} for k, v in sorted(environment.items())] + [{"name": "ADO_WEBHOOK_SHARED_SECRET", "secretRef": "ado-hook"}],
                    "probes": [{"type": kind, "httpGet": {"path": "/health", "port": 8080}, "periodSeconds": 10,
                                "timeoutSeconds": 3, "failureThreshold": 30 if kind == "Startup" else 10}
                               for kind in ("Startup", "Readiness", "Liveness")]}],
                "scale": {"minReplicas": 1, "maxReplicas": 1},
            },
        },
    }


def lock_expiry_wait(duration: str) -> int:
    """Wait a full broker lock duration plus margin; parse Azure's ISO duration."""
    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?", duration)
    if match is None:
        raise ValueError("Cannot determine the intake queue lock duration")
    seconds = sum(float(value or 0) * factor for value, factor in zip(match.groups(), (3600, 60, 1)))
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("Intake queue lock duration must be positive")
    return math.ceil(seconds) + 5


def deploy_ingress(config: dict, definition: dict, output: Path, *, intake_queue_id: str):
    name = definition["name"]
    apps = az(config, "containerapp", "list", "--resource-group", config["resourceGroup"])
    exists = any(app["name"] == name for app in apps)
    if exists:
        # Read before stopping intake, so a lookup/parse failure leaves it running.
        # The live value also covers Bicep parameter overrides and configuration drift.
        lock_wait = lock_expiry_wait(az(config, "resource", "show", "--ids", intake_queue_id,
                                       "--api-version", "2024-01-01", "--query", "properties.lockDuration"))
        revisions = az(config, "containerapp", "revision", "list", "--name", name, "--resource-group", config["resourceGroup"])
        old = [r["name"] for r in revisions if r["properties"].get("active")]
        for revision in old:
            az(config, "containerapp", "revision", "deactivate", "--revision", revision, "--name", name, "--resource-group", config["resourceGroup"])
        deadline = time.monotonic() + 300
        for revision in old:
            while az(config, "containerapp", "replica", "list", "--revision", revision, "--name", name, "--resource-group", config["resourceGroup"]):
                if time.monotonic() > deadline:
                    raise RuntimeError("Old intake router has not stopped")
                time.sleep(5)
        # Old unforwarded deliveries must become
        # visible before a new router can receive a later event for that item.
        time.sleep(lock_wait)
    definition_path = output / "ingress.json"
    emit(definition_path, definition)
    az(config, "containerapp", "update" if exists else "create", "--name", name,
       "--resource-group", config["resourceGroup"], "--yaml", str(definition_path))
    deadline = time.monotonic() + 600
    while True:
        details = az(config, "containerapp", "show", "--name", name, "--resource-group", config["resourceGroup"])
        properties = details["properties"]
        if properties.get("latestReadyRevisionName") == properties.get("latestRevisionName"):
            break
        if properties.get("provisioningState") == "Failed" or time.monotonic() > deadline:
            raise RuntimeError("New ingress revision did not become ready")
        time.sleep(5)
    emit(output / "ingress-deployment.json", {"app": name, "revision": properties["latestReadyRevisionName"]})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("infrastructure", "release"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--infrastructure", type=Path)
    parser.add_argument("--images", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    config = load_config(args.config)
    if args.operation == "infrastructure":
        provision(config, args.output, args.apply)
        return
    if not args.infrastructure or not args.images:
        parser.error("release requires --infrastructure and --images")
    infra = json.loads(args.infrastructure.read_text())
    images = json.loads(args.images.read_text())["images"]
    validate_images(images, infra)
    worker, ingress = release_environment(config, infra)
    definition = ingress_definition(config, infra, images["webhook"], ingress)
    emit(args.output / "release-plan.json", {"images": images, "workerEnvironment": worker, "ingress": definition})
    if not args.apply:
        print(json.dumps({"release": "planned", "output": str(args.output)}))
        return
    # Assign only exact resource/secret scopes, using object IDs rather than
    # directory lookup. The ADO organization membership is an operator step.
    for principal, role, scope in (
        (infra["identityPrincipalId"], "AcrPull", infra["registryId"]),
        (infra["projectPrincipalId"], "AcrPull", infra["registryId"]),
        (infra["identityPrincipalId"], "Foundry User", infra["projectId"]),
        (infra["identityPrincipalId"], "Key Vault Secrets User", infra["vaultId"] + "/secrets/" + config["webhookSecretName"]),
        (infra["identityPrincipalId"], "Azure Service Bus Data Receiver", infra["intakeQueueId"]),
        (infra["identityPrincipalId"], "Azure Service Bus Data Receiver", infra["workQueueId"]),
        (infra["identityPrincipalId"], "Azure Service Bus Data Sender", infra["workQueueId"]),
    ):
        grant(config, principal, role, scope)
    record = deploy_worker(config, infra, images["worker"], worker)
    emit(args.output / "worker-deployment.json", record)
    deploy_ingress(config, definition, args.output, intake_queue_id=infra["intakeQueueId"])
    print(json.dumps({"release": "deployed", "worker": record, "output": str(args.output)}))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"status": "stopped", "errorType": type(exc).__name__,
                          "reason": str(exc) if isinstance(exc, (ValueError, RuntimeError)) else "Inspect the failed operation in Azure"}), file=sys.stderr)
        raise SystemExit(1) from None
