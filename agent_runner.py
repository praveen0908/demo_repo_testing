"""
Run the provider-neutral STLC supervisor for one normalized integration prompt.

Each work item uses a dedicated filesystem root under ``<workspace>/workitems/…``. Agents in the same
work item share durable repository state; different work items remain isolated.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, Sequence

from andromeda import HumanMessage
from src.agents.streaming import supervisor_events, run_with_responsive_loop
from andromeda.tools import tool
from src.integrations.contracts import SourceControlAdapter, WorkTrackingAdapter
from src.integrations.models import Capability, ExternalReference, RepositoryReference, WorkItemContext
from src.integrations.registry import IntegrationBundle, build_integration_bundle
from src.tools.workspace import get_internal_trace_store_dir, get_workspace_base
from src.security import require_repository_access, SecurityPolicyError
from src.tools.git_process import run_git, SAFETY_OPTIONS as _GIT_SAFETY_OPTIONS, process_environment as _git_process_environment

logger = logging.getLogger(__name__)

WorkItemId = str | int


def _canonical_work_item_id(work_item_id: WorkItemId) -> str:
    value = str(work_item_id).strip()
    if not value:
        raise ValueError("work_item_id cannot be empty")
    return value


def _work_item_storage_key(work_item_id: WorkItemId) -> str:
    """Return a deterministic path/ref-safe key while preserving existing numeric IDs."""
    value = _canonical_work_item_id(work_item_id)
    if len(value) <= 80 and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value):
        return value
    readable = re.sub(r"[^A-Za-z0-9._-]", "_", value).strip("._-")[:48] or "item"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]
    return f"{readable}-{digest}"


def _work_item_reference_key(work_item: ExternalReference) -> str:
    """Namespace durable state by provider, connection, namespace, and external ID."""
    # Preserve the original all-Azure workspace path and branch identity. Azure work-item IDs
    # are organization-wide, and the explicit parity profile uses this logical connection.
    if (
        work_item.provider == "azure_devops"
        and work_item.connection_id == "azure-devops-work"
        and work_item.external_id.isdecimal()
    ):
        return _work_item_storage_key(work_item.external_id)
    identity = "\x1f".join(
        (
            work_item.provider,
            work_item.connection_id,
            work_item.namespace or "",
            work_item.external_id,
        )
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:10]
    provider = _work_item_storage_key(work_item.provider)[:24]
    connection = _work_item_storage_key(work_item.connection_id)[:32]
    external_id = _work_item_storage_key(work_item.external_id)[:48]
    return f"{provider}-{connection}-{external_id}-{digest}"


def _validate_work_item_reference(
    work_tracking: WorkTrackingAdapter,
    work_item: ExternalReference,
) -> None:
    if work_item.kind != "work_item":
        raise ValueError(f"Expected a work_item reference, got {work_item.kind!r}")
    if work_item.provider != work_tracking.provider:
        raise ValueError(
            "Work-item reference provider does not match the selected work-tracking adapter: "
            f"{work_item.provider!r} != {work_tracking.provider!r}"
        )
    if work_item.connection_id != work_tracking.connection_id:
        raise ValueError(
            "Work-item reference connection does not match the selected work-tracking adapter: "
            f"{work_item.connection_id!r} != {work_tracking.connection_id!r}"
        )


async def _run_agent_trace_async(
    prompt: str,
    agent_workspace: Path,
    project_name: Optional[str],
    integrations: IntegrationBundle,
    runtime_tools: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    """
    Build the supervisor and stream one agent trace.

    All roles share one native Andromeda workspace session rooted at ``agent_workspace``.
    Role-specific tool profiles control read, write, and shell access.
    """
    from src.agents.factory import create_supervisor_with_integrations

    sup = create_supervisor_with_integrations(
        agent_workspace,
        project_override=project_name,
        runtime_tools=runtime_tools,
        integration_bundle=integrations,
    )
    try:
        trace = await _collect_agent_trace(sup, prompt)
        reviewer = getattr(sup, "_prompt_injection_reviewer", None)
        if reviewer is not None and reviewer.blocked:
            from src.prompt_injection import DIRECT_BLOCKED_MESSAGE, PromptInjectionBlockedError
            raise PromptInjectionBlockedError(DIRECT_BLOCKED_MESSAGE, trace=trace)
        return trace
    finally:
        # Close nested agents first, then release the single shared workspace session once.
        for agent in list(getattr(sup, "agent_map", {}).values()):
            close = getattr(agent, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    logger.exception("Failed to close nested workspace agent %s", getattr(agent, "name", "unknown"))
        shared_session = getattr(sup, "_shared_workspace_session", None)
        if shared_session is not None:
            try:
                shared_session.cleanup()
            except Exception:
                logger.exception("Failed to clean up shared workspace session")


def _agent_workspace_directory(
    work_item: Optional[ExternalReference],
    base_path_override: Optional[Path],
) -> Path:
    """
    Filesystem and integration download anchor for one webhook/agent run.

    With ``base_path_override`` (tests), uses that path. With ``work_item``, uses a provider- and
    connection-namespaced directory. Otherwise ``<workspace>/workitems/_adhoc/run-<uuid>/`` so runs
    without a work item id still get an isolated directory (never the bare workspace root).
    """
    if base_path_override is not None:
        root = Path(base_path_override).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root
    outer = get_workspace_base().resolve()
    if work_item is None:
        adhoc = outer / "workitems" / "_adhoc" / f"run-{uuid.uuid4().hex}"
        adhoc.mkdir(parents=True, exist_ok=True)
        return adhoc.resolve()
    wi_dir = outer / "workitems" / _work_item_reference_key(work_item)
    wi_dir.mkdir(parents=True, exist_ok=True)
    return wi_dir.resolve()


def prepare_work_item_images(
    work_tracking: WorkTrackingAdapter,
    work_item: ExternalReference,
) -> Dict[str, Any]:
    """Best-effort extraction of attached/embedded images before any agent starts."""
    _validate_work_item_reference(work_tracking, work_item)
    return work_tracking.extract_work_item_images(
        work_item_id=work_item.external_id,
        output_path="attachments/images",
        overwrite=True,
    )


# ---------------------------------------------------------------------------
# Repository checkout: provider discovery resolves canonical repository references,
# then the shared Git workspace preserves durable work-item progress.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class _RepoCheckout:
    clone_url: str
    dest_dir: Path
    ref: str
    work_ref: Optional[str] = None


@dataclass(frozen=True)
class CheckoutResult:
    repo_path: Path
    repo_name: str
    repository_id: str
    project: str
    branch: str
    default_branch: str
    role_hint: Literal["application", "automation", "mixed", "unknown"] = "unknown"
    work_branch: Optional[str] = None
    checked_out_branch: Optional[str] = None
    provider: str = ""
    connection_id: str = ""
    provenance: str = "structured_relation"
    evidence: Optional[str] = None
    clone_url: str = ""
    # Capture the linked target before handing the checkout to executor code.
    target_commit: str = ""


# Conservative allow-list for a git ref taken from user-controllable work-item relations:
# letters/digits first, then letters/digits/._/- (blocks leading '-' option injection, spaces, ';', '^', '~', etc.).
_SAFE_GIT_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")


def _validate_git_ref(ref: str) -> str:
    """Reject unsafe/argument-injection-prone refs before they reach the git CLI."""
    if not ref or not _SAFE_GIT_REF_RE.match(ref) or ".." in ref:
        raise ValueError(f"Unsafe or invalid git branch/ref: {ref!r}")
    return ref


def _work_item_branch_name(work_item: ExternalReference, base_branch: str) -> str:
    """Return the stable agent branch for one work item and linked base branch."""
    branch_key = hashlib.sha256(_validate_git_ref(base_branch).encode("utf-8")).hexdigest()[:8]
    external_id = _work_item_storage_key(work_item.external_id)[:48]
    reference_key = _work_item_reference_key(work_item)
    if reference_key == external_id:
        return f"agent/wi-{external_id}-{branch_key}"
    scope_key = hashlib.sha256(reference_key.encode("utf-8")).hexdigest()[:8]
    return f"agent/wi-{external_id}-{scope_key}-{branch_key}"


def _git(
    args: List[str],
    *,
    cwd: Path,
    timeout_s: int = 120,
    env: Optional[Mapping[str, str] | Callable[[], Mapping[str, str]]] = None,
    expected_remote_url: Optional[str] = None,
) -> None:
    normalized_cwd = cwd.expanduser().resolve()
    if not normalized_cwd.exists():
        raise RuntimeError(f"git command cwd does not exist: {normalized_cwd}")
    run_git(
        args,
        cwd=str(normalized_cwd),
        check=True,
        capture_output=True,
        timeout=timeout_s,
        env=env,
        expected_remote_url=expected_remote_url,
    )


def _git_ref_exists(repo_path: Path, ref: str) -> bool:
    return (
        run_git(
            ["show-ref", "--verify", "--quiet", ref],
            cwd=str(repo_path.expanduser().resolve()),
            check=False,
            capture_output=True,
        ).returncode
        == 0
    )


def _git_worktree_is_dirty(repo_path: Path) -> bool:
    return bool(_git_capture(["status", "--porcelain", "-z"], cwd=repo_path))


def _git_current_branch(repo_path: Path) -> str:
    return _git_capture(["branch", "--show-current"], cwd=repo_path).strip()


def _safe_refresh_existing_checkout(
    repo_path: Path,
    *,
    base_ref: str,
    work_ref: Optional[str],
    auth_env: Mapping[str, str] | Callable[[], Mapping[str, str]],
    expected_remote_url: str,
) -> None:
    """Fetch and fast-forward a durable checkout without discarding local progress."""
    try:
        _git(["fetch", "--quiet", "--prune", "origin"], cwd=repo_path, env=auth_env,
             expected_remote_url=expected_remote_url)
    except SecurityPolicyError:
        raise
    except Exception:
        logger.warning("Could not fetch latest repository state for %s; preserving local checkout", repo_path)
        return

    remote_work = bool(work_ref and _git_ref_exists(repo_path, f"refs/remotes/origin/{work_ref}"))
    local_work = bool(work_ref and _git_ref_exists(repo_path, f"refs/heads/{work_ref}"))
    desired = work_ref if work_ref and (remote_work or local_work) else base_ref

    if _git_worktree_is_dirty(repo_path):
        logger.info("Repository %s is dirty; fetched refs but preserved its current branch and files", repo_path)
        return

    try:
        current = _git_current_branch(repo_path)
        if current != desired:
            if _git_ref_exists(repo_path, f"refs/heads/{desired}"):
                _git(["checkout", "--quiet", desired], cwd=repo_path)
            else:
                _git(
                    ["checkout", "--quiet", "-b", desired, "--track", f"origin/{desired}"],
                    cwd=repo_path,
                )
        if _git_ref_exists(repo_path, f"refs/remotes/origin/{desired}"):
            _git(["merge", "--quiet", "--ff-only", f"origin/{desired}"], cwd=repo_path)
    except Exception:
        # A local commit or concurrent remote update may prevent a fast-forward. Preserve both
        # histories for the agent to inspect instead of resetting or stashing automatically.
        logger.warning("Could not safely fast-forward %s to origin/%s; preserving local state", repo_path, desired)


def _ensure_repo_checked_out(
    checkout: _RepoCheckout,
    *,
    auth_env: Mapping[str, str] | Callable[[], Mapping[str, str]],
) -> Path:
    """
    Deterministically clone + checkout ``ref`` into ``dest_dir``.

    Authentication is supplied by the source-control adapter as an ephemeral environment, so
    credentials are never written to the clone URL, ``.git/config``, or command arguments.
    """
    ref = _validate_git_ref(checkout.ref)
    repo_dir = checkout.dest_dir.expanduser().resolve()
    if (repo_dir / ".git").exists():
        _verify_repository_remote(repo_dir, checkout.clone_url)
        _safe_refresh_existing_checkout(
            repo_dir,
            base_ref=ref,
            work_ref=checkout.work_ref,
            auth_env=auth_env,
            expected_remote_url=checkout.clone_url,
        )
        return repo_dir
    if repo_dir.exists():
        shutil.rmtree(repo_dir, ignore_errors=True)
    repo_dir.parent.mkdir(parents=True, exist_ok=True)
    _git(["clone", "--quiet", "--", checkout.clone_url, repo_dir.name], cwd=repo_dir.parent, env=auth_env)
    _git(["fetch", "--quiet", "--prune", "origin"], cwd=repo_dir, env=auth_env,
         expected_remote_url=checkout.clone_url)
    desired_ref = (
        checkout.work_ref
        if checkout.work_ref
        and _git_ref_exists(repo_dir, f"refs/remotes/origin/{checkout.work_ref}")
        else ref
    )
    # Initial checkout/reset/clean operate on already-fetched refs — no network or auth needed.
    _git(["checkout", "--quiet", "--force", "-B", desired_ref, f"origin/{desired_ref}"], cwd=repo_dir)
    _git(["reset", "--hard", f"origin/{desired_ref}"], cwd=repo_dir)
    _git(["clean", "-ffdx"], cwd=repo_dir)
    return repo_dir


def _repository_role_hint(
    repo_path: Path,
    repo_name: str,
) -> Literal["application", "automation", "mixed", "unknown"]:
    """Return a conservative content-based hint; the agent must still inspect the repository."""
    root = Path(repo_path)
    name = repo_name.lower()
    automation_name = any(token in name for token in ("test", "automation", "quality", "qa", "e2e"))
    automation_score = sum(token in name for token in ("test", "automation", "quality", "qa", "e2e"))
    application_score = sum(token in name for token in ("app", "service", "api", "portal", "web"))

    automation_patterns = (
        "playwright.config.*",
        "cypress.config.*",
        "robot.yaml",
        "pytest.ini",
        "tests",
        "test",
        "e2e",
        "features",
        "src/test",
    )
    application_patterns = (
        "src",
        "app",
        "server",
        "services",
        "Dockerfile",
        "docker-compose.*",
        "pyproject.toml",
        "*.csproj",
        "pom.xml",
    )
    automation_score += sum(any(root.glob(pattern)) for pattern in automation_patterns)
    java_test_only_layout = (root / "src" / "test").is_dir() and not (
        root / "src" / "main"
    ).exists()
    if not java_test_only_layout:
        application_score += sum(any(root.glob(pattern)) for pattern in application_patterns)

    if automation_score >= 2 and application_score >= 2:
        return "mixed"
    if application_score >= 2:
        return "application"
    if automation_name and automation_score >= 2:
        return "automation"
    if automation_score >= 2:
        return "automation"
    return "unknown"


def prepare_work_item_repositories(
    source_control: SourceControlAdapter,
    work_item: WorkItemContext,
    agent_workspace: Path,
) -> List[CheckoutResult]:
    """Resolve and clone every provider-approved repository into the work-item workspace.

    A failure for one repository is logged and does not prevent the remaining repositories
    from being made available to the agent.
    """
    references = source_control.discover_repositories(work_item)
    work_item_id = work_item.reference.external_id
    # Resolve credentials immediately before each Git command, including a fetch
    # following a long clone. Entra tokens may expire during an invocation.
    auth_env = source_control.git_auth_environment
    checkouts: List[CheckoutResult] = []
    repos_root = Path(agent_workspace).expanduser().resolve() / "repos"

    for reference in references:
        dest: Optional[Path] = None
        reused_existing_checkout = False
        try:
            require_repository_access(reference.provider, reference.project, reference.name)
            _validate_git_ref(reference.branch)
            safe = re.sub(r"[^A-Za-z0-9._-]", "_", reference.name) or "repo"
            branch_key = hashlib.sha256(reference.branch.encode("utf-8")).hexdigest()[:8]
            repository_key = re.sub(r"[^A-Za-z0-9]", "", reference.repository_id)[:8] or "repo"
            dest = repos_root / f"{safe}-{repository_key}-{branch_key}"
            reused_existing_checkout = (dest / ".git").exists()
            work_branch = _work_item_branch_name(work_item.reference, reference.branch)
            repo_path = _ensure_repo_checked_out(
                _RepoCheckout(
                    clone_url=reference.clone_url,
                    dest_dir=dest,
                    ref=reference.branch,
                    work_ref=work_branch,
                ),
                auth_env=auth_env,
            )
            checkout = CheckoutResult(
                repo_path=repo_path,
                repo_name=reference.name,
                repository_id=reference.repository_id,
                project=reference.project,
                branch=reference.branch,
                default_branch=reference.default_branch or reference.branch,
                role_hint=_repository_role_hint(repo_path, reference.name),
                work_branch=work_branch,
                checked_out_branch=(
                    _git_current_branch(repo_path)
                    if (repo_path / ".git").exists()
                    else reference.branch
                ),
                provider=reference.provider,
                connection_id=reference.connection_id,
                provenance=reference.provenance,
                evidence=reference.evidence,
                clone_url=reference.clone_url,
                target_commit=_git_capture(
                    ["rev-parse", "--verify", f"refs/remotes/origin/{reference.branch}^{{commit}}"],
                    cwd=repo_path,
                ).strip(),
            )
            checkouts.append(checkout)
            logger.info(
                "Checked out repo=%s branch=%s for work item %s -> %s",
                reference.name,
                reference.branch,
                work_item_id,
                repo_path,
            )
        except Exception:
            logger.exception(
                "Repo checkout failed for work item %s repository=%s branch=%s; continuing",
                work_item_id,
                reference.repository_id,
                reference.branch,
            )
            if dest is not None and not reused_existing_checkout:
                shutil.rmtree(dest, ignore_errors=True)
    return checkouts


def _repo_workspace_relative_path(repo_path: Path) -> str:
    parts = Path(repo_path).parts
    if "repos" in parts:
        index = len(parts) - 1 - list(reversed(parts)).index("repos")
        return Path(*parts[index:]).as_posix()
    return Path(repo_path).name


def _augment_prompt_with_workspace_contract(prompt: str) -> str:
    header = "\n".join(
        [
            "Workspace path contract:",
            "- Every agent shares the same current work-item root; sandbox tools may display it as /workspace.",
            "- For shell calls, cwd must be workspace-relative (`.` or `repos/...`), never `/workspace` or another absolute path.",
            "- Use work-item-relative paths such as attachments/..., artifacts/..., and repos/... .",
            "- Keep run artifacts, reports, screenshots, traces, checkpoints, and working notes in root artifacts/..., outside repos/... checkouts. A relative output path from repository cwd resolves inside that repository; test scripts can use STLC_WORK_ITEM_ARTIFACTS_DIR to find the root.",
            "- Never prepend workitems/<id>/; the current root already represents that work item.",
            "- Pass the same relative path returned by a delegate to integration file/publish tools.",
        ]
    )
    return f"{header}\n\n{prompt}"


def _augment_prompt_with_prepared_images(prompt: str, result: Dict[str, Any], reviewer=None) -> str:
    if reviewer is not None and not reviewer.allows(result, source="prepared_attachment_metadata"):
        from src.prompt_injection import BLOCKED_MESSAGE
        return f"Prepared attachment metadata: {BLOCKED_MESSAGE}\n\n{prompt}"
    saved = result.get("saved") or []
    if not saved:
        return prompt
    names = [str(item.get("name") or "").strip() for item in saved if item.get("name")]
    header = "\n".join(
        [
            "Work-item images were extracted before this agent run.",
            "- Inspect them with read/view tools when they are relevant; do not infer content from filenames.",
            f"- Workspace directory: attachments/images ({len(names)} image(s): {', '.join(names)})",
        ]
    )
    return f"{header}\n\n{prompt}"


def _augment_prompt_with_coverage_candidates(prompt: str, context: WorkItemContext) -> str:
    """Keep cited work-item IDs visible when a later text search finds no coverage."""
    candidates: set[str] = set()
    for url in context.relation_urls:
        match = re.search(r"/workItems/(\d+)(?:[/?#]|$)", url, re.IGNORECASE)
        if match:
            candidates.add(match.group(1))
    for comment in context.comments:
        for match in re.finditer(
            r"\b(?:test\s*cases?|TCs?)\b[^\d]{0,24}(\d{4,7})(?:\s*[-–—]\s*(\d{4,7}))?",
            comment, re.IGNORECASE,
        ):
            first = int(match.group(1))
            last = int(match.group(2) or first)
            if first <= last <= first + 25:
                candidates.update(str(item_id) for item_id in range(first, last + 1))
            else:
                candidates.add(str(first))
    candidates.discard(context.reference.external_id)
    if not candidates:
        return prompt
    listed = sorted(candidates, key=lambda value: int(value))[:30]
    header = (
        "Candidate work-item IDs cited by this item (links or Test Case comments): "
        + ", ".join(listed)
        + ". These are candidates, not verified coverage. Read each relevant case and its steps before "
        "claiming reuse or absence; a zero-result text search does not invalidate these IDs."
        " Prior mentions may describe rejected candidates or adaptation references. Preserve that context; "
        "reuse existing findings unless new information or adaptation requires another read. Missing optional "
        "candidate details do not stop independent planning from the current requirements."
    )
    return f"{header}\n\n{prompt}"


_MAX_REPOSITORY_AGENTS_CONTEXT_CHARS = 12_000


def _repository_agents_instructions(repo: CheckoutResult) -> Optional[str]:
    """Read compact root AGENTS.md instructions only from a verified test repository hint."""
    if repo.role_hint != "automation":
        return None
    content: Optional[str] = None
    if (repo.repo_path / ".git").exists():
        candidate_refs = [f"origin/{repo.branch}"]
        if repo.work_branch:
            candidate_refs.insert(0, f"origin/{repo.work_branch}")
        for ref in candidate_refs:
            if not _git_ref_has_path(repo.repo_path, ref, "AGENTS.md"):
                continue
            try:
                content = _git_capture(["show", f"{ref}:AGENTS.md"], cwd=repo.repo_path).strip()
            except Exception:
                logger.warning("Could not read test-repository profile from %s", ref)
            if content:
                break
    if not content:
        profile_path = repo.repo_path / "AGENTS.md"
        if not profile_path.is_file() or profile_path.is_symlink():
            return None
        try:
            content = profile_path.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            logger.warning("Could not read test-repository profile %s", profile_path)
            return None
    if not content:
        return None
    if len(content) > _MAX_REPOSITORY_AGENTS_CONTEXT_CHARS:
        return (
            content[:_MAX_REPOSITORY_AGENTS_CONTEXT_CHARS]
            + "\n\n[AGENTS.md truncated in supervisor context; read the repository file before implementation.]"
        )
    return content


def _repository_agents_is_durable(repo: CheckoutResult) -> bool:
    if not (repo.repo_path / ".git").exists():
        return False
    refs = [f"origin/{repo.branch}"]
    if repo.work_branch:
        refs.insert(0, f"origin/{repo.work_branch}")
    return any(_git_ref_has_path(repo.repo_path, ref, "AGENTS.md") for ref in refs)


def _augment_prompt_with_repositories(prompt: str, repo_contexts: Sequence[CheckoutResult], reviewer=None) -> str:
    lines = [
        "Repositories resolved from this work item are available in the isolated workspace.",
        "Repository handling rules:",
        "- Inspect every repository before planning, designing, or executing tests; verify the role hint from its contents.",
        "- Treat application source as read-only context. Use it to understand behavior, contracts, routes, identifiers, fixtures, and test IDs.",
        "- For an automation repository: Reuse its existing scripts, framework, helpers, conventions, and test IDs. Make only the minimal changes needed for this work item.",
        "- Before creating plan/design artifacts or new tests, map relevant existing specs/features, steps/helpers, data keys, tags/test IDs, and commands; state the exact uncovered gap.",
        "- Classify coverage for the exact requested product/workflow as `reuse unchanged`, `minimal extension`, or `no relevant coverage`; a linked repository or unrelated product/platform test does not establish reusable coverage.",
        "- Only `reuse unchanged` routes directly to execution without plan/design work.",
        "- For `minimal extension`, propose and, after HITL approval, publish a targeted native Test Case/design delta tied to selected existing assets before the minimal automation change and execution.",
        "- For `no relevant coverage`, the assignment-stage approval request must propose a native Test Plan, suites, and Test Cases in Design; after approval, publish those artifacts before implementation/execution.",
        "- When explicitly asked to modify existing Test Cases, identify verified external case IDs, preserve unspecified content, and update rather than duplicate them; step edits must supply the complete intended step list.",
        "- Run the narrowest relevant existing checks from that repository's workspace-relative cwd using the native sandbox shell.",
        "- Put generated reports, traces, screenshots, result JSON, scratch scripts, and working notes under the work-item root artifacts/..., outside every linked repository checkout. Configure repository test commands to write there rather than accepting output defaults inside the checkout.",
        "- Do not commit, push, or create a change request from the sandbox. Report any changes to the main supervisor, which decides whether publication is appropriate.",
        "- If repository publication tools are available, the main supervisor may publish reviewed automation changes. A dirty workspace is never published automatically.",
        "- Before returning, inspect each checkout's git status. Move only run-local files created by this attempt to root artifacts/...; preserve pre-existing unknown files. Report publishable source/test paths separately from any remaining run-local exclusions. Exclusions never belong in a change request.",
        "- Repository workspaces persist across invocations for this work item. Inspect and continue any existing local changes instead of recreating them.",
        "- Root AGENTS.md files are repository-native instructions scoped to their test repository. Include their constraints in every executor handoff and require the executor to read the file before editing.",
        "- During requirements analysis, if and only if a repository is verified as a dedicated test/automation repository and has no root AGENTS.md, load `repository-profile` and delegate its bounded bootstrap task to the test executor. Do not create AGENTS.md in application, mixed, or unknown repositories.",
        "- After validating a newly authored test-repository AGENTS.md, use repository_profile_publish if available; otherwise report the profile path for manual publication. This bootstrap does not create a change request.",
        "- Include repository-by-repository inspection, checks, results, and whether changes were necessary in the final execution response.",
        "Repositories:",
    ]
    for repo in repo_contexts:
        relative = _repo_workspace_relative_path(repo.repo_path)
        metadata = {
            "name": repo.repo_name, "path": relative, "branch": repo.branch,
            "work_branch": repo.work_branch, "checked_out_branch": repo.checked_out_branch,
            "default_branch": repo.default_branch, "role_hint": repo.role_hint,
            "repository_id": repo.repository_id, "provider": repo.provider, "provenance": repo.provenance,
        }
        if reviewer is not None and not reviewer.allows(metadata, source="prepared_repository_metadata"):
            from src.prompt_injection import BLOCKED_MESSAGE
            lines.append(f"- Repository metadata: {BLOCKED_MESSAGE}")
            continue
        lines.append(
            f"- {repo.repo_name}: path={relative}; branch={repo.branch}; "
            f"work_branch={repo.work_branch or 'none'}; checked_out_branch={repo.checked_out_branch or 'unknown'}; "
            f"default_branch={repo.default_branch}; role_hint={repo.role_hint}; "
            f"repository_id={repo.repository_id}; provider={repo.provider or 'configured'}; "
            f"provenance={repo.provenance}"
        )
        instructions = _repository_agents_instructions(repo)
        if instructions is not None:
            if reviewer is not None and not reviewer.allows(instructions, source="repository_profile"):
                from src.prompt_injection import BLOCKED_MESSAGE
                lines.append(f"- Repository profile content: {BLOCKED_MESSAGE}")
                continue
            lines.extend(
                [
                    f"\nRepository-native instructions for {repo.repo_name} ({relative}/AGENTS.md):",
                    "```markdown",
                    instructions,
                    "```",
                ]
            )
            if not _repository_agents_is_durable(repo):
                lines.append(
                    f"- {repo.repo_name} has a local-only AGENTS.md; validate it and call repository_profile_publish so it survives workspace loss."
                )
        elif repo.role_hint == "automation":
            lines.append(
                f"- {repo.repo_name} has no root AGENTS.md yet; run the repository-profile bootstrap before completing requirements analysis."
            )
    return "\n".join(lines) + f"\n\n{prompt}"


_SENSITIVE_REPOSITORY_PATH_RE = re.compile(
    r"(^|/)(?:\.env(?:\.[^/]*)?|auth(?:entication)?[-_.]?state(?:[._-][^/]*)?|"
    r"storage[-_.]?state(?:[._-][^/]*)?|credentials?(?:[._-][^/]*)?|"
    r"secrets?(?:[._-][^/]*)?)(?:/|$)",
    re.IGNORECASE,
)
_RUN_LOCAL_REPOSITORY_PATH_RE = re.compile(
    r"(^|/)(?:reports?|playwright-report|test-results?|screenshots?|traces?|coverage|"
    r"allure-results|allure-report|blob-report|artifacts)(?:/|$)|\.(?:log|tmp)$",
    re.IGNORECASE,
)


def _git_capture(
    args: List[str],
    *,
    cwd: Path,
    timeout_s: int = 120,
    env: Optional[Dict[str, str]] = None,
) -> str:
    normalized_cwd = cwd.expanduser().resolve()
    if not normalized_cwd.exists():
        raise RuntimeError(f"git command cwd does not exist: {normalized_cwd}")
    completed = run_git(
        args,
        cwd=str(normalized_cwd),
        check=True,
        capture_output=True,
        timeout=timeout_s,
        env=env,
        text=True,
    )
    return completed.stdout


def _assert_secret_free_blob(repo_path: Path, object_name: str) -> None:
    from src.observability import contains_runtime_secret
    blob = run_git(["show", object_name], cwd=repo_path, check=True, capture_output=True, timeout=120).stdout
    if contains_runtime_secret(blob):
        raise SecurityPolicyError("Publication contains a runtime credential or private key; remove it and use runtime configuration")


def _publication_base(repo: CheckoutResult) -> str:
    return repo.target_commit or _git_capture(
        ["rev-parse", "--verify", f"refs/remotes/origin/{repo.branch}^{{commit}}"],
        cwd=repo.repo_path,
    ).strip()


def _assert_secret_free_commits(repo: CheckoutResult, source_commit: str = "HEAD") -> None:
    objects = _git_capture(["rev-list", "--objects", f"{_publication_base(repo)}..{source_commit}"], cwd=repo.repo_path)
    for line in objects.splitlines():
        object_id = line.split(" ", 1)[0]
        if _git_capture(["cat-file", "-t", object_id], cwd=repo.repo_path).strip() == "blob":
            _assert_secret_free_blob(repo.repo_path, object_id)


def _verify_repository_remote(repo_path: Path, expected_url: str) -> None:
    """Check effective fetch and push URLs before exposing host-side credentials."""
    if not expected_url:
        raise SecurityPolicyError("Repository has no trusted discovery URL; rediscover it before publication")
    for arguments in (["remote", "get-url", "--all", "origin"], ["remote", "get-url", "--push", "--all", "origin"]):
        urls = _git_capture(arguments, cwd=repo_path).splitlines()
        if urls != [expected_url]:
            raise SecurityPolicyError("Repository remote no longer matches its approved discovery URL")


def _repository_changed_paths(repo_path: Path) -> List[str]:
    tracked = _git_capture(["diff", "--name-only", "-z", "HEAD"], cwd=repo_path).split("\0")
    untracked = _git_capture(
        ["ls-files", "--others", "--exclude-standard", "-z"],
        cwd=repo_path,
    ).split("\0")
    return list(dict.fromkeys(path for path in [*tracked, *untracked] if path))


def _repository_committed_paths_ahead(
    repo_path: Path, target_branch: str, *, source_commit: str = "HEAD", base_commit: Optional[str] = None,
) -> List[str]:
    """Include every changed path in history, including deleted files and merges."""
    return list(dict.fromkeys(
        path
        for path in _git_capture(
            ["log", "--format=", "--name-only", "--no-renames", "--root", "-m", "-z",
             f"{base_commit or f'origin/{target_branch}'}..{source_commit}"],
            cwd=repo_path,
        ).split("\0")
        if path
    ))


def _validate_publication_history(
    repo: CheckoutResult, source_commit: str, *, include_paths: Optional[Sequence[str]] = None,
    exclude_paths: Optional[Sequence[str]] = None,
) -> List[str]:
    base_commit = _publication_base(repo)
    if run_git(["merge-base", "--is-ancestor", base_commit, source_commit],
               cwd=repo.repo_path, check=False, capture_output=True).returncode != 0:
        raise SecurityPolicyError("Publication must descend from the recorded linked target commit")
    paths = _repository_committed_paths_ahead(
        repo.repo_path, repo.branch, source_commit=source_commit, base_commit=base_commit,
    )
    prohibited = [path for path in paths if
                  _SENSITIVE_REPOSITORY_PATH_RE.search(path.replace("\\", "/")) or
                  _RUN_LOCAL_REPOSITORY_PATH_RE.search(path.replace("\\", "/"))]
    requested = set(include_paths) if include_paths is not None else set(paths)
    unselected = sorted(set(paths) - requested | (set(paths) & set(exclude_paths or ())))
    if prohibited or unselected:
        raise SecurityPolicyError(
            "Outgoing history contains prohibited or unselected paths; preserve the checkout and review "
            "its commits before publication: " + ", ".join(sorted(set(prohibited) | set(unselected)))
        )
    _assert_secret_free_commits(repo, source_commit)
    return paths


def _assert_publishable_paths(paths: Sequence[str]) -> None:
    sensitive = sorted(path for path in paths if _SENSITIVE_REPOSITORY_PATH_RE.search(path.replace("\\", "/")))
    if sensitive:
        raise RuntimeError(
            "Refusing to publish possible secret/auth files: " + ", ".join(sensitive[:10])
        )


def _normalize_requested_repository_paths(paths: Optional[Sequence[str]]) -> Optional[List[str]]:
    if paths is None:
        return None
    normalized: List[str] = []
    for raw in paths:
        path = str(raw).strip().replace("\\", "/")
        parts = path.split("/")
        if not path or path.startswith("/") or any(part in {"", ".", ".."} for part in parts):
            raise ValueError(f"Invalid repository-relative publication path: {raw!r}")
        normalized.append(path)
    return list(dict.fromkeys(normalized))


def _select_repository_publication_paths(
    changed_paths: Sequence[str],
    *,
    include_paths: Optional[Sequence[str]] = None,
    exclude_paths: Optional[Sequence[str]] = None,
) -> tuple[List[str], List[Dict[str, str]]]:
    """Select source/test changes while retaining secrets and run-local evidence locally."""
    changed = list(dict.fromkeys(path.replace("\\", "/") for path in changed_paths))
    changed_set = set(changed)
    requested = _normalize_requested_repository_paths(include_paths)
    explicit_exclusions = set(_normalize_requested_repository_paths(exclude_paths) or [])
    if requested is not None:
        missing = sorted(set(requested) - changed_set)
        if missing:
            raise ValueError("Requested publication paths are not changed: " + ", ".join(missing))
    unknown_exclusions = sorted(explicit_exclusions - changed_set)
    if unknown_exclusions:
        raise ValueError(
            "Requested publication exclusions are not changed: " + ", ".join(unknown_exclusions)
        )

    requested_set = set(requested) if requested is not None else changed_set
    selected: List[str] = []
    excluded: List[Dict[str, str]] = []
    for path in changed:
        reason: Optional[str] = None
        if path not in requested_set:
            reason = "not_selected"
        elif path in explicit_exclusions:
            reason = "agent_excluded"
        elif _SENSITIVE_REPOSITORY_PATH_RE.search(path):
            reason = "sensitive_or_auth"
        elif _RUN_LOCAL_REPOSITORY_PATH_RE.search(path):
            reason = "run_local_artifact"
        if reason:
            excluded.append({"path": path, "reason": reason})
        else:
            selected.append(path)
    return selected, excluded


def _checkout_work_item_branch(repo: CheckoutResult, work_item: ExternalReference) -> str:
    """Move to the stable work-item branch without resetting or cleaning the worktree."""
    source_branch = repo.work_branch or _work_item_branch_name(work_item, repo.branch)
    _validate_git_ref(source_branch)
    if _git_current_branch(repo.repo_path) == source_branch:
        return source_branch
    if _git_ref_exists(repo.repo_path, f"refs/heads/{source_branch}"):
        _git(["checkout", "--quiet", source_branch], cwd=repo.repo_path)
    elif _git_ref_exists(repo.repo_path, f"refs/remotes/origin/{source_branch}"):
        _git(
            ["checkout", "--quiet", "-b", source_branch, "--track", f"origin/{source_branch}"],
            cwd=repo.repo_path,
        )
    else:
        _git(["checkout", "--quiet", "-b", source_branch], cwd=repo.repo_path)
    return source_branch


def _git_ref_has_path(repo_path: Path, ref: str, path: str) -> bool:
    return (
        run_git(
            ["cat-file", "-e", f"{ref}:{path}"],
            cwd=str(repo_path.expanduser().resolve()),
            check=False,
            capture_output=True,
        ).returncode
        == 0
    )


def _source_reference(
    repo: CheckoutResult,
    source_control: SourceControlAdapter,
) -> RepositoryReference:
    return RepositoryReference(
        provider=repo.provider or source_control.provider,
        connection_id=repo.connection_id or source_control.connection_id,
        repository_id=repo.repository_id,
        name=repo.repo_name,
        project=repo.project,
        branch=repo.branch,
        default_branch=repo.default_branch,
        clone_url=repo.clone_url,
        provenance=repo.provenance,
        evidence=repo.evidence,
    )


def _publish_test_repository_profile(
    repo: CheckoutResult,
    *,
    work_item: ExternalReference,
    source_control: SourceControlAdapter,
) -> Dict[str, Any]:
    """Commit and push an initial root AGENTS.md without opening a change request."""
    require_repository_access(repo.provider or source_control.provider, repo.project, repo.repo_name)
    base = {
        "repository": repo.repo_name,
        "repository_id": repo.repository_id,
        "branch": repo.branch,
        "role_hint": repo.role_hint,
        "profile_path": "AGENTS.md",
    }
    if repo.role_hint != "automation":
        return {
            **base,
            "status": "not_published",
            "error": "AGENTS.md bootstrap is allowed only in a verified dedicated test/automation repository.",
        }

    profile_path = repo.repo_path / "AGENTS.md"
    if not profile_path.is_file() or profile_path.is_symlink():
        return {
            **base,
            "status": "not_published",
            "error": "A regular root AGENTS.md file was not found in the test repository.",
        }
    profile_size = profile_path.stat().st_size
    if profile_size <= 0 or profile_size > 32_000:
        return {
            **base,
            "status": "not_published",
            "error": "AGENTS.md must be non-empty and no larger than 32 KB.",
        }

    if _git_ref_has_path(repo.repo_path, f"origin/{repo.branch}", "AGENTS.md"):
        return {
            **base,
            "status": "profile_exists_on_base",
            "error": "The linked base branch already has AGENTS.md; agent-authored updates are not published.",
        }

    source_branch = _checkout_work_item_branch(repo, work_item)
    _verify_repository_remote(repo.repo_path, repo.clone_url)
    _validate_publication_history(repo, "HEAD", include_paths=["AGENTS.md"])
    from src.observability import contains_runtime_secret
    if contains_runtime_secret(profile_path.read_bytes()):
        raise SecurityPolicyError("Repository profile contains a runtime credential or private key")
    _git(["reset", "--quiet", "HEAD", "--"], cwd=repo.repo_path)
    _git(["add", "--", "AGENTS.md"], cwd=repo.repo_path)
    staged_paths = [
        path
        for path in _git_capture(
            ["diff", "--cached", "--name-only", "-z"],
            cwd=repo.repo_path,
        ).split("\0")
        if path
    ]
    if not staged_paths:
        remote_has_profile = _git_ref_has_path(
            repo.repo_path,
            f"origin/{source_branch}",
            "AGENTS.md",
        )
        if not remote_has_profile:
            source_commit = _git_capture(["rev-parse", "HEAD"], cwd=repo.repo_path).strip()
            _validate_publication_history(repo, source_commit, include_paths=["AGENTS.md"])
            _git(
                ["push", "--quiet", "origin", f"{source_commit}:refs/heads/{source_branch}"],
                cwd=repo.repo_path,
                timeout_s=180,
                env=source_control.git_auth_environment,
                expected_remote_url=repo.clone_url,
            )
            return {
                **base,
                "status": "profile_published",
                "source_branch": source_branch,
                "published_paths": ["AGENTS.md"],
                "change_request_created": False,
            }
        return {
            **base,
            "status": "profile_already_published",
            "source_branch": source_branch,
        }
    if staged_paths != ["AGENTS.md"]:
        raise RuntimeError("Only root AGENTS.md may be staged by the repository-profile publisher.")

    _git(
        [
            "-c",
            "user.name=ET STLC Agent",
            "-c",
            "user.email=et-stlc-agent@localhost",
            "commit",
            "--quiet",
            "-m",
            "docs: define test repository agent practices",
        ],
        cwd=repo.repo_path,
    )
    source_commit = _git_capture(["rev-parse", "HEAD"], cwd=repo.repo_path).strip()
    _validate_publication_history(repo, source_commit, include_paths=["AGENTS.md"])
    _git(
        ["push", "--quiet", "origin", f"{source_commit}:refs/heads/{source_branch}"],
        cwd=repo.repo_path,
        timeout_s=180,
        env=source_control.git_auth_environment,
        expected_remote_url=repo.clone_url,
    )
    return {
        **base,
        "status": "profile_published",
        "source_branch": source_branch,
        "published_paths": ["AGENTS.md"],
        "change_request_created": False,
    }


def _publish_repository_changes(
    repo: CheckoutResult,
    *,
    work_item: ExternalReference,
    source_control: SourceControlAdapter,
    include_paths: Optional[Sequence[str]] = None,
    exclude_paths: Optional[Sequence[str]] = None,
    change_request_mode: str = "ready",
) -> Dict[str, Any]:
    """Publish or resume a reviewed automation change through the stable work-item branch."""
    require_repository_access(repo.provider or source_control.provider, repo.project, repo.repo_name)
    changed_paths: List[str] = []
    publishable_paths: List[str] = []
    excluded_paths: List[Dict[str, str]] = []
    source_branch = repo.work_branch or _work_item_branch_name(work_item, repo.branch)
    base = {
        "repository": repo.repo_name,
        "repository_id": repo.repository_id,
        "target_branch": repo.branch,
        "source_branch": source_branch,
        "role_hint": repo.role_hint,
        "changed_paths": changed_paths,
        "selected_paths": publishable_paths,
        "published_paths": [],
        "excluded_paths": excluded_paths,
    }
    if repo.role_hint != "automation":
        return {
            **base,
            "status": "not_published",
            "error": (
                "Changes were detected outside a dedicated automation repository "
                f"(role hint: {repo.role_hint})."
            ),
        }

    stage = "select_changes"
    committed = False
    branch_pushed = False
    ahead_commits = 0
    try:
        normalized_mode = str(change_request_mode).strip().lower()
        if normalized_mode not in {"ready", "draft"}:
            raise ValueError("change_request_mode must be 'ready' or 'draft'.")
        base["change_request_mode"] = normalized_mode
        source_branch = _checkout_work_item_branch(repo, work_item)
        base_ref = f"refs/remotes/origin/{repo.branch}"
        if not _git_ref_exists(repo.repo_path, base_ref):
            changed_paths = _repository_changed_paths(repo.repo_path)
            publishable_paths, excluded_paths = _select_repository_publication_paths(
                changed_paths,
                include_paths=include_paths,
                exclude_paths=exclude_paths,
            )
            base.update(
                {
                    "source_branch": source_branch,
                    "changed_paths": changed_paths,
                    "selected_paths": publishable_paths,
                    "excluded_paths": excluded_paths,
                }
            )
            if changed_paths and not publishable_paths:
                return {
                    **base,
                    "status": "not_published",
                    "error": "No publishable source or test changes remain after guarded path selection.",
                }
            raise RuntimeError(
                f"Linked target branch origin/{repo.branch} is unavailable in the checkout."
            )
        changed_paths = _repository_changed_paths(repo.repo_path)
        already_committed_paths = _repository_committed_paths_ahead(
            repo.repo_path, repo.branch, base_commit=_publication_base(repo),
        )
        requested_paths = _normalize_requested_repository_paths(include_paths)
        requested_exclusions = _normalize_requested_repository_paths(exclude_paths) or []
        _validate_publication_history(repo, "HEAD", include_paths=requested_paths,
                                      exclude_paths=requested_exclusions)
        known_paths = set(changed_paths) | set(already_committed_paths)
        if requested_paths is not None:
            missing = sorted(set(requested_paths) - known_paths)
            if missing:
                raise ValueError(
                    "Requested publication paths are neither changed nor already committed on the "
                    "work-item branch: " + ", ".join(missing)
                )
        committed_exclusions = sorted(set(requested_exclusions) & set(already_committed_paths))
        if committed_exclusions:
            raise ValueError(
                "Cannot exclude paths already committed on the work-item branch: "
                + ", ".join(committed_exclusions)
            )
        dirty_requested = (
            [path for path in requested_paths if path in changed_paths]
            if requested_paths is not None
            else None
        )
        dirty_exclusions = [path for path in requested_exclusions if path in changed_paths]
        publishable_paths, excluded_paths = _select_repository_publication_paths(
            changed_paths,
            include_paths=dirty_requested,
            exclude_paths=dirty_exclusions,
        )
        committed_publishable, committed_excluded = _select_repository_publication_paths(
            already_committed_paths
        )
        if committed_excluded:
            raise RuntimeError(
                "The work-item branch already contains committed sensitive or run-local paths: "
                + ", ".join(item["path"] for item in committed_excluded)
            )
        selected_paths = list(dict.fromkeys([*committed_publishable, *publishable_paths]))
        base.update(
            {
                "source_branch": source_branch,
                "changed_paths": changed_paths,
                "selected_paths": selected_paths,
                "excluded_paths": excluded_paths,
                "already_committed_paths": already_committed_paths,
            }
        )

        if publishable_paths:
            stage = "commit"
            _assert_publishable_paths(publishable_paths)
            # Clear pre-existing index state so an unrelated staged file cannot hitchhike.
            _git(["reset", "--quiet", "HEAD", "--"], cwd=repo.repo_path)
            _git(["add", "--", *publishable_paths], cwd=repo.repo_path)
            staged_paths = [
                path
                for path in _git_capture(
                    ["diff", "--cached", "--name-only", "-z"],
                    cwd=repo.repo_path,
                ).split("\0")
                if path
            ]
            _assert_publishable_paths(staged_paths)
            if set(staged_paths) != set(publishable_paths):
                raise RuntimeError(
                    "Staged publication paths do not match the reviewed selection: "
                    + ", ".join(staged_paths)
                )
            for path in staged_paths:
                if _git_ref_has_path(repo.repo_path, "", path):
                    _assert_secret_free_blob(repo.repo_path, ":" + path)
            _git(
                [
                    "-c",
                    "user.name=ET STLC Agent",
                    "-c",
                    "user.email=et-stlc-agent@localhost",
                    "commit",
                    "--quiet",
                    "-m",
                    f"test: cover work item {work_item.external_id}",
                ],
                cwd=repo.repo_path,
            )
            committed = True

        source_commit = _git_capture(["rev-parse", "HEAD"], cwd=repo.repo_path).strip()
        ahead_commits = int(
            _git_capture(
                ["rev-list", "--count", f"{_publication_base(repo)}..{source_commit}"],
                cwd=repo.repo_path,
            ).strip()
            or "0"
        )
        base["ahead_commits"] = ahead_commits
        committed_paths = _validate_publication_history(
            repo, source_commit, include_paths=requested_paths, exclude_paths=requested_exclusions,
        )
        committed_publishable, committed_excluded = _select_repository_publication_paths(
            committed_paths
        )
        if committed_excluded:
            raise RuntimeError(
                "Refusing to publish a work-item branch containing committed sensitive or run-local paths: "
                + ", ".join(item["path"] for item in committed_excluded)
            )
        base["selected_paths"] = committed_publishable
        base["already_committed_paths"] = committed_paths

        # A previous call may already have committed its reviewed changes and then failed while
        # pushing or creating the change request. A clean worktree is then resumable, not unchanged.
        if ahead_commits == 0:
            if changed_paths and not publishable_paths:
                return {
                    **base,
                    "status": "not_published",
                    "error": "No publishable source or test changes remain after guarded path selection.",
                }
            return {**base, "status": "unchanged"}

        stage = "push"
        _verify_repository_remote(repo.repo_path, repo.clone_url)
        _git(
            ["push", "--quiet", "origin", f"{source_commit}:refs/heads/{source_branch}"],
            cwd=repo.repo_path,
            timeout_s=180,
            env=source_control.git_auth_environment,
            expected_remote_url=repo.clone_url,
        )
        branch_pushed = True

        stage = "find_or_create_change_request"
        repository_reference = _source_reference(repo, source_control)
        existing_change_request = source_control.find_active_change_request(
            repository_reference,
            source_branch=source_branch,
            target_branch=repo.branch,
        )
        desired_draft = normalized_mode == "draft"
        if (
            existing_change_request
            and bool(existing_change_request.get("is_draft", False)) != desired_draft
            and existing_change_request.get("change_request_id") is not None
        ):
            source_control.set_change_request_draft(
                repository_reference,
                change_request_id=existing_change_request["change_request_id"],
                is_draft=desired_draft,
            )
            existing_change_request["is_draft"] = desired_draft
        change_request = existing_change_request or source_control.create_change_request(
            repository_reference,
            source_branch=source_branch,
            target_branch=repo.branch,
            title=f"WI {work_item.external_id}: update {repo.repo_name} automation",
            description=(
                f"Minimal automated-test changes produced for work item {work_item.external_id}. "
                f"Base branch: {repo.branch}."
                + (f" Work item: {work_item.url}." if work_item.url else "")
            ),
            work_item=work_item,
            is_draft=desired_draft,
        )
        return {
            **base,
            "status": (
                "change_request_updated"
                if existing_change_request
                else "change_request_created"
            ),
            "published_paths": committed_publishable,
            "committed_this_call": committed,
            "branch_pushed": branch_pushed,
            "change_request_id": change_request.get("change_request_id"),
            "change_request_url": change_request.get("web_url"),
        }
    except SecurityPolicyError as exc:
        return {**base, "status": "not_published", "error": str(exc), "retry_same_call": False}
    except Exception as exc:
        recovery_by_stage = {
            "select_changes": "Correct the reviewed include/exclude paths from changed_paths and retry once.",
            "commit": "The worktree is preserved. Inspect the reported git error, correct it, and retry publication once.",
            "push": "The local work-item commit is preserved. Retry the same publication call; do not recreate the changes.",
            "find_or_create_change_request": (
                "The work-item branch is pushed. Retry the same publication call to find or create the change request."
            ),
        }
        return {
            **base,
            "status": "retryable_failure",
            "failed_stage": stage,
            "error": str(exc),
            "committed_this_call": committed,
            "branch_pushed": branch_pushed,
            "ahead_commits": ahead_commits,
            "retry_same_call": stage in {"push", "find_or_create_change_request"},
            "recovery_action": recovery_by_stage[stage],
            "retry_arguments": {
                "repository_id": repo.repository_id,
                "target_branch": repo.branch,
                "include_paths_csv": ",".join(include_paths or []),
                "exclude_paths_csv": ",".join(exclude_paths or []),
                "mode": str(change_request_mode).strip().lower() or "ready",
            },
        }


def _make_repository_publication_tools(
    repos: Sequence[CheckoutResult],
    *,
    work_item: ExternalReference,
    source_control: SourceControlAdapter,
) -> List[Any]:
    """Expose guarded publication to the main supervisor so reporting remains agent-driven."""
    by_key = {(repo.repository_id, repo.branch): repo for repo in repos}
    by_id: Dict[str, List[CheckoutResult]] = {}
    for repo in repos:
        by_id.setdefault(str(repo.repository_id), []).append(repo)

    def find_repo(repository_id: str, branch: str) -> Optional[CheckoutResult]:
        return by_key.get((str(repository_id), str(branch)))

    def not_found(repository_id: str, branch: str) -> Dict[str, Any]:
        return {
            "status": "repository_not_found",
            "repository_id": repository_id,
            "branch": branch,
            "available": [
                {"repository_id": item.repository_id, "branch": item.branch, "name": item.repo_name}
                for item in repos
            ],
        }

    @tool
    def repository_profile_publish(
        repository_id: str,
        branch: str,
    ) -> Dict[str, Any]:
        """Commit and push a newly authored root AGENTS.md to the stable work-item branch.

        Use during requirements analysis only after the executor verifies a dedicated test or
        automation repository, finds no existing root AGENTS.md, and authors a compact, durable,
        evidence-based repository profile. This tool stages only AGENTS.md, never opens a change request, and
        refuses application/mixed/unknown repositories or agent-authored updates to a profile that
        already exists on the linked base branch. Every outgoing commit must affect only AGENTS.md;
        unrelated existing commits are preserved locally and prevent bootstrap publication.
        """
        repo = find_repo(repository_id, branch)
        if repo is None:
            return not_found(repository_id, branch)
        try:
            return _publish_test_repository_profile(
                repo,
                work_item=work_item,
                source_control=source_control,
            )
        except Exception as exc:
            logger.exception("Failed to publish test-repository profile for %s", repo.repo_name)
            return {
                "repository": repo.repo_name,
                "repository_id": repo.repository_id,
                "branch": repo.branch,
                "role_hint": repo.role_hint,
                "status": "publish_failed",
                "error": str(exc),
            }

    @tool
    def repository_changes_publish(
        repository_id: str,
        include_paths_csv: str = "",
        exclude_paths_csv: str = "",
        target_branch: str = "",
        mode: str = "ready",
    ) -> Dict[str, Any]:
        """Publish reviewed changes for one linked repository and return change-request/no-change details.

        Identify the repository by repository_id. The linked target branch and stable work-item
        source branch are selected internally; target_branch is an optional validation override and
        must equal the linked target. Provide comma-separated include paths when the workspace mixes
        intended source/test changes with temporary files. Include paths must cover every changed
        path in outgoing commits, including an earlier AGENTS.md profile commit if still ahead of
        the linked target. Committed exclusions or prohibited historical files require branch review;
        this tool does not rewrite retained work. The operation is idempotent: retry a
        retryable_failure to resume a committed or pushed publication. Set mode to ready or draft;
        draft is appropriate when code is coherent but live validation remains externally blocked.
        Secret/auth paths and generated execution artifacts are always left local.
        """
        matches = by_id.get(str(repository_id), [])
        requested_target = str(target_branch).strip()
        normalized_mode = str(mode).strip().lower()
        if not matches:
            return {
                "status": "repository_not_found",
                "repository_id": repository_id,
                "available": [
                    {
                        "repository_id": item.repository_id,
                        "target_branch": item.branch,
                        "source_branch": item.work_branch,
                        "name": item.repo_name,
                    }
                    for item in repos
                ],
                "retry_same_call": False,
            }
        if normalized_mode not in {"ready", "draft"}:
            return {
                "status": "invalid_change_request_mode",
                "repository_id": repository_id,
                "requested_mode": mode,
                "allowed_modes": ["ready", "draft"],
                "retry_same_call": True,
                "recovery_action": "Retry with mode set to ready or draft.",
            }
        if len(matches) > 1 and requested_target:
            matches = [item for item in matches if item.branch == requested_target]
        if len(matches) != 1:
            return {
                "status": "ambiguous_repository",
                "repository_id": repository_id,
                "targets": [item.branch for item in matches],
                "retry_same_call": False,
            }
        repo = matches[0]
        if requested_target and requested_target != repo.branch:
            return {
                "status": "invalid_target_branch",
                "repository_id": repo.repository_id,
                "requested_target_branch": requested_target,
                "target_branch": repo.branch,
                "source_branch": repo.work_branch or _work_item_branch_name(work_item, repo.branch),
                "retry_same_call": True,
                "recovery_action": (
                    "Retry with target_branch omitted or set to the linked target branch. "
                    "Never pass the work-item source branch as target_branch."
                ),
                "retry_arguments": {
                    "repository_id": repo.repository_id,
                    "target_branch": repo.branch,
                    "include_paths_csv": include_paths_csv,
                    "exclude_paths_csv": exclude_paths_csv,
                    "mode": mode,
                },
            }
        try:
            return _publish_repository_changes(
                repo,
                work_item=work_item,
                source_control=source_control,
                include_paths=[path.strip() for path in include_paths_csv.split(",") if path.strip()]
                or None,
                exclude_paths=[path.strip() for path in exclude_paths_csv.split(",") if path.strip()]
                or None,
                change_request_mode=normalized_mode,
            )
        except Exception as exc:
            logger.exception("Failed to publish changes for repository %s", repo.repo_name)
            return {
                "repository": repo.repo_name,
                "repository_id": repo.repository_id,
                "branch": repo.branch,
                "role_hint": repo.role_hint,
                "status": "publish_failed",
                "error": str(exc),
            }

    return [repository_profile_publish, repository_changes_publish]


def _set_work_item_agent_status(
    work_tracking: WorkTrackingAdapter,
    work_item: Optional[ExternalReference],
    status: Literal["Queued", "Running", "Succeeded", "Failed"],
) -> None:
    """
    Deterministic integration status update. Safe to call on every path: logs and does not raise.
    """
    if work_item is None:
        return
    try:
        _validate_work_item_reference(work_tracking, work_item)
        work_tracking.set_agent_status(work_item.external_id, status)
    except Exception:
        logger.exception("Failed to set work item %s agent status to %s", work_item.external_id, status)


def _comment_on_blocked_work_item(work_tracking: WorkTrackingAdapter, work_item: ExternalReference,
                                 comment: Optional[str] = None) -> None:
    """Post the fixed input-block notification without asking an agent to compose it."""
    from src.prompt_injection import DIRECT_BLOCKED_COMMENT
    _validate_work_item_reference(work_tracking, work_item)
    comment_tool = work_tracking.make_tools(read_only=False).get("work_comment_add")
    if comment_tool is None:
        raise SecurityPolicyError("Work tracking does not expose the canonical work_comment_add tool")
    comment_tool.invoke({"id": work_item.external_id, "comment": comment or DIRECT_BLOCKED_COMMENT})


def _screen_direct_inputs(prompt: str, work_tracking: WorkTrackingAdapter, work_item, reviewer, *, request_data=None):
    """Screen primary work-item text before extraction, checkout or graph execution."""
    from src.prompt_injection import DIRECT_BLOCKED_MESSAGE, PromptInjectionBlockedError, ReviewOutcome
    outcome = reviewer.review(prompt if request_data is None else request_data, source="work_item_request")
    if not outcome.allowed:
        reviewer.reject_direct(outcome)
        raise PromptInjectionBlockedError(DIRECT_BLOCKED_MESSAGE)
    if work_item is None:
        return None
    try:
        context = work_tracking.get_work_item_context(work_item.external_id)
        _validate_work_item_reference(work_tracking, context.reference)
    except Exception as exc:
        reviewer.reject_direct(ReviewOutcome("review_unavailable", "work_item_fetch", error_type=type(exc).__name__))
        raise PromptInjectionBlockedError(DIRECT_BLOCKED_MESSAGE) from exc
    reviewer.work_item_ids.add(context.reference.external_id.strip().casefold())
    content = {
        "title": context.title, "details": context.text, "comments": context.comments,
        "comment_records": context.metadata.get("comment_records", ()),
    }
    outcome = reviewer.review(content, source="work_item_fields")
    if not outcome.allowed:
        reviewer.reject_direct(outcome)
        raise PromptInjectionBlockedError(DIRECT_BLOCKED_MESSAGE)
    return context


def activate_work_item_if_new(
    work_tracking: WorkTrackingAdapter,
    work_item: Optional[ExternalReference],
) -> None:
    if work_item is None:
        return
    try:
        _validate_work_item_reference(work_tracking, work_item)
        work_tracking.activate_if_new(work_item.external_id)
    except Exception:
        logger.exception("Failed to auto-activate work item %s", work_item.external_id)


def _shorten(value: Any, limit: int = 1500) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    if len(text) <= limit:
        return text
    return text[: limit - 20] + "\n... [truncated]"


async def _collect_agent_trace(supervisor, prompt: str) -> Dict[str, Any]:
    blocks: list[Dict[str, str]] = []
    open_block: Optional[Dict[str, str]] = None
    tool_events: list[Dict[str, str]] = []
    agents_seen: list[str] = []
    tools_seen: list[str] = []
    last_agent: Optional[str] = None
    last_response: Optional[str] = None

    def flush_open_block() -> None:
        nonlocal open_block
        if open_block and open_block.get("content", "").strip():
            blocks.append(open_block)
        open_block = None

    async for event in supervisor_events(supervisor, [HumanMessage(content=prompt)]):
        event_type = event.get("type")
        if event_type == "end":
            break

        agent_name = event.get("agent") or last_agent or "unknown"
        if event.get("agent"):
            last_agent = event["agent"]
            agents_seen.append(event["agent"])

        if event_type in {"reasoning_chunk", "response_chunk"}:
            content = event.get("content") or ""
            block_type = "reasoning" if event_type == "reasoning_chunk" else "response"
            if open_block and open_block["type"] == block_type and open_block["agent"] == agent_name:
                open_block["content"] += content
            else:
                flush_open_block()
                open_block = {"type": block_type, "agent": agent_name, "content": content}
            continue

        flush_open_block()
        if event_type == "tool_call":
            tool_name = str((event.get("raw") or {}).get("name") or "unknown")
            tools_seen.append(tool_name)
            tool_events.append(
                {
                    "type": "tool_call",
                    "agent": agent_name,
                    "name": tool_name,
                    "payload": _shorten((event.get("raw") or {}).get("input", {})),
                }
            )
        elif event_type == "tool_result":
            tool_events.append(
                {
                    "type": "tool_result",
                    "agent": agent_name,
                    "name": str((event.get("raw") or {}).get("name") or "unknown"),
                    "payload": _shorten((event.get("raw") or {}).get("output", "")),
                }
            )
        elif event_type == "response_end":
            last_response = event.get("content") or last_response

    flush_open_block()
    return {
        "blocks": blocks,
        "tool_events": tool_events,
        "agents_seen": list(dict.fromkeys(agents_seen)),
        "tool_counts": dict(Counter(tools_seen)),
        "last_response": last_response or "",
    }


def _format_trace_markdown(
    prompt: str,
    trace: Dict[str, Any],
    *,
    work_item: Optional[ExternalReference] = None,
    event_type: Optional[str] = None,
    capture: Optional[Dict[str, Any]] = None,
) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    lines = [
        "# Agent Execution Trace",
        "",
        f"- Timestamp (UTC): `{timestamp}`",
    ]
    if work_item is not None:
        lines.extend(
            [
                f"- Work item: `{work_item.provider}/{work_item.external_id}`",
                f"- Work-tracking connection: `{work_item.connection_id}`",
            ]
        )
    if event_type:
        lines.append(f"- Event type: `{event_type}`")
    if capture:
        active = [name for name, matched in capture.items() if matched]
        if active:
            lines.append(f"- <event-trigger> matches: `{', '.join(active)}`")

    lines.extend(
        [
            "",
            "## Input",
            "```text",
            prompt,
            "```",
            "",
            "## Summary",
            f"- Agents involved: `{', '.join(trace.get('agents_seen') or ['unknown'])}`",
            f"- Tools used: `{json.dumps(trace.get('tool_counts') or {}, ensure_ascii=False)}`",
        ]
    )

    last_response = str(trace.get("last_response") or "").strip()
    if last_response:
        lines.extend(
            [
                f"- Final response excerpt: `{_shorten(last_response, 400)}`",
            ]
        )

    lines.extend(["", "## Trajectory"])
    step_no = 1
    for block in trace.get("blocks", []):
        lines.extend(
            [
                f"### {step_no}. {block['type']}",
                "```text",
                _shorten(block.get("content", ""), 2500),
                "```",
            ]
        )
        step_no += 1
    for event in trace.get("tool_events", []):
        label = "tool call" if event["type"] == "tool_call" else "tool result"
        lines.extend(
            [
                f"### {step_no}. {event['agent']} {label}: `{event['name']}`",
                "```text",
                event["payload"],
                "```",
            ]
        )
        step_no += 1

    return "\n".join(lines).strip() + "\n"


def _format_failure_trace_markdown(
    prompt: str,
    error: Exception,
    *,
    work_item: Optional[ExternalReference] = None,
    event_type: Optional[str] = None,
    capture: Optional[Dict[str, Any]] = None,
    review_diagnostics: Optional[Dict[str, Any]] = None,
    trace: Optional[Dict[str, Any]] = None,
) -> str:
    from src.prompt_injection import PromptInjectionBlockedError
    blocked = isinstance(error, PromptInjectionBlockedError)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    lines = [
        "# Agent Execution Trace",
        "",
        f"- Timestamp (UTC): `{timestamp}`",
        f"- Execution status: `{'blocked' if blocked else 'failed'}`",
    ]
    if work_item is not None:
        lines.extend(
            [
                f"- Work item: `{work_item.provider}/{work_item.external_id}`",
                f"- Work-tracking connection: `{work_item.connection_id}`",
            ]
        )
    if event_type:
        lines.append(f"- Event type: `{event_type}`")
    if capture:
        active = [name for name, matched in capture.items() if matched]
        if active:
            lines.append(f"- <event-trigger> matches: `{', '.join(active)}`")
    if review_diagnostics:
        for key, value in review_diagnostics.items():
            if value is not None:
                lines.append(f"- Safety review {key}: `{value}`")
    lines.extend(
        [
            f"- {'Stop' if blocked else 'Error'} type: `{type(error).__name__}`",
            f"- {'Stop' if blocked else 'Error'} message: `{_shorten(str(error), 500)}`",
            "",
            "## Input",
            "```text",
            prompt,
            "```",
        ]
    )
    if trace:
        lines.extend(["", "## Interrupted tools", f"- Tools used: `{json.dumps(trace.get('tool_counts') or {})}`"])
        for event in trace.get("tool_events", []):
            lines.extend(["", f"### {event['type']}: `{event['name']}`", "```text", event["payload"], "```"])
    return "\n".join(lines).strip() + "\n"


def _write_trace_artifact(
    trace_storage_dir: Path,
    markdown: str,
    work_item: Optional[ExternalReference],
) -> Path:
    trace_storage_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    wi_part = f"WI-{_work_item_reference_key(work_item)}_" if work_item is not None else ""
    destination = trace_storage_dir / f"{wi_part}{timestamp}_agent_trace.md"
    from src.observability import redact
    destination.write_text(redact(markdown), encoding="utf-8")
    return destination


def run_agent_prompt(
    prompt: str,
    *,
    base_path: Optional[Path] = None,
    work_item: Optional[ExternalReference] = None,
    event_type: Optional[str] = None,
    capture: Optional[Dict[str, Any]] = None,
    integration_bundle: Optional[IntegrationBundle] = None,
    raise_errors: bool = False,
    request_data: Optional[Dict[str, Any]] = None,
) -> None:
    """Run once, serializing writers to the same retained workspace across processes."""
    root = _agent_workspace_directory(work_item, base_path)
    lock_dir = get_workspace_base() / ".runtime-locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(str(root).encode()).hexdigest()
    with (lock_dir / f"{key}.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            _run_agent_prompt(
                prompt, base_path=base_path, work_item=work_item,
                event_type=event_type, capture=capture,
                integration_bundle=integration_bundle, raise_errors=raise_errors,
                request_data=request_data,
            )
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _run_agent_prompt(
    prompt: str,
    *,
    base_path: Optional[Path] = None,
    work_item: Optional[ExternalReference] = None,
    event_type: Optional[str] = None,
    capture: Optional[Dict[str, Any]] = None,
    integration_bundle: Optional[IntegrationBundle] = None,
    raise_errors: bool = False,
    request_data: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Invoke the supervisor once with a single user message containing ``prompt``.
    Logs errors and writes a trace. Managed jobs set ``raise_errors`` so the job
    handler can return a blocked outcome or propagate an unexpected failure;
    direct legacy callers retain non-raising behavior.

    Filesystem and integration download paths resolve under a deterministic, provider-aware
    ``<workspace>/workitems/<reference-key>/`` directory, or a fresh ``workitems/_adhoc/run-<uuid>/``
    folder when no work-item reference is present (unless ``base_path`` is passed). The explicit
    all-Azure profile preserves the original numeric directory names. Execution traces are written
    under ``<workspace>/_trace_store/`` (outside those directories).

    Browser and repository automation run through the native Andromeda workspace session.
    """
    agent_workspace = _agent_workspace_directory(work_item, base_path)
    external_prompt = prompt
    prompt = _augment_prompt_with_workspace_contract(prompt)
    trace_storage_dir = get_internal_trace_store_dir()
    trace_path: Optional[Path] = None
    repo_contexts: List[CheckoutResult] = []
    integrations: Optional[IntegrationBundle] = None
    reviewer = None
    try:
        integrations = integration_bundle or build_integration_bundle(
            agent_workspace,
            project_override=work_item.namespace if work_item is not None else None,
        )
        if work_item is not None:
            _validate_work_item_reference(integrations.work_tracking, work_item)
        _set_work_item_agent_status(integrations.work_tracking, work_item, "Queued")
        from src.prompt_injection import InjectionReviewer, injection_review_scope
        reviewer = InjectionReviewer(
            work_item_id=work_item.external_id if work_item is not None else None,
            notify_direct_block=(
                lambda: _comment_on_blocked_work_item(integrations.work_tracking, work_item, reviewer.blocked_comment)
            ) if work_item is not None else None,
        )
        work_item_context = _screen_direct_inputs(external_prompt, integrations.work_tracking, work_item, reviewer,
                                                request_data=request_data)
        activate_work_item_if_new(integrations.work_tracking, work_item)
        _set_work_item_agent_status(integrations.work_tracking, work_item, "Running")

        if work_item is not None:
            try:
                image_result = prepare_work_item_images(
                    integrations.work_tracking,
                    work_item,
                )
                prompt = _augment_prompt_with_prepared_images(prompt, image_result, reviewer=reviewer)
            except Exception:
                logger.exception(
                    "Work-item image extraction failed for %s; continuing without prepared images",
                    work_item.external_id,
                )

        # Resolve and clone every repository into the isolated workspace. Individual
        # checkout failures remain non-fatal so other repositories stay usable.
        if base_path is None and work_item is not None:
            try:
                prompt = _augment_prompt_with_coverage_candidates(prompt, work_item_context)
                repo_contexts = prepare_work_item_repositories(
                    integrations.source_control,
                    work_item_context,
                    agent_workspace,
                )
            except Exception:
                logger.exception(
                    "Repository discovery failed for work item %s; continuing without code workspace",
                    work_item.external_id,
                )
                repo_contexts = []
            if repo_contexts:
                prompt = _augment_prompt_with_repositories(prompt, repo_contexts, reviewer=reviewer)

        runtime_tools = (
            _make_repository_publication_tools(
                repo_contexts,
                work_item=work_item,
                source_control=integrations.source_control,
            )
            if work_item is not None and repo_contexts
            and Capability.SOURCE_CHANGE_REQUESTS in integrations.source_control.capabilities
            else []
        )
        reviewer.register_runtime_prompt(prompt)
        with injection_review_scope(reviewer):
            trace = run_with_responsive_loop(
                _run_agent_trace_async(
                    prompt,
                    agent_workspace,
                    work_item.namespace if work_item is not None else None,
                    integrations,
                    runtime_tools=runtime_tools,
                )
            )
        markdown = _format_trace_markdown(
            prompt,
            trace,
            work_item=work_item,
            event_type=event_type,
            capture=capture,
        )
        trace_path = _write_trace_artifact(trace_storage_dir, markdown, work_item)
        _set_work_item_agent_status(integrations.work_tracking, work_item, "Succeeded")
        logger.info("Integration webhook agent completed; trace_path=%s", trace_path)
    except Exception as exc:
        from src.prompt_injection import PromptInjectionBlockedError
        blocked = isinstance(exc, PromptInjectionBlockedError)
        if blocked:
            logger.warning("Integration webhook agent blocked by work-item safety check")
        else:
            logger.exception("Integration webhook agent failed")
        if integrations is not None:
            _set_work_item_agent_status(integrations.work_tracking, work_item, "Failed")
        try:
            if trace_path is None:
                failure_markdown = _format_failure_trace_markdown(
                    prompt,
                    exc,
                    work_item=work_item,
                    event_type=event_type,
                    capture=capture,
                    review_diagnostics=reviewer.rejection.diagnostics() if blocked and reviewer and reviewer.rejection else None,
                    trace=getattr(exc, "trace", None),
                )
                trace_path = _write_trace_artifact(trace_storage_dir, failure_markdown, work_item)
            logger.info(
                "Integration webhook %s trace stored; trace_path=%s",
                "blocked" if blocked else "failure", trace_path,
            )
        except Exception:
            logger.exception("Integration agent failed and failure trace storage also failed")
            if blocked and raise_errors:
                raise
        if raise_errors:
            raise


def schedule_agent_prompt(
    prompt: str,
    *,
    work_item: Optional[ExternalReference] = None,
    event_type: Optional[str] = None,
    capture: Optional[Dict[str, Any]] = None,
    delivery_id: Optional[str] = None,
    request_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a serializable run reference only after runtime acceptance."""
    from src.runtime import submit_agent_prompt

    return submit_agent_prompt(
        prompt, work_item=work_item, event_type=event_type,
        capture=capture, delivery_id=delivery_id,
        request_data=request_data,
    )
