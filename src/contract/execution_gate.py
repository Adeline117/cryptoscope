"""Single fail-closed gate for every exchange state-changing action."""
from __future__ import annotations

import hmac
import os


class ExecutionBlocked(RuntimeError):
    """Raised before an exchange client is reached."""


def execution_mode() -> str:
    mode = os.getenv("CRYPTOSCOPE_EXECUTION_MODE", "research").strip().lower()
    return mode if mode in {"research", "paper", "live"} else "research"


def require_live_approval(action: str, approval_token: str | None) -> None:
    """Require both an explicit live mode and an out-of-band manual token."""
    mode = execution_mode()
    if mode != "live":
        raise ExecutionBlocked(f"{action} blocked: execution mode is {mode}")
    expected = os.getenv("CRYPTOSCOPE_LIVE_APPROVAL_TOKEN", "")
    if not expected:
        raise ExecutionBlocked(f"{action} blocked: live approval is not configured")
    if not approval_token or not hmac.compare_digest(str(approval_token), expected):
        raise ExecutionBlocked(f"{action} blocked: manual approval is missing or invalid")
