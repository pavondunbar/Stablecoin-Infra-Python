"""
shared/context.py — Request-scoped audit context via contextvars.

Extracts actor identity and request correlation headers from incoming
HTTP requests and makes them available to downstream code via
thread-safe context variables.
"""

import contextvars
import uuid
from typing import Optional

from fastapi import Request

_request_id_var: contextvars.ContextVar[Optional[uuid.UUID]] = (
    contextvars.ContextVar("request_id", default=None)
)
_actor_id_var: contextvars.ContextVar[Optional[uuid.UUID]] = (
    contextvars.ContextVar("actor_id", default=None)
)
_actor_service_var: contextvars.ContextVar[Optional[str]] = (
    contextvars.ContextVar("actor_service", default=None)
)


def _parse_uuid(value: str) -> Optional[uuid.UUID]:
    """Parse a string to UUID, returning None on failure."""
    if not value:
        return None
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError):
        return None


def extract_context(request: Request) -> dict:
    """Read audit headers from the request and set contextvars.

    Returns a dict with typed values suitable for passing to model
    constructors.
    """
    request_id = _parse_uuid(
        request.headers.get("X-Request-ID", "")
    )
    actor_id = _parse_uuid(
        request.headers.get("X-Actor-ID", "")
    )
    actor_service = (
        request.headers.get("X-Actor-Service", "") or None
    )

    _request_id_var.set(request_id)
    _actor_id_var.set(actor_id)
    _actor_service_var.set(actor_service)

    return {
        "request_id": request_id,
        "actor_id": actor_id,
        "actor_service": actor_service,
    }


def get_context() -> dict:
    """Read current context from contextvars."""
    return {
        "request_id": _request_id_var.get(),
        "actor_id": _actor_id_var.get(),
        "actor_service": _actor_service_var.get(),
    }
