"""
shared/rbac.py — Role-Based Access Control for the stablecoin platform.

Provides API key hashing, lookup, role enforcement via FastAPI
dependencies, and separation-of-duties validation.
"""

import hashlib
from typing import Optional

from fastapi import HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session


ROUTE_ROLE_MAP = {
    "/tokens/issue":   {"admin", "trader"},
    "/tokens/redeem":  {"admin", "trader"},
    "/tokens/balance": {"admin", "trader", "auditor"},
    "/accounts":       {"admin"},
    "/settlements/submit": {"admin", "trader"},
    "/payments/conditional": {"admin", "trader"},
    "/payments/escrow":      {"admin", "trader"},
    "/fx/settle":            {"admin", "trader"},
    "/fx/quote":             {"admin", "trader"},
    "/fx/rates":             {"admin", "trader", "auditor"},
}

APPROVE_ROLES = {"admin", "approver"}
SIGN_ROLES = {"admin", "signer"}


def hash_api_key(raw_key: str) -> str:
    """SHA-256 hash for API key storage and lookup."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def resolve_api_key(db: Session, raw_key: str):
    """Look up an active API key by its hash."""
    from shared.models import ApiKey

    key_hash = hash_api_key(raw_key)
    return db.execute(
        select(ApiKey).where(
            ApiKey.key_hash == key_hash,
            ApiKey.is_active.is_(True),
        )
    ).scalar_one_or_none()


def require_role(*allowed_roles: str):
    """FastAPI dependency factory that enforces role membership.

    Reads X-Actor-Role and X-Actor-ID headers set by the gateway.
    Returns 403 if the actor's role is not in the allowed set.
    """
    async def _dependency(request: Request):
        role = request.headers.get("X-Actor-Role", "")
        actor_id = request.headers.get("X-Actor-ID", "")
        if role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Role '{role}' not authorized. "
                    f"Required: {', '.join(allowed_roles)}"
                ),
            )
        return {"role": role, "actor_id": actor_id}
    return _dependency


def check_separation_of_duties(
    approved_by: str, signed_by: str
) -> None:
    """Raise 403 if the same actor approved and signed."""
    if approved_by == signed_by:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Separation of duties: approver and signer must differ",
        )


def match_route_role(path: str) -> Optional[set[str]]:
    """Find the allowed roles for a given request path."""
    if "/approve" in path:
        return APPROVE_ROLES
    if "/sign" in path:
        return SIGN_ROLES
    for route_prefix, roles in ROUTE_ROLE_MAP.items():
        if route_prefix in path:
            return roles
    return None
