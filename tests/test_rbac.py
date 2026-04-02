"""
tests/test_rbac.py — RBAC module tests.

Covers: hash determinism, key resolution, role enforcement,
separation of duties, and admin superset access.
"""

import uuid
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from shared.rbac import (
    hash_api_key,
    resolve_api_key,
    require_role,
    check_separation_of_duties,
    match_route_role,
)
from tests.conftest import make_api_key


class TestHashApiKey:
    def test_deterministic(self):
        assert hash_api_key("key-1") == hash_api_key("key-1")

    def test_different_keys_differ(self):
        assert hash_api_key("key-1") != hash_api_key("key-2")

    def test_returns_64_char_hex(self):
        h = hash_api_key("test")
        assert len(h) == 64
        int(h, 16)  # validates hex


class TestResolveApiKey:
    def test_found(self, db):
        key = make_api_key(db, raw_key="resolve-test-key")
        result = resolve_api_key(db, "resolve-test-key")
        assert result is not None
        assert result.id == key.id

    def test_not_found(self, db):
        result = resolve_api_key(db, "nonexistent-key")
        assert result is None

    def test_inactive_key_not_returned(self, db):
        make_api_key(db, raw_key="inactive-key", is_active=False)
        result = resolve_api_key(db, "inactive-key")
        assert result is None


class TestRequireRole:
    @pytest.mark.asyncio
    async def test_allowed_role(self):
        dep = require_role("admin", "trader")
        request = MagicMock()
        request.headers = {
            "X-Actor-Role": "admin",
            "X-Actor-ID": str(uuid.uuid4()),
        }
        result = await dep(request)
        assert result["role"] == "admin"

    @pytest.mark.asyncio
    async def test_denied_role(self):
        dep = require_role("admin", "trader")
        request = MagicMock()
        request.headers = {
            "X-Actor-Role": "auditor",
            "X-Actor-ID": str(uuid.uuid4()),
        }
        with pytest.raises(HTTPException) as exc_info:
            await dep(request)
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_has_broad_access(self):
        dep = require_role("admin", "approver")
        request = MagicMock()
        request.headers = {
            "X-Actor-Role": "admin",
            "X-Actor-ID": str(uuid.uuid4()),
        }
        result = await dep(request)
        assert result["role"] == "admin"


class TestSeparationOfDuties:
    def test_different_actors_pass(self):
        check_separation_of_duties(
            str(uuid.uuid4()), str(uuid.uuid4())
        )

    def test_same_actor_raises(self):
        actor = str(uuid.uuid4())
        with pytest.raises(HTTPException) as exc_info:
            check_separation_of_duties(actor, actor)
        assert exc_info.value.status_code == 403


class TestMatchRouteRole:
    def test_tokens_issue(self):
        roles = match_route_role("/v1/tokens/issue")
        assert "admin" in roles
        assert "trader" in roles

    def test_approve_route(self):
        roles = match_route_role("/settlements/abc/approve")
        assert "approver" in roles
        assert "admin" in roles

    def test_sign_route(self):
        roles = match_route_role("/settlements/abc/sign")
        assert "signer" in roles
        assert "admin" in roles

    def test_unknown_route(self):
        roles = match_route_role("/unknown/route")
        assert roles is None
