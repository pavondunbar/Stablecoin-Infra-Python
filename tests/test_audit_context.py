"""
tests/test_audit_context.py — Audit trail context tests.

Covers: header extraction, missing header defaults,
contextvars lifecycle, and audit fields persisted on models.
"""

import uuid

from unittest.mock import MagicMock

from shared.context import extract_context, get_context
from shared.models import (
    RTGSSettlement, RTGSSettlementStatusHistory,
    SettlementStatus, CurrencyCode,
)
from shared.status import record_status


class TestExtractContext:
    def test_extracts_all_headers(self):
        request = MagicMock()
        req_id = str(uuid.uuid4())
        actor_id = str(uuid.uuid4())
        request.headers = {
            "X-Request-ID": req_id,
            "X-Actor-ID": actor_id,
            "X-Actor-Service": "api-gateway",
        }

        ctx = extract_context(request)
        assert str(ctx["request_id"]) == req_id
        assert str(ctx["actor_id"]) == actor_id
        assert ctx["actor_service"] == "api-gateway"

    def test_missing_headers_return_none(self):
        request = MagicMock()
        request.headers = {}

        ctx = extract_context(request)
        assert ctx["request_id"] is None
        assert ctx["actor_id"] is None
        assert ctx["actor_service"] is None

    def test_invalid_uuid_returns_none(self):
        request = MagicMock()
        request.headers = {
            "X-Request-ID": "not-a-uuid",
            "X-Actor-ID": "also-not-uuid",
            "X-Actor-Service": "test",
        }

        ctx = extract_context(request)
        assert ctx["request_id"] is None
        assert ctx["actor_id"] is None
        assert ctx["actor_service"] == "test"


class TestGetContext:
    def test_returns_values_after_extract(self):
        request = MagicMock()
        req_id = str(uuid.uuid4())
        request.headers = {
            "X-Request-ID": req_id,
            "X-Actor-ID": str(uuid.uuid4()),
            "X-Actor-Service": "test-svc",
        }
        extract_context(request)

        ctx = get_context()
        assert str(ctx["request_id"]) == req_id
        assert ctx["actor_service"] == "test-svc"


class TestAuditFieldsPersisted:
    def test_status_history_stores_audit_fields(self, db, alice, omnibus):
        """Audit fields are persisted on status history rows."""
        req_id = uuid.uuid4()
        actor_id = uuid.uuid4()

        settlement = RTGSSettlement(
            settlement_ref=f"AUDIT-{uuid.uuid4().hex[:8]}",
            sending_account_id=alice.id,
            receiving_account_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
            currency=CurrencyCode.USD,
            amount=1000,
            priority=5,
            status=SettlementStatus.PENDING,
        )
        db.add(settlement)
        db.flush()

        row = record_status(
            db, RTGSSettlementStatusHistory,
            "settlement_id", settlement.id,
            SettlementStatus.PENDING.value,
            request_id=req_id,
            actor_id=actor_id,
            actor_service="rtgs",
        )

        assert row.request_id == req_id
        assert row.actor_id == actor_id
        assert row.actor_service == "rtgs"

    def test_model_audit_columns_nullable(self, db, alice, omnibus):
        """Audit columns accept None for backward compat."""
        settlement = RTGSSettlement(
            settlement_ref=f"AUDIT2-{uuid.uuid4().hex[:8]}",
            sending_account_id=alice.id,
            receiving_account_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
            currency=CurrencyCode.USD,
            amount=2000,
            priority=5,
            status=SettlementStatus.PENDING,
            request_id=None,
            actor_id=None,
            actor_service=None,
        )
        db.add(settlement)
        db.flush()

        assert settlement.request_id is None
        assert settlement.actor_id is None
        assert settlement.actor_service is None
