"""
tests/test_outbox.py — Tests for the transactional outbox module.

Covers:
  - Event insertion with proper fields
  - Pydantic model serialization
  - Dict event serialization
  - Aggregate ID assignment
  - Published_at starts as NULL
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.models import OutboxEvent
from shared.outbox import insert_outbox_event
from tests.conftest import make_account


class TestInsertOutboxEvent:

    def test_inserts_event_row(self, db, omnibus):
        event = insert_outbox_event(
            db, "AGG-001", "token.issuance.completed",
            {"account_id": "abc", "amount": "1000"},
        )
        assert event.id is not None
        assert event.aggregate_id == "AGG-001"
        assert event.event_type == "token.issuance.completed"

    def test_payload_is_serialized(self, db, omnibus):
        payload = {"key": "value", "nested": {"a": 1}}
        event = insert_outbox_event(db, "AGG-002", "test.event", payload)
        assert event.payload == payload

    def test_published_at_is_null(self, db, omnibus):
        event = insert_outbox_event(
            db, "AGG-003", "test.unpublished", {"data": True},
        )
        assert event.published_at is None

    def test_created_at_is_set(self, db, omnibus):
        event = insert_outbox_event(db, "AGG-004", "test.timing", {})
        assert event.created_at is not None

    def test_multiple_events_for_same_aggregate(self, db, omnibus):
        insert_outbox_event(db, "MULTI-AGG", "event.one", {"step": 1})
        insert_outbox_event(db, "MULTI-AGG", "event.two", {"step": 2})

        events = db.execute(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == "MULTI-AGG")
        ).scalars().all()
        assert len(events) == 2

    def test_pydantic_model_serialization(self, db, omnibus):
        """Test that Pydantic-like objects with model_dump() are handled."""

        class FakeEvent:
            def model_dump(self, mode=None):
                return {"field": "value", "amount": "100"}

        event = insert_outbox_event(db, "PYDANTIC-001", "test.pydantic", FakeEvent())
        assert event.payload == {"field": "value", "amount": "100"}

    def test_event_with_dict_method(self, db, omnibus):
        """Test backward compat with objects having .dict() (Pydantic v1)."""

        class LegacyEvent:
            def dict(self):
                return {"legacy": True, "count": 42}

        event = insert_outbox_event(db, "LEGACY-001", "test.legacy", LegacyEvent())
        assert event.payload["legacy"] is True
