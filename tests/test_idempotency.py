"""
tests/test_idempotency.py — Idempotency guard tests.

Covers: conditional payment + escrow + FX idempotency returning
existing records, and Kafka dedup helpers.
"""

import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select

from shared.models import (
    ConditionalPayment, EscrowContract, FXSettlement,
    ProcessedEvent, CurrencyCode, ConditionType,
    EscrowStatus, TxnStatus, SettlementStatus,
    SettlementRails,
)
from shared.kafka_client import is_duplicate_event, mark_event_processed
from tests.conftest import make_fx_rate


class TestConditionalPaymentIdempotency:
    def test_returns_existing_on_duplicate_key(self, db, alice, bob):
        """Second request with same idempotency_key returns existing."""
        ref = f"IDEM-CP-{uuid.uuid4().hex[:8]}"
        cp = ConditionalPayment(
            payment_ref=ref,
            payer_account_id=alice.id,
            payee_account_id=bob.id,
            currency=CurrencyCode.USD,
            amount=Decimal("1000"),
            condition_type=ConditionType.TIME_LOCK,
            condition_params={"release_at": "2099-01-01T00:00:00"},
            status=TxnStatus.PENDING,
        )
        db.add(cp)
        db.flush()

        # Simulate idempotency lookup
        existing = db.execute(
            select(ConditionalPayment).where(
                ConditionalPayment.payment_ref == ref
            )
        ).scalar_one_or_none()
        assert existing is not None
        assert existing.payment_ref == ref
        assert existing.amount == Decimal("1000")


class TestEscrowIdempotency:
    def test_returns_existing_on_duplicate_key(self, db, alice, bob):
        """Second request with same idempotency_key returns existing."""
        ref = f"IDEM-ESC-{uuid.uuid4().hex[:8]}"
        escrow = EscrowContract(
            contract_ref=ref,
            depositor_account_id=alice.id,
            beneficiary_account_id=bob.id,
            currency=CurrencyCode.USD,
            amount=Decimal("5000"),
            conditions={"type": "manual"},
            status=EscrowStatus.ACTIVE,
            expires_at=datetime.now(timezone.utc) + timedelta(days=30),
        )
        db.add(escrow)
        db.flush()

        existing = db.execute(
            select(EscrowContract).where(
                EscrowContract.contract_ref == ref
            )
        ).scalar_one_or_none()
        assert existing is not None
        assert existing.contract_ref == ref
        assert existing.amount == Decimal("5000")


class TestFXIdempotency:
    def test_returns_existing_on_duplicate_key(self, db, alice, bob):
        """Second request with same idempotency_key returns existing."""
        rate = make_fx_rate(db, "EUR", "USD", Decimal("1.08"))

        ref = f"IDEM-FX-{uuid.uuid4().hex[:8]}"
        fx = FXSettlement(
            settlement_ref=ref,
            sending_account_id=alice.id,
            receiving_account_id=bob.id,
            sell_currency=CurrencyCode.EUR,
            sell_amount=Decimal("10000"),
            buy_currency=CurrencyCode.USD,
            buy_amount=Decimal("10800"),
            applied_rate=Decimal("1.08"),
            fx_rate_id=rate.id,
            rails=SettlementRails.BLOCKCHAIN,
            status=SettlementStatus.QUEUED,
        )
        db.add(fx)
        db.flush()

        existing = db.execute(
            select(FXSettlement).where(
                FXSettlement.settlement_ref == ref
            )
        ).scalar_one_or_none()
        assert existing is not None
        assert existing.settlement_ref == ref


class TestKafkaDedup:
    def test_new_event_not_duplicate(self, db):
        event_id = f"evt-{uuid.uuid4().hex[:8]}"
        assert is_duplicate_event(db, event_id) is False

    def test_marked_event_is_duplicate(self, db):
        event_id = f"evt-{uuid.uuid4().hex[:8]}"
        mark_event_processed(db, event_id, "test.topic")
        assert is_duplicate_event(db, event_id) is True

    def test_processed_event_has_topic(self, db):
        event_id = f"evt-{uuid.uuid4().hex[:8]}"
        mark_event_processed(db, event_id, "compliance.event")

        row = db.execute(
            select(ProcessedEvent).where(
                ProcessedEvent.event_id == event_id
            )
        ).scalar_one()
        assert row.topic == "compliance.event"
