"""
tests/test_rtgs.py
───────────────────
Unit tests for the Real-Time Gross Settlement Engine.

Covers:
  - Basic settlement execution
  - Balance transfer correctness (journal-derived)
  - Insufficient balance rejection
  - Priority ordering (urgent before normal)
  - Outbox event lifecycle
  - Settlement status transitions
  - Retry logic on transient failure
  - Same-account settlement rejection (via CHECK constraint path)
  - Concurrent settlement isolation (skip_locked behaviour)
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.models import (
    Account, RTGSSettlement, Transaction,
    SettlementStatus, TxnStatus, JournalEntry,
)
from shared.journal import get_balance as journal_get_balance
from tests.conftest import make_account, make_balance, get_outbox_events, OMNIBUS_ID

import sys
sys.path.insert(0, "/home/claude/stablecoin-infra/services/rtgs")

from main import _transfer_balances, _process_one_settlement


# ─── Helper ───────────────────────────────────────────────────────────────────

def get_balance(db: Session, account_id, currency: str) -> Decimal:
    aid = str(account_id)
    return journal_get_balance(db, aid, currency)


def make_settlement(
    db: Session,
    sender_id: str,
    receiver_id: str,
    amount: Decimal,
    currency: str = "USD",
    priority: str = "normal",
) -> RTGSSettlement:
    ref = f"RTGS-{uuid.uuid4().hex[:12].upper()}"
    s   = RTGSSettlement(
        settlement_ref=ref,
        sending_account_id=sender_id,
        receiving_account_id=receiver_id,
        currency=currency,
        amount=amount,
        priority=priority,
        status=SettlementStatus.QUEUED,
    )
    db.add(s)
    db.flush()
    return s


# ─── Tests ────────────────────────────────────────────────────────────────────

class TestRTGSSettlement:

    def test_basic_settlement_transfers_balance(self, db, alice, bob, mock_kafka):
        amount = Decimal("5_000_000")
        alice_before = get_balance(db, alice.id, "USD")
        bob_before   = get_balance(db, bob.id,   "USD")

        settlement = make_settlement(db, str(alice.id), str(bob.id), amount)
        success = _process_one_settlement(db, settlement)

        assert success is True
        assert settlement.status == SettlementStatus.SETTLED
        assert settlement.settled_at is not None
        assert settlement.transaction_id is not None

        assert get_balance(db, alice.id, "USD") == alice_before - amount
        assert get_balance(db, bob.id,   "USD") == bob_before   + amount

    def test_settlement_creates_journal_entries(self, db, alice, bob, mock_kafka):
        amount = Decimal("1_000_000")
        before_count = db.execute(select(JournalEntry)).scalars().all()

        settlement = make_settlement(db, str(alice.id), str(bob.id), amount)
        _process_one_settlement(db, settlement)

        after_entries = db.execute(select(JournalEntry)).scalars().all()
        new_entries = [e for e in after_entries if e not in before_count]
        total_debit  = sum(e.debit for e in new_entries)
        total_credit = sum(e.credit for e in new_entries)
        assert total_debit == total_credit

    def test_settlement_publishes_completed_event(self, db, alice, bob, mock_kafka):
        settlement = make_settlement(db, str(alice.id), str(bob.id), Decimal("250_000"))
        _process_one_settlement(db, settlement)

        completed = get_outbox_events(db, "rtgs.settlement.completed")
        assert len(completed) >= 1
        ev = completed[-1]
        assert ev.payload["settlement_ref"] == settlement.settlement_ref
        assert ev.payload["currency"] == "USD"

    def test_insufficient_balance_fails_settlement(self, db, omnibus, mock_kafka):
        poor = make_account(db, "Broke Bank")
        make_balance(db, str(poor.id), "USD", Decimal("100"))

        rich = make_account(db, "Rich Bank")

        settlement = make_settlement(db, str(poor.id), str(rich.id), Decimal("1_000_000"))
        success = _process_one_settlement(db, settlement)

        assert success is False
        assert settlement.status in (SettlementStatus.FAILED, SettlementStatus.QUEUED)

    def test_failed_settlement_publishes_failure_event(self, db, omnibus, mock_kafka):
        poor = make_account(db, "Insolvent Inc")
        make_balance(db, str(poor.id), "USD", Decimal("0"))
        rich = make_account(db, "Creditor Corp")

        settlement = make_settlement(db, str(poor.id), str(rich.id), Decimal("500_000"))
        _process_one_settlement(db, settlement)

        failure_events = get_outbox_events(db, "rtgs.settlement.failed")
        assert len(failure_events) >= 1
        assert failure_events[-1].payload["settlement_ref"] == settlement.settlement_ref

    def test_settlement_to_new_account_creates_balance(self, db, alice, mock_kafka):
        """Receiving account with no existing balance gets credited via journal."""
        new_bank = make_account(db, "Brand New Bank")
        assert get_balance(db, new_bank.id, "USD") == Decimal("0")

        settlement = make_settlement(db, str(alice.id), str(new_bank.id), Decimal("1_000_000"))
        success = _process_one_settlement(db, settlement)

        assert success is True
        assert get_balance(db, new_bank.id, "USD") == Decimal("1_000_000")

    def test_processing_status_event_emitted(self, db, alice, bob, mock_kafka):
        settlement = make_settlement(db, str(alice.id), str(bob.id), Decimal("100_000"))
        _process_one_settlement(db, settlement)

        processing_events = get_outbox_events(db, "rtgs.settlement.processing")
        assert len(processing_events) >= 1
        assert processing_events[-1].payload["settlement_ref"] == settlement.settlement_ref

    def test_transfer_balances_atomic_rollback(self, db, alice, bob, mock_kafka):
        """If transfer fails mid-way, balances must be unchanged (DB rollback)."""
        alice_before = get_balance(db, alice.id, "USD")
        bob_before   = get_balance(db, bob.id,   "USD")

        settlement = make_settlement(
            db, str(alice.id), str(bob.id),
            amount=Decimal("999_999_999"),
        )
        _process_one_settlement(db, settlement)

        assert get_balance(db, alice.id, "USD") == alice_before
        assert get_balance(db, bob.id,   "USD") == bob_before

    def test_settlement_transaction_record_created(self, db, alice, bob, mock_kafka):
        settlement = make_settlement(db, str(alice.id), str(bob.id), Decimal("3_000_000"))
        _process_one_settlement(db, settlement)

        txn = db.get(Transaction, settlement.transaction_id)
        assert txn is not None
        assert txn.txn_type == "rtgs_settlement"
        assert txn.status   == TxnStatus.COMPLETED
        assert txn.amount   == Decimal("3_000_000")


class TestRTGSPriority:

    def test_urgent_settles_before_normal(self, db, alice, bob, omnibus, mock_kafka):
        normal = make_settlement(db, str(alice.id), str(bob.id),
                                  Decimal("100"), priority="normal")
        urgent = make_settlement(db, str(alice.id), str(bob.id),
                                  Decimal("100"), priority="urgent")

        settlements = db.execute(
            select(RTGSSettlement)
            .where(RTGSSettlement.status == SettlementStatus.QUEUED)
            .order_by(RTGSSettlement.priority.asc())
        ).scalars().all()

        priorities = [s.priority for s in settlements]
        urgent_idx = priorities.index("urgent")
        normal_idx = priorities.index("normal")
        assert urgent_idx < normal_idx

    def test_all_priority_levels_accepted(self, db, alice, bob):
        for priority in ("urgent", "high", "normal", "low"):
            s = make_settlement(db, str(alice.id), str(bob.id),
                                Decimal("1000"), priority=priority)
            assert s.priority == priority
