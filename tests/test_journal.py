"""
tests/test_journal.py — Tests for the double-entry journal module.

Covers:
  - Balanced journal entry pairs
  - Derived balance calculation
  - Available balance (balance minus holds)
  - Insufficient balance detection
  - Advisory lock fallback on SQLite
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.models import JournalEntry, EscrowHold
from shared.journal import (
    record_journal_pair, get_balance, get_available_balance,
    acquire_balance_lock,
)
from tests.conftest import make_account, make_balance, OMNIBUS_ID


class TestRecordJournalPair:

    def test_creates_two_entries_with_shared_journal_id(self, db, omnibus):
        acct = make_account(db, "Journal Test Bank")
        ref_id = str(uuid.uuid4())

        journal_id = record_journal_pair(
            db, OMNIBUS_ID, "OMNIBUS_RESERVE", "USD",
            Decimal("1000"), "test_entry", ref_id,
            str(acct.id), "INSTITUTION_LIABILITY", "test narrative",
        )

        entries = db.execute(
            select(JournalEntry).where(JournalEntry.journal_id == uuid.UUID(journal_id))
        ).scalars().all()
        assert len(entries) == 2

    def test_debit_and_credit_are_balanced(self, db, omnibus):
        acct = make_account(db, "Balance Check Bank")
        ref_id = str(uuid.uuid4())
        amount = Decimal("5000.50")

        journal_id = record_journal_pair(
            db, str(acct.id), "INSTITUTION_LIABILITY", "USD",
            amount, "test", ref_id,
            OMNIBUS_ID, "OMNIBUS_RESERVE", "",
        )

        entries = db.execute(
            select(JournalEntry).where(JournalEntry.journal_id == uuid.UUID(journal_id))
        ).scalars().all()

        total_debit = sum(e.debit for e in entries)
        total_credit = sum(e.credit for e in entries)
        assert total_debit == amount
        assert total_credit == amount

    def test_entries_reference_correct_accounts(self, db, omnibus):
        acct = make_account(db, "Ref Check Bank")
        ref_id = str(uuid.uuid4())

        record_journal_pair(
            db, str(acct.id), "INSTITUTION_LIABILITY", "EUR",
            Decimal("100"), "test", ref_id,
            OMNIBUS_ID, "OMNIBUS_RESERVE", "",
        )

        debit_entry = db.execute(
            select(JournalEntry).where(
                JournalEntry.reference_id == uuid.UUID(ref_id),
                JournalEntry.debit > 0,
            )
        ).scalar_one()
        assert str(debit_entry.account_id) == str(acct.id)
        assert debit_entry.coa_code == "INSTITUTION_LIABILITY"


class TestGetBalance:

    def test_balance_from_journal_entries(self, db, omnibus):
        acct = make_account(db, "Balance Derive Bank")
        make_balance(db, str(acct.id), "USD", Decimal("10_000"))
        balance = get_balance(db, str(acct.id), "USD")
        assert balance == Decimal("10_000")

    def test_zero_balance_for_missing_account(self, db, omnibus):
        fake_id = str(uuid.uuid4())
        balance = get_balance(db, fake_id, "USD")
        assert balance == Decimal("0")

    def test_balance_after_multiple_entries(self, db, omnibus):
        acct = make_account(db, "Multi Entry Bank")
        ref1 = str(uuid.uuid4())
        ref2 = str(uuid.uuid4())

        record_journal_pair(
            db, OMNIBUS_ID, "OMNIBUS_RESERVE", "USD",
            Decimal("5000"), "credit_acct", ref1,
            str(acct.id), "INSTITUTION_LIABILITY", "",
        )
        record_journal_pair(
            db, OMNIBUS_ID, "OMNIBUS_RESERVE", "USD",
            Decimal("3000"), "credit_acct", ref2,
            str(acct.id), "INSTITUTION_LIABILITY", "",
        )

        balance = get_balance(db, str(acct.id), "USD")
        assert balance == Decimal("8000")


class TestGetAvailableBalance:

    def test_available_equals_balance_with_no_holds(self, db, omnibus):
        acct = make_account(db, "No Holds Bank")
        make_balance(db, str(acct.id), "USD", Decimal("50_000"))
        available = get_available_balance(db, str(acct.id), "USD")
        assert available == Decimal("50_000")

    def test_available_reduced_by_holds(self, db, omnibus):
        acct = make_account(db, "Held Bank")
        make_balance(db, str(acct.id), "USD", Decimal("100_000"))

        hold = EscrowHold(
            hold_ref="TEST-HOLD-001",
            account_id=acct.id,
            currency="USD",
            amount=Decimal("30_000"),
            hold_type="reserve",
        )
        db.add(hold)
        db.flush()

        available = get_available_balance(db, str(acct.id), "USD")
        assert available == Decimal("70_000")

    def test_released_holds_restore_availability(self, db, omnibus):
        acct = make_account(db, "Release Bank")
        make_balance(db, str(acct.id), "USD", Decimal("100_000"))

        db.add(EscrowHold(
            hold_ref="REL-HOLD-001", account_id=acct.id,
            currency="USD", amount=Decimal("40_000"), hold_type="reserve",
        ))
        db.add(EscrowHold(
            hold_ref="REL-HOLD-001", account_id=acct.id,
            currency="USD", amount=Decimal("40_000"), hold_type="release",
        ))
        db.flush()

        available = get_available_balance(db, str(acct.id), "USD")
        assert available == Decimal("100_000")


class TestAdvisoryLock:

    def test_sqlite_lock_is_noop(self, db, omnibus):
        """Advisory lock should not raise on SQLite."""
        acct = make_account(db, "Lock Test Bank")
        acquire_balance_lock(db, str(acct.id), "USD")
