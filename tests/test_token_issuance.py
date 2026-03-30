"""
tests/test_token_issuance.py
Unit tests for the Token Issuance & Redemption Engine.

Covers:
  - Happy-path token issuance
  - Happy-path redemption
  - Double-entry journal balance invariant
  - Idempotency (duplicate requests return same result)
  - Insufficient reserve balance
  - KYC/AML gate enforcement
  - Inactive account rejection
  - Available balance = balance - reserved
  - Outbox event assertions
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.models import (
    Account, TokenIssuance, Transaction, TxnStatus,
    JournalEntry, OutboxEvent, EscrowHold,
)
from shared.journal import get_balance as journal_get_balance
from shared.journal import get_available_balance
from tests.conftest import OMNIBUS_ID, make_account, make_balance, get_outbox_events


import sys
sys.path.insert(0, "/home/claude/stablecoin-infra/services/token-issuance")

from main import _issue_tokens, _redeem_tokens


# ─── Helper ───────────────────────────────────────────────────────────────────

def get_balance(db: Session, account_id, currency: str) -> Decimal:
    aid = str(account_id)
    return journal_get_balance(db, aid, currency)


# ─── Issuance Tests ───────────────────────────────────────────────────────────

class TestTokenIssuance:

    def test_basic_issuance_credits_account(self, db, alice, omnibus, mock_kafka):
        before = get_balance(db, alice.id, "USD")
        amount = Decimal("1_000_000")

        issuance = _issue_tokens(
            db,
            account_id=str(alice.id),
            currency="USD",
            amount=amount,
            backing_ref="FIAT-DEP-001",
            custodian="JPMorgan",
            idempotency_key=None,
        )

        assert issuance.status == TxnStatus.COMPLETED
        assert issuance.amount == amount
        after = get_balance(db, alice.id, "USD")
        assert after == before + amount

    def test_issuance_debits_omnibus(self, db, alice, omnibus, mock_kafka):
        omnibus_before = get_balance(db, OMNIBUS_ID, "USD")
        amount = Decimal("500_000")

        _issue_tokens(db, str(alice.id), "USD", amount, "DEP-002", None, None)

        omnibus_after = get_balance(db, OMNIBUS_ID, "USD")
        assert omnibus_after == omnibus_before - amount

    def test_double_entry_journal_balance(self, db, alice, omnibus, mock_kafka):
        """Sum of all debits == sum of all credits for the issuance."""
        amount = Decimal("750_000")

        before_count = db.execute(select(JournalEntry)).scalars().all()
        _issue_tokens(db, str(alice.id), "USD", amount, "DEP-003", None, None)
        after_entries = db.execute(select(JournalEntry)).scalars().all()

        new_entries = [e for e in after_entries if e not in before_count]
        total_debit = sum(e.debit for e in new_entries)
        total_credit = sum(e.credit for e in new_entries)
        assert total_debit == total_credit

    def test_issuance_publishes_outbox_events(self, db, alice, omnibus, mock_kafka):
        _issue_tokens(db, str(alice.id), "USD", Decimal("100_000"), "DEP-004", None, None)

        completed = get_outbox_events(db, "token.issuance.completed")
        balance_events = get_outbox_events(db, "token.balance.updated")

        assert len(completed) >= 1
        assert len(balance_events) >= 1
        ev = completed[-1]
        assert ev.payload["account_id"] == str(alice.id)
        assert ev.payload["currency"] == "USD"

    def test_idempotency_returns_same_result(self, db, alice, omnibus, mock_kafka):
        key = "ISS-IDEM-001"
        r1 = _issue_tokens(db, str(alice.id), "USD", Decimal("200_000"), "DEP-005", None, key)
        r2 = _issue_tokens(db, str(alice.id), "USD", Decimal("200_000"), "DEP-005", None, key)

        assert r1.issuance_ref == r2.issuance_ref

        issuances = db.execute(
            select(TokenIssuance).where(TokenIssuance.issuance_ref == key)
        ).scalars().all()
        assert len(issuances) == 1

    def test_kyc_aml_gate_blocks_issuance(self, db, unverified_account, omnibus, mock_kafka):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            _issue_tokens(
                db, str(unverified_account.id), "USD",
                Decimal("1_000"), "DEP-099", None, None,
            )
        assert exc_info.value.status_code == 403
        assert "KYC" in exc_info.value.detail or "AML" in exc_info.value.detail

    def test_inactive_account_blocked(self, db, omnibus, mock_kafka):
        inactive = make_account(db, "Frozen Corp", active=False)
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            _issue_tokens(db, str(inactive.id), "USD", Decimal("1_000"), "DEP-100", None, None)
        assert exc_info.value.status_code in (403, 404)

    def test_insufficient_reserve_raises(self, db, omnibus, mock_kafka):
        """Attempting to issue more than the reserve holds should fail."""
        recipient = make_account(db, "Rich Bank")
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            _issue_tokens(
                db, str(recipient.id), "EUR",
                Decimal("2_000_000_000"), "DEP-101", None, None,
            )
        assert exc_info.value.status_code == 422
        assert "reserve" in exc_info.value.detail.lower()

    def test_account_not_found(self, db, omnibus, mock_kafka):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            _issue_tokens(db, str(uuid.uuid4()), "USD", Decimal("1_000"), "DEP-102", None, None)
        assert exc_info.value.status_code == 404


# ─── Redemption Tests ─────────────────────────────────────────────────────────

class TestTokenRedemption:

    def test_basic_redemption_debits_account(self, db, alice, omnibus, mock_kafka):
        before = get_balance(db, alice.id, "USD")
        amount = Decimal("2_000_000")

        redemption = _redeem_tokens(
            db,
            account_id=str(alice.id),
            currency="USD",
            amount=amount,
            settlement_ref="SETTLE-001",
            idempotency_key=None,
        )

        assert redemption.status == TxnStatus.COMPLETED
        after = get_balance(db, alice.id, "USD")
        assert after == before - amount

    def test_redemption_credits_omnibus(self, db, alice, omnibus, mock_kafka):
        omnibus_before = get_balance(db, OMNIBUS_ID, "USD")
        amount = Decimal("1_000_000")

        _redeem_tokens(db, str(alice.id), "USD", amount, None, None)

        omnibus_after = get_balance(db, OMNIBUS_ID, "USD")
        assert omnibus_after == omnibus_before + amount

    def test_redemption_publishes_outbox_events(self, db, alice, omnibus, mock_kafka):
        _redeem_tokens(db, str(alice.id), "USD", Decimal("500_000"), None, None)

        events = get_outbox_events(db, "token.redemption.completed")
        assert len(events) >= 1
        assert events[-1].payload["account_id"] == str(alice.id)

    def test_redemption_insufficient_balance(self, db, alice, omnibus, mock_kafka):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            _redeem_tokens(db, str(alice.id), "USD", Decimal("100_000_000"), None, None)
        assert exc_info.value.status_code == 422
        assert "available" in exc_info.value.detail.lower()

    def test_redemption_respects_reserved_balance(self, db, omnibus, mock_kafka):
        """Reserved funds must not be spendable via redemption."""
        acct = make_account(db, "Reserved Corp")
        make_balance(db, str(acct.id), "USD",
                     balance=Decimal("1_000_000"),
                     reserved=Decimal("900_000"))
        db.add(EscrowHold(
            hold_ref="TEST-RESERVE", account_id=acct.id,
            currency="USD", amount=Decimal("900_000"),
            hold_type="reserve",
        ))
        db.flush()

        from fastapi import HTTPException
        with pytest.raises(HTTPException):
            _redeem_tokens(db, str(acct.id), "USD", Decimal("200_000"), None, None)

    def test_redemption_idempotency(self, db, alice, omnibus, mock_kafka):
        key = "RED-IDEM-001"
        amount = Decimal("100_000")
        r1 = _redeem_tokens(db, str(alice.id), "USD", amount, None, key)
        r2 = _redeem_tokens(db, str(alice.id), "USD", amount, None, key)
        assert r1.issuance_ref == r2.issuance_ref

    def test_double_entry_on_redemption(self, db, alice, omnibus, mock_kafka):
        amount = Decimal("300_000")

        before_entries = db.execute(select(JournalEntry)).scalars().all()
        _redeem_tokens(db, str(alice.id), "USD", amount, None, None)
        after_entries = db.execute(select(JournalEntry)).scalars().all()

        new_entries = [e for e in after_entries if e not in before_entries]
        total_debit = sum(e.debit for e in new_entries)
        total_credit = sum(e.credit for e in new_entries)
        assert total_debit == total_credit

    def test_precision_handling(self, db, alice, omnibus, mock_kafka):
        """18-decimal-place amounts should round correctly."""
        amount = Decimal("999999.999999999999999999999")
        issuance = _issue_tokens(db, str(alice.id), "USD", amount, "DEP-PREC", None, None)
        assert issuance.amount == amount.quantize(Decimal("0.000000000000000001"))
