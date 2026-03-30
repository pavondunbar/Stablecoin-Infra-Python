"""
tests/test_status_history.py — Tests for the status history module.

Covers:
  - Status recording for various entity types
  - Current status query (latest wins)
  - Detail JSONB storage
  - Multiple status transitions
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from shared.models import (
    RTGSSettlement, RTGSSettlementStatusHistory,
    TokenIssuance, TokenIssuanceStatusHistory,
    Transaction, TransactionStatusHistory,
    SettlementStatus, TxnStatus,
)
from shared.status import record_status, get_current_status
from tests.conftest import make_account, make_balance, OMNIBUS_ID


class TestRecordStatus:

    def test_inserts_status_row(self, db, omnibus):
        acct = make_account(db, "Status Bank A")
        settlement = RTGSSettlement(
            settlement_ref="SH-TEST-001",
            sending_account_id=acct.id,
            receiving_account_id=acct.id,
            currency="USD",
            amount=Decimal("1000"),
            status=SettlementStatus.QUEUED,
        )
        db.add(settlement)
        db.flush()

        row = record_status(
            db, RTGSSettlementStatusHistory,
            "settlement_id", settlement.id,
            "processing",
        )
        assert row.status == "processing"
        assert row.settlement_id == settlement.id

    def test_stores_detail_jsonb(self, db, omnibus):
        acct = make_account(db, "Detail Bank")
        settlement = RTGSSettlement(
            settlement_ref="SH-TEST-002",
            sending_account_id=acct.id,
            receiving_account_id=acct.id,
            currency="USD",
            amount=Decimal("2000"),
            status=SettlementStatus.QUEUED,
        )
        db.add(settlement)
        db.flush()

        detail = {"reason": "balance check passed", "amount": "2000"}
        row = record_status(
            db, RTGSSettlementStatusHistory,
            "settlement_id", settlement.id,
            "settled", detail=detail,
        )
        assert row.detail == detail

    def test_extra_fields_stored(self, db, omnibus):
        acct = make_account(db, "Extra Fields Bank")
        txn = Transaction(
            txn_ref="TXN-SH-001",
            debit_account_id=acct.id,
            credit_account_id=acct.id,
            currency="USD",
            amount=Decimal("500"),
            txn_type="test",
            status=TxnStatus.PENDING,
        )
        db.add(txn)
        db.flush()

        row = record_status(
            db, TransactionStatusHistory,
            "transaction_id", txn.id,
            "completed",
            tx_hash="0xabc123",
            block_number=12345,
        )
        assert row.tx_hash == "0xabc123"
        assert row.block_number == 12345


class TestGetCurrentStatus:

    def test_returns_latest_status(self, db, omnibus):
        acct = make_account(db, "Latest Status Bank")
        settlement = RTGSSettlement(
            settlement_ref="SH-LATEST-001",
            sending_account_id=acct.id,
            receiving_account_id=acct.id,
            currency="USD",
            amount=Decimal("3000"),
            status=SettlementStatus.QUEUED,
        )
        db.add(settlement)
        db.flush()

        record_status(
            db, RTGSSettlementStatusHistory,
            "settlement_id", settlement.id, "queued",
        )
        record_status(
            db, RTGSSettlementStatusHistory,
            "settlement_id", settlement.id, "processing",
        )
        record_status(
            db, RTGSSettlementStatusHistory,
            "settlement_id", settlement.id, "settled",
        )

        current = get_current_status(
            db, RTGSSettlementStatusHistory,
            "settlement_id", settlement.id,
        )
        assert current == "settled"

    def test_returns_none_for_no_history(self, db, omnibus):
        fake_id = uuid.uuid4()
        current = get_current_status(
            db, RTGSSettlementStatusHistory,
            "settlement_id", fake_id,
        )
        assert current is None

    def test_token_issuance_status_tracking(self, db, omnibus):
        issuance = TokenIssuance(
            issuance_ref="ISS-SH-001",
            account_id=uuid.UUID(OMNIBUS_ID),
            currency="USD",
            amount=Decimal("10_000"),
            issuance_type="mint",
            status=TxnStatus.PENDING,
        )
        db.add(issuance)
        db.flush()

        record_status(
            db, TokenIssuanceStatusHistory,
            "issuance_id", issuance.id, "pending",
        )
        record_status(
            db, TokenIssuanceStatusHistory,
            "issuance_id", issuance.id, "completed",
        )

        current = get_current_status(
            db, TokenIssuanceStatusHistory,
            "issuance_id", issuance.id,
        )
        assert current == "completed"
