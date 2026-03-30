"""
tests/test_e2e_scenarios.py
─────────────────────────────
End-to-end scenario tests that exercise multi-service flows within a single
in-memory DB session.  Each scenario tests a realistic business workflow.

Scenarios:
  1. Full issuance -> RTGS transfer -> redemption lifecycle
  2. Trade finance: escrow funded by token issuance, released on delivery
  3. Conditional cross-border payment (oracle trigger)
  4. FX settlement followed by RTGS in buy currency
  5. Multi-party settlement chain (A->B->C via sequential RTGS)
  6. Concurrency-safe double-spend attempt (optimistic locking)
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.models import (
    Account, ConditionalPayment, EscrowContract, FXSettlement,
    RTGSSettlement, TokenBalance, TokenIssuance, Transaction,
    EscrowHold,
    ConditionType, EscrowStatus, SettlementStatus, TxnStatus,
)
from shared.journal import get_balance as journal_get_balance
from shared.journal import get_available_balance
from tests.conftest import (
    make_account, make_balance, make_fx_rate,
    get_outbox_events, OMNIBUS_ID,
)

import sys
sys.path.insert(0, "/home/claude/stablecoin-infra/services/token-issuance")
import main as token_svc

sys.path.insert(0, "/home/claude/stablecoin-infra/services/rtgs")
import main as rtgs_svc

sys.path.insert(0, "/home/claude/stablecoin-infra/services/payment-engine")
import main as payment_svc

sys.path.insert(0, "/home/claude/stablecoin-infra/services/fx-settlement")
import main as fx_svc
from main import NOSTRO_MAP, PRECISION, RATE_PREC
from decimal import ROUND_DOWN


# ─── Helpers ─────────────────────────────────────────────────────────────────

def bal(db, account_id, currency: str) -> Decimal:
    aid = str(account_id)
    return journal_get_balance(db, aid, currency)


def get_active_holds(db, account_id, currency: str) -> Decimal:
    """Sum of active holds (reserve - release) for an account/currency."""
    from sqlalchemy import func
    acct_uuid = uuid.UUID(str(account_id)) if not isinstance(account_id, uuid.UUID) else account_id
    reserves = db.execute(
        select(func.coalesce(func.sum(EscrowHold.amount), 0)).where(
            EscrowHold.account_id == acct_uuid,
            EscrowHold.currency == currency,
            EscrowHold.hold_type == "reserve",
        )
    ).scalar() or Decimal("0")
    releases = db.execute(
        select(func.coalesce(func.sum(EscrowHold.amount), 0)).where(
            EscrowHold.account_id == acct_uuid,
            EscrowHold.currency == currency,
            EscrowHold.hold_type == "release",
        )
    ).scalar() or Decimal("0")
    return Decimal(str(reserves)) - Decimal(str(releases))


def setup_nostros(db):
    nostro_defs = [
        ("USD", NOSTRO_MAP["USD"]),
        ("EUR", NOSTRO_MAP["EUR"]),
        ("GBP", NOSTRO_MAP["GBP"]),
    ]
    for ccy, acc_id in nostro_defs:
        existing = db.execute(
            select(Account).where(Account.id == acc_id)
        ).scalar_one_or_none()
        if not existing:
            db.add(Account(
                id=uuid.UUID(acc_id),
                entity_name=f"FX_NOSTRO_{ccy}",
                account_type="bank",
                kyc_verified=True, aml_cleared=True, is_active=True,
            ))
        make_balance(db, acc_id, ccy, Decimal("500_000_000"))
    db.flush()


# ─── Scenario 1: Issuance -> RTGS Transfer -> Redemption ───────────────────

class TestScenario1_IssuanceToRTGSToRedemption:
    """
    Bank A (issuer) -> issues tokens -> RTGS to Bank B -> Bank B redeems.
    Models the full JPM Coin round-trip.
    """

    def test_full_lifecycle(self, db, omnibus, mock_kafka):
        bank_a = make_account(db, "Bank A (Issuer)")
        bank_b = make_account(db, "Bank B (Recipient)")

        # Step 1: Issue $10M to Bank A
        token_svc._issue_tokens(
            db, str(bank_a.id), "USD",
            Decimal("10_000_000"), "DEP-E2E-001", "JPMorgan", None,
        )
        assert bal(db, bank_a.id, "USD") == Decimal("10_000_000")
        issuance_events = get_outbox_events(db, "token.issuance.completed")
        assert len(issuance_events) >= 1

        # Step 2: RTGS transfer $7M from Bank A -> Bank B
        settlement = RTGSSettlement(
            settlement_ref="RTGS-E2E-001",
            sending_account_id=bank_a.id,
            receiving_account_id=bank_b.id,
            currency="USD",
            amount=Decimal("7_000_000"),
            priority="high",
            status=SettlementStatus.QUEUED,
        )
        db.add(settlement)
        db.flush()
        rtgs_svc._process_one_settlement(db, settlement)

        assert settlement.status == SettlementStatus.SETTLED
        assert bal(db, bank_a.id, "USD") == Decimal("3_000_000")
        assert bal(db, bank_b.id, "USD") == Decimal("7_000_000")

        # Step 3: Bank B redeems $5M back to fiat
        token_svc._redeem_tokens(
            db, str(bank_b.id), "USD",
            Decimal("5_000_000"), "REDEEM-E2E-001", None,
        )
        assert bal(db, bank_b.id, "USD") == Decimal("2_000_000")
        redemption_events = get_outbox_events(db, "token.redemption.completed")
        assert len(redemption_events) >= 1

        # Verify outbox event counts
        rtgs_events = get_outbox_events(db, "rtgs.settlement.completed")
        assert len(rtgs_events) >= 1
        balance_events = get_outbox_events(db, "token.balance.updated")
        assert len(balance_events) >= 2

    def test_omnibus_balance_net_change(self, db, omnibus, mock_kafka):
        """
        After issuance of X and redemption of Y, omnibus net change = Y - X.
        """
        bank = make_account(db, "Net Bank")
        omnibus_before = bal(db, OMNIBUS_ID, "USD")

        token_svc._issue_tokens(db, str(bank.id), "USD", Decimal("1_000_000"), "DEP-NET", None, None)
        token_svc._redeem_tokens(db, str(bank.id), "USD", Decimal("400_000"), "REDEEM-NET", None)

        omnibus_after = bal(db, OMNIBUS_ID, "USD")
        assert omnibus_after == omnibus_before - Decimal("600_000")


# ─── Scenario 2: Trade Finance Escrow ────────────────────────────────────────

class TestScenario2_TradeFinanceEscrow:
    """
    Buyer funds an escrow from issued tokens.
    On delivery confirmation, escrow releases to seller.
    Models L/C (Letter of Credit) digital equivalent.
    """

    def test_buyer_funded_escrow_released_on_delivery(self, db, omnibus, mock_kafka):
        buyer  = make_account(db, "Buyer Corp")
        seller = make_account(db, "Seller Corp")

        token_svc._issue_tokens(db, str(buyer.id), "USD", Decimal("5_000_000"), "DEP-TRADE", None, None)
        assert bal(db, buyer.id, "USD") == Decimal("5_000_000")

        payment_svc._reserve_funds(db, str(buyer.id), "USD", Decimal("3_000_000"))
        escrow = EscrowContract(
            contract_ref="ESC-TRADE-001",
            depositor_account_id=buyer.id,
            beneficiary_account_id=seller.id,
            currency="USD",
            amount=Decimal("3_000_000"),
            conditions={"delivery_ref": "BL-2024-001"},
            status=EscrowStatus.ACTIVE,
            expires_at=datetime.now(timezone.utc) + timedelta(days=30),
        )
        db.add(escrow)
        db.flush()

        # Buyer still has $5M total but $3M reserved
        assert bal(db, buyer.id, "USD") == Decimal("5_000_000")
        assert get_active_holds(db, buyer.id, "USD") == Decimal("3_000_000")

        # Delivery confirmed -> release escrow to seller
        payment_svc._locked_transfer(
            db,
            debit_id=str(buyer.id),
            credit_id=str(seller.id),
            currency="USD",
            amount=Decimal("3_000_000"),
            narrative="Trade escrow release BL-2024-001",
            txn_type="escrow_release",
            release_reserve=True,
        )
        escrow.status = EscrowStatus.RELEASED
        db.flush()

        assert bal(db, buyer.id,  "USD") == Decimal("2_000_000")
        assert bal(db, seller.id, "USD") == Decimal("3_000_000")
        assert escrow.status             == EscrowStatus.RELEASED

    def test_escrow_refunded_on_expired_delivery(self, db, omnibus, mock_kafka):
        buyer  = make_account(db, "Expired Buyer")
        seller = make_account(db, "Non-Delivering Seller")
        make_balance(db, str(buyer.id), "USD", Decimal("1_000_000"))

        # Reserve funds via EscrowHold
        payment_svc._reserve_funds(db, str(buyer.id), "USD", Decimal("500_000"))

        escrow = EscrowContract(
            contract_ref="ESC-EXPIRED-001",
            depositor_account_id=buyer.id,
            beneficiary_account_id=seller.id,
            currency="USD",
            amount=Decimal("500_000"),
            conditions={},
            status=EscrowStatus.ACTIVE,
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
        db.add(escrow)
        db.flush()

        holds_before = get_active_holds(db, buyer.id, "USD")
        payment_svc._expire_escrow(db, escrow)

        assert escrow.status == EscrowStatus.EXPIRED
        assert get_active_holds(db, buyer.id, "USD") == holds_before - Decimal("500_000")


# ─── Scenario 3: Oracle-Triggered Cross-Border Payment ───────────────────────

class TestScenario3_OracleConditionalPayment:
    """
    Payment only releases when an external oracle confirms a rate fix.
    Models an FX forward settlement conditioned on SOFR publication.
    """

    def test_payment_pending_until_oracle_posts(self, db, alice, bob, mock_kafka):
        cp = ConditionalPayment(
            payment_ref="CP-SOFR-001",
            payer_account_id=alice.id,
            payee_account_id=bob.id,
            currency="USD",
            amount=Decimal("1_000_000"),
            condition_type=ConditionType.ORACLE_TRIGGER,
            condition_params={"oracle_key": "SOFR", "expected_value": "5.33"},
            status=TxnStatus.PENDING,
        )
        db.add(cp)
        db.flush()

        wrong = {"oracle_key": "SOFR", "oracle_value": "5.20"}
        assert payment_svc.evaluate_condition("oracle_trigger", cp.condition_params, wrong) is False
        assert bal(db, bob.id, "USD") == Decimal("30_000_000")

    def test_payment_executes_when_oracle_matches(self, db, alice, bob, mock_kafka):
        alice_before = bal(db, alice.id, "USD")
        bob_before   = bal(db, bob.id,   "USD")

        cp = ConditionalPayment(
            payment_ref="CP-SOFR-002",
            payer_account_id=alice.id,
            payee_account_id=bob.id,
            currency="USD",
            amount=Decimal("2_000_000"),
            condition_type=ConditionType.ORACLE_TRIGGER,
            condition_params={"oracle_key": "SOFR", "expected_value": "5.33"},
            status=TxnStatus.PENDING,
        )
        db.add(cp)
        db.flush()

        correct = {"oracle_key": "SOFR", "oracle_value": "5.33"}
        payment_svc._execute_conditional_payment(db, cp, correct, "sofr-oracle")

        assert cp.status == TxnStatus.COMPLETED
        assert bal(db, alice.id, "USD") == alice_before - Decimal("2_000_000")
        assert bal(db, bob.id,   "USD") == bob_before   + Decimal("2_000_000")


# ─── Scenario 4: FX Settlement -> RTGS in Buy Currency ────────────────────────

class TestScenario4_FXThenRTGS:
    """
    Bank A exchanges USD -> EUR via FX rails,
    then immediately RTGS-transfers the EUR to Bank B.
    Models a cross-border corporate payment.
    """

    def test_fx_then_rtgs(self, db, alice, bob, omnibus, mock_kafka):
        setup_nostros(db)
        make_fx_rate(db, "USD", "EUR", Decimal("0.918"))

        sell = Decimal("2_000_000")
        rate = Decimal("0.918")
        buy  = (sell * rate).quantize(PRECISION, rounding=ROUND_DOWN)

        # FX: Alice USD -> Alice EUR
        fx = FXSettlement(
            settlement_ref="FXS-CHAIN-001",
            sending_account_id=alice.id,
            receiving_account_id=alice.id,
            sell_currency="USD",
            sell_amount=sell,
            buy_currency="EUR",
            buy_amount=buy,
            applied_rate=rate,
            rails=SettlementRails.BLOCKCHAIN,
            status=SettlementStatus.QUEUED,
            value_date=datetime.now(timezone.utc).date(),
        )
        db.add(fx)
        db.flush()
        from shared.models import SettlementRails

        with patch(
            "main._sign_settlement",
            return_value="0x" + "ab" * 32,
        ):
            fx_svc._process_fx_settlement(db, fx)

        assert fx.status == SettlementStatus.SETTLED
        assert bal(db, alice.id, "EUR") == Decimal("20_000_000") + buy

        # RTGS: Alice EUR -> Bob EUR
        transfer = Decimal("500_000")
        rtgs_settlement = RTGSSettlement(
            settlement_ref="RTGS-CHAIN-001",
            sending_account_id=alice.id,
            receiving_account_id=bob.id,
            currency="EUR",
            amount=transfer,
            priority="high",
            status=SettlementStatus.QUEUED,
        )
        db.add(rtgs_settlement)
        db.flush()
        rtgs_svc._process_one_settlement(db, rtgs_settlement)

        assert rtgs_settlement.status == SettlementStatus.SETTLED
        assert bal(db, alice.id, "EUR") == Decimal("20_000_000") + buy - transfer
        assert bal(db, bob.id,   "EUR") == transfer


# ─── Scenario 5: Multi-Party Settlement Chain ────────────────────────────────

class TestScenario5_SettlementChain:
    """
    A -> B -> C sequential RTGS.
    Models correspondent banking chains (RLN / intraday liquidity).
    """

    def test_three_hop_settlement(self, db, omnibus, mock_kafka):
        bank_a = make_account(db, "Chain Bank A")
        bank_b = make_account(db, "Chain Bank B")
        bank_c = make_account(db, "Chain Bank C")

        make_balance(db, str(bank_a.id), "USD", Decimal("5_000_000"))
        make_balance(db, str(bank_b.id), "USD", Decimal("1_000_000"))

        # A -> B: $3M
        s1 = RTGSSettlement(
            settlement_ref="CHAIN-1",
            sending_account_id=bank_a.id, receiving_account_id=bank_b.id,
            currency="USD", amount=Decimal("3_000_000"), priority="normal",
            status=SettlementStatus.QUEUED,
        )
        db.add(s1); db.flush()
        rtgs_svc._process_one_settlement(db, s1)
        assert s1.status == SettlementStatus.SETTLED

        # B -> C: $2M (using B's original $1M + $3M just received)
        s2 = RTGSSettlement(
            settlement_ref="CHAIN-2",
            sending_account_id=bank_b.id, receiving_account_id=bank_c.id,
            currency="USD", amount=Decimal("2_000_000"), priority="normal",
            status=SettlementStatus.QUEUED,
        )
        db.add(s2); db.flush()
        rtgs_svc._process_one_settlement(db, s2)
        assert s2.status == SettlementStatus.SETTLED

        assert bal(db, bank_a.id, "USD") == Decimal("2_000_000")
        assert bal(db, bank_b.id, "USD") == Decimal("2_000_000")
        assert bal(db, bank_c.id, "USD") == Decimal("2_000_000")


# ─── Scenario 6: Double-Spend Prevention ─────────────────────────────────────

class TestScenario6_DoubleSpendPrevention:
    """
    Two concurrent RTGS instructions attempting to spend the same balance.
    Only one should succeed; the second should fail (insufficient funds).
    """

    def test_sequential_double_spend_rejected(self, db, omnibus, mock_kafka):
        source = make_account(db, "Double Spender")
        dest_a = make_account(db, "Destination A")
        dest_b = make_account(db, "Destination B")
        make_balance(db, str(source.id), "USD", Decimal("1_000_000"))

        # First RTGS: $900K — should succeed
        s1 = RTGSSettlement(
            settlement_ref="DS-001",
            sending_account_id=source.id, receiving_account_id=dest_a.id,
            currency="USD", amount=Decimal("900_000"), priority="normal",
            status=SettlementStatus.QUEUED,
        )
        db.add(s1); db.flush()
        ok1 = rtgs_svc._process_one_settlement(db, s1)
        assert ok1 is True

        # Second RTGS: $900K — should fail (only $100K left)
        s2 = RTGSSettlement(
            settlement_ref="DS-002",
            sending_account_id=source.id, receiving_account_id=dest_b.id,
            currency="USD", amount=Decimal("900_000"), priority="normal",
            status=SettlementStatus.QUEUED,
        )
        db.add(s2); db.flush()
        ok2 = rtgs_svc._process_one_settlement(db, s2)
        assert ok2 is False

        assert bal(db, source.id, "USD") == Decimal("100_000")
        assert bal(db, dest_a.id, "USD") == Decimal("900_000")
        assert bal(db, dest_b.id, "USD") == Decimal("0")
