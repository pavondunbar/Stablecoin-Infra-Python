"""
shared/models.py — SQLAlchemy ORM models for the stablecoin platform.
All services import from this module to keep the domain model canonical.
"""

import enum
import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, Column, Date, DateTime,
    Enum, ForeignKey, Index, Integer, Numeric, SmallInteger, String,
    Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.sql import func


# ─── Enumerations ────────────────────────────────────────────────────────────

class AccountType(str, enum.Enum):
    INSTITUTIONAL = "institutional"
    BANK          = "bank"
    CORRESPONDENT = "correspondent"
    CENTRAL_BANK  = "central_bank"

class TokenStatus(str, enum.Enum):
    ACTIVE    = "active"
    SUSPENDED = "suspended"
    REDEEMED  = "redeemed"

class TxnStatus(str, enum.Enum):
    PENDING    = "pending"
    PROCESSING = "processing"
    COMPLETED  = "completed"
    FAILED     = "failed"
    REVERSED   = "reversed"

class SettlementStatus(str, enum.Enum):
    QUEUED     = "queued"
    PROCESSING = "processing"
    SETTLED    = "settled"
    FAILED     = "failed"
    CANCELLED  = "cancelled"

class EscrowStatus(str, enum.Enum):
    PENDING  = "pending"
    ACTIVE   = "active"
    RELEASED = "released"
    REFUNDED = "refunded"
    EXPIRED  = "expired"

class CurrencyCode(str, enum.Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"
    SGD = "SGD"
    CHF = "CHF"
    HKD = "HKD"

class SettlementRails(str, enum.Enum):
    BLOCKCHAIN = "blockchain"
    SWIFT      = "swift"
    INTERNAL   = "internal"
    FEDWIRE    = "fedwire"
    TARGET2    = "target2"

class ConditionType(str, enum.Enum):
    TIME_LOCK             = "time_lock"
    ORACLE_TRIGGER        = "oracle_trigger"
    MULTI_SIG             = "multi_sig"
    DELIVERY_CONFIRMATION = "delivery_confirmation"
    KYC_VERIFIED          = "kyc_verified"

class NormalBalance(str, enum.Enum):
    DEBIT  = "debit"
    CREDIT = "credit"

class COAAccountType(str, enum.Enum):
    ASSET     = "asset"
    LIABILITY = "liability"
    REVENUE   = "revenue"

class HoldType(str, enum.Enum):
    RESERVE = "reserve"
    RELEASE = "release"


# ─── Base ─────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ─── Account ─────────────────────────────────────────────────────────────────

class Account(Base):
    __tablename__ = "accounts"

    id                      = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_name             = Column(String(255), nullable=False)
    account_type            = Column(Enum(AccountType, name="account_type", values_callable=lambda e: [x.value for x in e]), nullable=False)
    bic_code                = Column(String(11))
    legal_entity_identifier = Column(String(20), unique=True)
    is_active               = Column(Boolean, nullable=False, default=True)
    kyc_verified            = Column(Boolean, nullable=False, default=False)
    aml_cleared             = Column(Boolean, nullable=False, default=False)
    risk_tier               = Column(SmallInteger, nullable=False, default=3)
    extra_metadata          = Column("metadata", JSONB, nullable=False, default=dict)
    created_at              = Column(DateTime(timezone=True), server_default=func.now())
    updated_at              = Column(DateTime(timezone=True), onupdate=func.now())

    token_balances = relationship("TokenBalance", back_populates="account", lazy="selectin")


# ─── TokenBalance (legacy — replaced by account_balances VIEW) ───────────────

class TokenBalance(Base):
    __tablename__ = "token_balances"
    __table_args__ = (UniqueConstraint("account_id", "currency"),)

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id   = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    currency     = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    balance      = Column(Numeric(28, 8), nullable=False, default=Decimal("0"))
    reserved     = Column(Numeric(28, 8), nullable=False, default=Decimal("0"))
    token_status = Column(Enum(TokenStatus, name="token_status", values_callable=lambda e: [x.value for x in e]), nullable=False, default=TokenStatus.ACTIVE)
    version      = Column(BigInteger, nullable=False, default=0)
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    account = relationship("Account", back_populates="token_balances")

    @property
    def available(self) -> Decimal:
        return self.balance - self.reserved


# ─── LedgerEntry (legacy — replaced by JournalEntry) ────────────────────────

class LedgerEntry(Base):
    __tablename__ = "ledger_entries"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    txn_ref       = Column(String(64), nullable=False)
    entry_type    = Column(String(20), nullable=False)
    account_id    = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    currency      = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    amount        = Column(Numeric(28, 8), nullable=False)
    balance_after = Column(Numeric(28, 8), nullable=False)
    narrative     = Column(Text)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())


# ─── ChartOfAccounts ─────────────────────────────────────────────────────────

class ChartOfAccounts(Base):
    __tablename__ = "chart_of_accounts"

    code           = Column(String(30), primary_key=True)
    name           = Column(String(255), nullable=False)
    account_type   = Column(String(20), nullable=False)
    normal_balance = Column(String(10), nullable=False)


# ─── JournalEntry (replaces LedgerEntry — append-only) ──────────────────────

class JournalEntry(Base):
    __tablename__ = "journal_entries"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    journal_id   = Column(UUID(as_uuid=True), nullable=False, index=True)
    account_id   = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    coa_code     = Column(String(30), ForeignKey("chart_of_accounts.code"), nullable=False)
    currency     = Column(String(10), nullable=False)
    debit        = Column(Numeric(38, 18), nullable=False, default=Decimal("0"))
    credit       = Column(Numeric(38, 18), nullable=False, default=Decimal("0"))
    entry_type   = Column(String(50), nullable=False)
    reference_id = Column(UUID(as_uuid=True), index=True)
    narrative    = Column(Text)
    created_at   = Column(DateTime(timezone=True), server_default=func.now())


# ─── OutboxEvent (transactional outbox) ──────────────────────────────────────

class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregate_id = Column(String(255), nullable=False)
    event_type   = Column(String(255), nullable=False)
    payload      = Column(JSONB, nullable=False)
    created_at   = Column(DateTime(timezone=True), server_default=func.now())
    published_at = Column(DateTime(timezone=True))


# ─── EscrowHold (append-only holds for reserved funds) ──────────────────────

class EscrowHold(Base):
    __tablename__ = "escrow_holds"

    id                  = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hold_ref            = Column(String(64), nullable=False, index=True)
    account_id          = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    currency            = Column(String(10), nullable=False)
    amount              = Column(Numeric(38, 18), nullable=False)
    hold_type           = Column(String(10), nullable=False)
    related_entity_type = Column(String(50))
    related_entity_id   = Column(UUID(as_uuid=True))
    created_at          = Column(DateTime(timezone=True), server_default=func.now())


# ─── Status History Models ───────────────────────────────────────────────────

class TransactionStatusHistory(Base):
    __tablename__ = "transaction_status_history"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    transaction_id = Column(UUID(as_uuid=True), ForeignKey("transactions.id"), nullable=False, index=True)
    status         = Column(String(20), nullable=False)
    detail         = Column(JSONB)
    tx_hash        = Column(String(255))
    block_number   = Column(BigInteger)
    created_at     = Column(DateTime(timezone=True), server_default=func.now())


class RTGSSettlementStatusHistory(Base):
    __tablename__ = "rtgs_settlement_status_history"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    settlement_id = Column(UUID(as_uuid=True), ForeignKey("rtgs_settlements.id"), nullable=False, index=True)
    status        = Column(String(20), nullable=False)
    detail        = Column(JSONB)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())


class FXSettlementStatusHistory(Base):
    __tablename__ = "fx_settlement_status_history"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    settlement_id = Column(UUID(as_uuid=True), ForeignKey("fx_settlements.id"), nullable=False, index=True)
    status        = Column(String(20), nullable=False)
    detail        = Column(JSONB)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())


class EscrowStatusHistory(Base):
    __tablename__ = "escrow_status_history"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    escrow_id  = Column(UUID(as_uuid=True), ForeignKey("escrow_contracts.id"), nullable=False, index=True)
    status     = Column(String(20), nullable=False)
    detail     = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ConditionalPaymentStatusHistory(Base):
    __tablename__ = "conditional_payment_status_history"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    payment_id = Column(UUID(as_uuid=True), ForeignKey("conditional_payments.id"), nullable=False, index=True)
    status     = Column(String(20), nullable=False)
    detail     = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class TokenIssuanceStatusHistory(Base):
    __tablename__ = "token_issuance_status_history"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    issuance_id = Column(UUID(as_uuid=True), ForeignKey("token_issuances.id"), nullable=False, index=True)
    status      = Column(String(20), nullable=False)
    detail      = Column(JSONB)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())


# ─── TokenIssuance ────────────────────────────────────────────────────────────

class TokenIssuance(Base):
    __tablename__ = "token_issuances"

    id                  = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    issuance_ref        = Column(String(64), unique=True, nullable=False)
    account_id          = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    currency            = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    amount              = Column(Numeric(28, 8), nullable=False)
    backing_ref         = Column(String(255))
    custodian           = Column(String(100))
    issuance_type       = Column(String(20), nullable=False)
    status              = Column(Enum(TxnStatus, name="txn_status", values_callable=lambda e: [x.value for x in e]), nullable=False, default=TxnStatus.PENDING)
    compliance_check_id = Column(UUID(as_uuid=True))
    issued_at           = Column(DateTime(timezone=True))
    redeemed_at         = Column(DateTime(timezone=True))
    created_at          = Column(DateTime(timezone=True), server_default=func.now())


# ─── Transaction ─────────────────────────────────────────────────────────────

class Transaction(Base):
    __tablename__ = "transactions"

    id                = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    txn_ref           = Column(String(64), unique=True, nullable=False)
    debit_account_id  = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    credit_account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    currency          = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    amount            = Column(Numeric(28, 8), nullable=False)
    txn_type          = Column(String(50), nullable=False)
    status            = Column(Enum(TxnStatus, name="txn_status", values_callable=lambda e: [x.value for x in e]), nullable=False, default=TxnStatus.PENDING)
    idempotency_key   = Column(String(128), unique=True)
    parent_txn_id     = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))
    extra_metadata    = Column("metadata", JSONB, nullable=False, default=dict)
    created_at        = Column(DateTime(timezone=True), server_default=func.now())
    settled_at        = Column(DateTime(timezone=True))


# ─── RTGSSettlement ───────────────────────────────────────────────────────────

class RTGSSettlement(Base):
    __tablename__ = "rtgs_settlements"

    id                   = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    settlement_ref       = Column(String(64), unique=True, nullable=False)
    sending_account_id   = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    receiving_account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    currency             = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    amount               = Column(Numeric(28, 8), nullable=False)
    priority             = Column(String(10), nullable=False, default="normal")
    status               = Column(Enum(SettlementStatus, name="settlement_status", values_callable=lambda e: [x.value for x in e]), nullable=False, default=SettlementStatus.QUEUED)
    transaction_id       = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))
    scheduled_at         = Column(DateTime(timezone=True))
    queued_at            = Column(DateTime(timezone=True), server_default=func.now())
    processing_started   = Column(DateTime(timezone=True))
    settled_at           = Column(DateTime(timezone=True))
    failure_reason       = Column(Text)
    retry_count          = Column(SmallInteger, nullable=False, default=0)
    extra_metadata       = Column("metadata", JSONB, nullable=False, default=dict)


# ─── EscrowContract ───────────────────────────────────────────────────────────

class EscrowContract(Base):
    __tablename__ = "escrow_contracts"

    id                     = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    contract_ref           = Column(String(64), unique=True, nullable=False)
    depositor_account_id   = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    beneficiary_account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    currency               = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    amount                 = Column(Numeric(28, 8), nullable=False)
    conditions             = Column(JSONB, nullable=False)
    status                 = Column(Enum(EscrowStatus, name="escrow_status", values_callable=lambda e: [x.value for x in e]), nullable=False, default=EscrowStatus.PENDING)
    lock_txn_id            = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))
    release_txn_id         = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))
    arbiter_account_id     = Column(UUID(as_uuid=True), ForeignKey("accounts.id"))
    expires_at             = Column(DateTime(timezone=True), nullable=False)
    released_at            = Column(DateTime(timezone=True))
    release_triggered_by   = Column(String(100))
    extra_metadata         = Column("metadata", JSONB, nullable=False, default=dict)
    created_at             = Column(DateTime(timezone=True), server_default=func.now())


# ─── ConditionalPayment ───────────────────────────────────────────────────────

class ConditionalPayment(Base):
    __tablename__ = "conditional_payments"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    payment_ref      = Column(String(64), unique=True, nullable=False)
    payer_account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    payee_account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    currency         = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    amount           = Column(Numeric(28, 8), nullable=False)
    condition_type   = Column(Enum(ConditionType, name="condition_type", values_callable=lambda e: [x.value for x in e]), nullable=False)
    condition_params = Column(JSONB, nullable=False)
    status           = Column(Enum(TxnStatus, name="txn_status", values_callable=lambda e: [x.value for x in e]), nullable=False, default=TxnStatus.PENDING)
    trigger_data     = Column(JSONB)
    transaction_id   = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    executed_at      = Column(DateTime(timezone=True))
    expires_at       = Column(DateTime(timezone=True))


# ─── FXRate ───────────────────────────────────────────────────────────────────

class FXRate(Base):
    __tablename__ = "fx_rates"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    base_currency  = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    quote_currency = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    mid_rate       = Column(Numeric(18, 8), nullable=False)
    bid_rate       = Column(Numeric(18, 8))
    ask_rate       = Column(Numeric(18, 8))
    spread_bps     = Column(Numeric(8, 4))
    source         = Column(String(50), nullable=False, default="internal")
    is_active      = Column(Boolean, nullable=False, default=True)
    valid_from     = Column(DateTime(timezone=True), server_default=func.now())
    valid_until    = Column(DateTime(timezone=True))


# ─── ComplianceEvent ──────────────────────────────────────────────────────────

class ComplianceEvent(Base):
    __tablename__ = "compliance_events"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_type = Column(String(50),  nullable=False)
    entity_id   = Column(UUID(as_uuid=True), nullable=False)
    event_type  = Column(String(50),  nullable=False)
    result      = Column(String(20),  nullable=False)
    score       = Column(Numeric(5, 2))
    details     = Column(JSONB, nullable=False, default=dict)
    checked_at  = Column(DateTime(timezone=True), server_default=func.now())


# ─── FXSettlement ─────────────────────────────────────────────────────────────

class FXSettlement(Base):
    __tablename__ = "fx_settlements"

    id                   = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    settlement_ref       = Column(String(64), unique=True, nullable=False)
    sending_account_id   = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    receiving_account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    sell_currency        = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    sell_amount          = Column(Numeric(28, 8), nullable=False)
    buy_currency         = Column(Enum(CurrencyCode, name="currency_code", values_callable=lambda e: [x.value for x in e]), nullable=False)
    buy_amount           = Column(Numeric(28, 8), nullable=False)
    applied_rate         = Column(Numeric(18, 8), nullable=False)
    fx_rate_id           = Column(UUID(as_uuid=True), ForeignKey("fx_rates.id"))
    rails                = Column(Enum(SettlementRails, name="settlement_rails", values_callable=lambda e: [x.value for x in e]), nullable=False, default=SettlementRails.BLOCKCHAIN)
    status               = Column(Enum(SettlementStatus, name="settlement_status", values_callable=lambda e: [x.value for x in e]), nullable=False, default=SettlementStatus.QUEUED)
    sell_txn_id          = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))
    buy_txn_id           = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))
    blockchain_tx_hash   = Column(String(255))
    value_date           = Column(Date)
    created_at           = Column(DateTime(timezone=True), server_default=func.now())
    settled_at           = Column(DateTime(timezone=True))
    extra_metadata       = Column("metadata", JSONB, nullable=False, default=dict)
