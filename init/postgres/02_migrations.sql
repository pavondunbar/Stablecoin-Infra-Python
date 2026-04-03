-- ═══════════════════════════════════════════════════════════════════════════
-- Migrations 0002–0004: applied automatically on fresh database init
-- (mirrors migrations/apply_pending.sql)
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ============================================================
-- Migration 0002: Performance indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_rtgs_sending_status
    ON rtgs_settlements (sending_account_id, status);
CREATE INDEX IF NOT EXISTS idx_rtgs_receiving_status
    ON rtgs_settlements (receiving_account_id, status);
CREATE INDEX IF NOT EXISTS idx_fx_sender
    ON fx_settlements (sending_account_id, created_at);
CREATE INDEX IF NOT EXISTS idx_compliance_entity_time
    ON compliance_events (entity_type, entity_id, checked_at);
CREATE INDEX IF NOT EXISTS idx_condpay_expires
    ON conditional_payments (expires_at)
    WHERE status = 'pending' AND expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ledger_account_ccy
    ON ledger_entries (account_id, currency, created_at);
CREATE INDEX IF NOT EXISTS idx_issuance_custodian
    ON token_issuances (custodian, status)
    WHERE custodian IS NOT NULL;

-- ============================================================
-- Migration 0003: Blockchain-grade architecture
-- ============================================================

-- 1. Chart of accounts
CREATE TABLE IF NOT EXISTS chart_of_accounts (
    code        VARCHAR(30) PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    account_type VARCHAR(20) NOT NULL,
    normal_balance VARCHAR(10) NOT NULL
);

INSERT INTO chart_of_accounts (code, name, account_type, normal_balance) VALUES
    ('OMNIBUS_RESERVE', 'Omnibus Reserve Account', 'asset', 'debit'),
    ('INSTITUTION_LIABILITY', 'Institution Token Liability', 'liability', 'credit'),
    ('FX_NOSTRO_USD', 'FX Nostro USD Account', 'asset', 'debit'),
    ('FX_NOSTRO_EUR', 'FX Nostro EUR Account', 'asset', 'debit'),
    ('FX_NOSTRO_GBP', 'FX Nostro GBP Account', 'asset', 'debit'),
    ('ESCROW_HOLDING', 'Escrow Holding Account', 'liability', 'credit'),
    ('SETTLEMENT_PENDING', 'Pending Settlement Account', 'liability', 'credit'),
    ('FEE_REVENUE', 'Fee Revenue Account', 'revenue', 'credit')
ON CONFLICT (code) DO NOTHING;

-- 2. Journal entries
CREATE TABLE IF NOT EXISTS journal_entries (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    journal_id   UUID NOT NULL,
    account_id   UUID REFERENCES accounts(id),
    coa_code     VARCHAR(30) REFERENCES chart_of_accounts(code),
    currency     VARCHAR(10),
    debit        NUMERIC(38,18) NOT NULL DEFAULT 0,
    credit       NUMERIC(38,18) NOT NULL DEFAULT 0,
    entry_type   VARCHAR(50),
    reference_id UUID,
    narrative    TEXT,
    created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_je_journal_id ON journal_entries (journal_id);
CREATE INDEX IF NOT EXISTS idx_je_account_id ON journal_entries (account_id);
CREATE INDEX IF NOT EXISTS idx_je_reference_id ON journal_entries (reference_id);
CREATE INDEX IF NOT EXISTS idx_je_account_coa_ccy ON journal_entries (account_id, coa_code, currency);

-- 3. Outbox events
CREATE TABLE IF NOT EXISTS outbox_events (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id VARCHAR(255),
    event_type   VARCHAR(255),
    payload      JSONB,
    created_at   TIMESTAMPTZ DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (published_at)
    WHERE published_at IS NULL;

-- 4. Escrow holds
CREATE TABLE IF NOT EXISTS escrow_holds (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hold_ref            VARCHAR(64),
    account_id          UUID REFERENCES accounts(id),
    currency            VARCHAR(10),
    amount              NUMERIC(38,18),
    hold_type           VARCHAR(10),
    related_entity_type VARCHAR(50),
    related_entity_id   UUID,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_eh_hold_ref ON escrow_holds (hold_ref);
CREATE INDEX IF NOT EXISTS idx_eh_account_ccy_type ON escrow_holds (account_id, currency, hold_type);

-- 5. Status history tables

CREATE TABLE IF NOT EXISTS transaction_status_history (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID NOT NULL REFERENCES transactions(id),
    status         VARCHAR(20) NOT NULL,
    detail         JSONB,
    tx_hash        VARCHAR(255),
    block_number   BIGINT,
    created_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_transaction_status_history_transaction_id
    ON transaction_status_history (transaction_id);

CREATE TABLE IF NOT EXISTS rtgs_settlement_status_history (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    settlement_id UUID NOT NULL REFERENCES rtgs_settlements(id),
    status        VARCHAR(20) NOT NULL,
    detail        JSONB,
    created_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rtgs_settlement_status_history_settlement_id
    ON rtgs_settlement_status_history (settlement_id);

CREATE TABLE IF NOT EXISTS fx_settlement_status_history (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    settlement_id UUID NOT NULL REFERENCES fx_settlements(id),
    status        VARCHAR(20) NOT NULL,
    detail        JSONB,
    created_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fx_settlement_status_history_settlement_id
    ON fx_settlement_status_history (settlement_id);

CREATE TABLE IF NOT EXISTS escrow_status_history (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    escrow_id  UUID NOT NULL REFERENCES escrow_contracts(id),
    status     VARCHAR(20) NOT NULL,
    detail     JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_escrow_status_history_escrow_id
    ON escrow_status_history (escrow_id);

CREATE TABLE IF NOT EXISTS conditional_payment_status_history (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payment_id UUID NOT NULL REFERENCES conditional_payments(id),
    status     VARCHAR(20) NOT NULL,
    detail     JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_conditional_payment_status_history_payment_id
    ON conditional_payment_status_history (payment_id);

CREATE TABLE IF NOT EXISTS token_issuance_status_history (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    issuance_id UUID NOT NULL REFERENCES token_issuances(id),
    status      VARCHAR(20) NOT NULL,
    detail      JSONB,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_token_issuance_status_history_issuance_id
    ON token_issuance_status_history (issuance_id);

-- 6. Immutability triggers

CREATE OR REPLACE FUNCTION reject_journal_entry_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'journal_entries rows are immutable — UPDATE and DELETE are forbidden';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_journal_entries_immutable
BEFORE UPDATE OR DELETE ON journal_entries
FOR EACH ROW EXECUTE FUNCTION reject_journal_entry_mutation();

CREATE OR REPLACE FUNCTION reject_outbox_event_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'outbox_events rows cannot be deleted';
        RETURN NULL;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.id         IS DISTINCT FROM NEW.id
        OR OLD.aggregate_id IS DISTINCT FROM NEW.aggregate_id
        OR OLD.event_type IS DISTINCT FROM NEW.event_type
        OR OLD.payload    IS DISTINCT FROM NEW.payload
        OR OLD.created_at IS DISTINCT FROM NEW.created_at
        THEN
            RAISE EXCEPTION 'outbox_events — only published_at may be updated';
        END IF;
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_outbox_events_immutable
BEFORE UPDATE OR DELETE ON outbox_events
FOR EACH ROW EXECUTE FUNCTION reject_outbox_event_mutation();

CREATE OR REPLACE FUNCTION reject_escrow_hold_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'escrow_holds rows are immutable — UPDATE and DELETE are forbidden';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_escrow_holds_immutable
BEFORE UPDATE OR DELETE ON escrow_holds
FOR EACH ROW EXECUTE FUNCTION reject_escrow_hold_mutation();

-- Status history immutability triggers
CREATE OR REPLACE FUNCTION reject_transaction_status_history_mutation()
RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'transaction_status_history rows are immutable'; RETURN NULL; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_transaction_status_history_immutable BEFORE UPDATE OR DELETE ON transaction_status_history FOR EACH ROW EXECUTE FUNCTION reject_transaction_status_history_mutation();

CREATE OR REPLACE FUNCTION reject_rtgs_settlement_status_history_mutation()
RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'rtgs_settlement_status_history rows are immutable'; RETURN NULL; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_rtgs_settlement_status_history_immutable BEFORE UPDATE OR DELETE ON rtgs_settlement_status_history FOR EACH ROW EXECUTE FUNCTION reject_rtgs_settlement_status_history_mutation();

CREATE OR REPLACE FUNCTION reject_fx_settlement_status_history_mutation()
RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'fx_settlement_status_history rows are immutable'; RETURN NULL; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_fx_settlement_status_history_immutable BEFORE UPDATE OR DELETE ON fx_settlement_status_history FOR EACH ROW EXECUTE FUNCTION reject_fx_settlement_status_history_mutation();

CREATE OR REPLACE FUNCTION reject_escrow_status_history_mutation()
RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'escrow_status_history rows are immutable'; RETURN NULL; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_escrow_status_history_immutable BEFORE UPDATE OR DELETE ON escrow_status_history FOR EACH ROW EXECUTE FUNCTION reject_escrow_status_history_mutation();

CREATE OR REPLACE FUNCTION reject_conditional_payment_status_history_mutation()
RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'conditional_payment_status_history rows are immutable'; RETURN NULL; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_conditional_payment_status_history_immutable BEFORE UPDATE OR DELETE ON conditional_payment_status_history FOR EACH ROW EXECUTE FUNCTION reject_conditional_payment_status_history_mutation();

CREATE OR REPLACE FUNCTION reject_token_issuance_status_history_mutation()
RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'token_issuance_status_history rows are immutable'; RETURN NULL; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_token_issuance_status_history_immutable BEFORE UPDATE OR DELETE ON token_issuance_status_history FOR EACH ROW EXECUTE FUNCTION reject_token_issuance_status_history_mutation();

-- 7. Status-sync triggers

CREATE OR REPLACE FUNCTION sync_transactions_status()
RETURNS TRIGGER AS $$ BEGIN UPDATE transactions SET status = NEW.status::txn_status WHERE id = NEW.transaction_id; RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_transaction_status_history_sync AFTER INSERT ON transaction_status_history FOR EACH ROW EXECUTE FUNCTION sync_transactions_status();

CREATE OR REPLACE FUNCTION sync_rtgs_settlements_status()
RETURNS TRIGGER AS $$ BEGIN UPDATE rtgs_settlements SET status = NEW.status::settlement_status WHERE id = NEW.settlement_id; RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_rtgs_settlement_status_history_sync AFTER INSERT ON rtgs_settlement_status_history FOR EACH ROW EXECUTE FUNCTION sync_rtgs_settlements_status();

CREATE OR REPLACE FUNCTION sync_fx_settlements_status()
RETURNS TRIGGER AS $$ BEGIN UPDATE fx_settlements SET status = NEW.status::settlement_status WHERE id = NEW.settlement_id; RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_fx_settlement_status_history_sync AFTER INSERT ON fx_settlement_status_history FOR EACH ROW EXECUTE FUNCTION sync_fx_settlements_status();

CREATE OR REPLACE FUNCTION sync_escrow_contracts_status()
RETURNS TRIGGER AS $$ BEGIN UPDATE escrow_contracts SET status = NEW.status::escrow_status WHERE id = NEW.escrow_id; RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_escrow_status_history_sync AFTER INSERT ON escrow_status_history FOR EACH ROW EXECUTE FUNCTION sync_escrow_contracts_status();

CREATE OR REPLACE FUNCTION sync_conditional_payments_status()
RETURNS TRIGGER AS $$ BEGIN UPDATE conditional_payments SET status = NEW.status::txn_status WHERE id = NEW.payment_id; RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_conditional_payment_status_history_sync AFTER INSERT ON conditional_payment_status_history FOR EACH ROW EXECUTE FUNCTION sync_conditional_payments_status();

CREATE OR REPLACE FUNCTION sync_token_issuances_status()
RETURNS TRIGGER AS $$ BEGIN UPDATE token_issuances SET status = NEW.status::txn_status WHERE id = NEW.issuance_id; RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_token_issuance_status_history_sync AFTER INSERT ON token_issuance_status_history FOR EACH ROW EXECUTE FUNCTION sync_token_issuances_status();

-- 8. Views

CREATE OR REPLACE VIEW account_balances AS
SELECT account_id, currency,
       SUM(credit) - SUM(debit) AS balance
  FROM journal_entries
 WHERE coa_code = 'INSTITUTION_LIABILITY'
 GROUP BY account_id, currency;

CREATE OR REPLACE VIEW account_active_holds AS
SELECT account_id, currency,
       SUM(CASE WHEN hold_type = 'reserve' THEN amount ELSE -amount END) AS held
  FROM escrow_holds
 GROUP BY account_id, currency;

CREATE OR REPLACE VIEW account_available_balances AS
SELECT b.account_id, b.currency, b.balance,
       COALESCE(h.held, 0) AS held,
       b.balance - COALESCE(h.held, 0) AS available
  FROM account_balances b
  LEFT JOIN account_active_holds h
    ON b.account_id = h.account_id AND b.currency = h.currency;

-- ============================================================
-- Migration 0004: RBAC, Audit Trail, Idempotency
-- ============================================================

-- 1. Create user_role enum
DO $$ BEGIN
    CREATE TYPE user_role AS ENUM ('admin', 'approver', 'signer', 'trader', 'auditor');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- 2. Create api_keys table
CREATE TABLE IF NOT EXISTS api_keys (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key_hash    VARCHAR(64) UNIQUE NOT NULL,
    actor_id    UUID NOT NULL,
    actor_name  VARCHAR(255) NOT NULL,
    role        user_role NOT NULL,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ DEFAULT now(),
    expires_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_api_keys_key_hash ON api_keys (key_hash);
CREATE INDEX IF NOT EXISTS ix_api_keys_actor_id ON api_keys (actor_id);

-- 3. Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     VARCHAR(255) PRIMARY KEY,
    topic        VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT now()
);

-- 4. Add new settlement statuses
-- NOTE: ALTER TYPE ... ADD VALUE cannot run inside a transaction,
-- so we handle this with a DO block that catches duplicates.
COMMIT;
DO $$ BEGIN ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'pending'; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'approved'; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'signed'; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'broadcasted'; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'confirmed'; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
BEGIN;

-- 5. Add approval/signing columns to rtgs_settlements
ALTER TABLE rtgs_settlements ADD COLUMN IF NOT EXISTS approved_by UUID;
ALTER TABLE rtgs_settlements ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ;
ALTER TABLE rtgs_settlements ADD COLUMN IF NOT EXISTS signed_by UUID;
ALTER TABLE rtgs_settlements ADD COLUMN IF NOT EXISTS signed_at TIMESTAMPTZ;

-- 6. Add audit columns to business tables
ALTER TABLE token_issuances ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE token_issuances ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE token_issuances ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE rtgs_settlements ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE rtgs_settlements ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE rtgs_settlements ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE fx_settlements ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE fx_settlements ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE fx_settlements ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE escrow_contracts ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE escrow_contracts ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE escrow_contracts ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE conditional_payments ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE conditional_payments ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE conditional_payments ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE compliance_events ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE compliance_events ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE compliance_events ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE transaction_status_history ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE transaction_status_history ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE transaction_status_history ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE rtgs_settlement_status_history ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE rtgs_settlement_status_history ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE rtgs_settlement_status_history ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE fx_settlement_status_history ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE fx_settlement_status_history ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE fx_settlement_status_history ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE escrow_status_history ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE escrow_status_history ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE escrow_status_history ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE conditional_payment_status_history ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE conditional_payment_status_history ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE conditional_payment_status_history ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

ALTER TABLE token_issuance_status_history ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE token_issuance_status_history ADD COLUMN IF NOT EXISTS actor_id UUID;
ALTER TABLE token_issuance_status_history ADD COLUMN IF NOT EXISTS actor_service VARCHAR(100);

-- 7. Seed demo API keys
INSERT INTO api_keys (id, key_hash, actor_id, actor_name, role, is_active) VALUES
    (uuid_generate_v4(), encode(sha256('admin-key-demo-001'::bytea), 'hex'),
     uuid_generate_v4(), 'Platform Admin', 'admin', true),
    (uuid_generate_v4(), encode(sha256('approver-key-demo-001'::bytea), 'hex'),
     uuid_generate_v4(), 'Settlement Approver', 'approver', true),
    (uuid_generate_v4(), encode(sha256('signer-key-demo-001'::bytea), 'hex'),
     uuid_generate_v4(), 'Settlement Signer', 'signer', true),
    (uuid_generate_v4(), encode(sha256('trader-key-demo-001'::bytea), 'hex'),
     uuid_generate_v4(), 'Institutional Trader', 'trader', true),
    (uuid_generate_v4(), encode(sha256('auditor-key-demo-001'::bytea), 'hex'),
     uuid_generate_v4(), 'Compliance Auditor', 'auditor', true)
ON CONFLICT (key_hash) DO NOTHING;

-- 8. Bootstrap journal entries for system accounts
INSERT INTO journal_entries
    (journal_id, account_id, coa_code, currency, debit, credit, entry_type, narrative)
SELECT
    uuid_generate_v4(), v.account_id::UUID, v.coa_code, v.currency,
    v.amount, 0, 'bootstrap', v.narrative
FROM (VALUES
    ('00000000-0000-0000-0000-000000000001', 'OMNIBUS_RESERVE', 'USD', 1000000000.000000000000000000::NUMERIC(38,18), 'Initial omnibus reserve funding USD'),
    ('00000000-0000-0000-0000-000000000001', 'OMNIBUS_RESERVE', 'EUR', 1000000000.000000000000000000::NUMERIC(38,18), 'Initial omnibus reserve funding EUR'),
    ('00000000-0000-0000-0000-000000000001', 'OMNIBUS_RESERVE', 'GBP', 1000000000.000000000000000000::NUMERIC(38,18), 'Initial omnibus reserve funding GBP'),
    ('00000000-0000-0000-0000-000000000002', 'FX_NOSTRO_USD',   'USD', 500000000.000000000000000000::NUMERIC(38,18),  'Initial FX nostro funding USD'),
    ('00000000-0000-0000-0000-000000000003', 'FX_NOSTRO_EUR',   'EUR', 500000000.000000000000000000::NUMERIC(38,18),  'Initial FX nostro funding EUR'),
    ('00000000-0000-0000-0000-000000000004', 'FX_NOSTRO_GBP',   'GBP', 500000000.000000000000000000::NUMERIC(38,18),  'Initial FX nostro funding GBP')
) AS v(account_id, coa_code, currency, amount, narrative)
WHERE NOT EXISTS (
    SELECT 1 FROM journal_entries WHERE entry_type = 'bootstrap'
);

COMMIT;
