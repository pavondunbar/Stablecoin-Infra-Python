"""Blockchain-grade architecture — journal ledger, outbox, escrow holds,
status history, immutability triggers, and balance views

Revision ID: 0003_blockchain_grade
Revises: 0002_perf_indexes
Create Date: 2024-12-01 00:00:00.000000 UTC

Adds double-entry journal ledger with chart of accounts, transactional
outbox for reliable event publishing, escrow hold tracking, per-entity
status history tables with immutability guarantees, and materialised
balance views.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# -- Revision identifiers ----------------------------------------------------

revision = "0003_blockchain_grade"
down_revision = "0002_perf_indexes"
branch_labels = None
depends_on = None


# -- Helpers ------------------------------------------------------------------

_STATUS_HISTORY_TABLES = [
    ("transaction_status_history", "transaction_id", "transactions"),
    ("rtgs_settlement_status_history", "settlement_id", "rtgs_settlements"),
    ("fx_settlement_status_history", "settlement_id", "fx_settlements"),
    ("escrow_status_history", "escrow_id", "escrow_contracts"),
    (
        "conditional_payment_status_history",
        "payment_id",
        "conditional_payments",
    ),
    ("token_issuance_status_history", "issuance_id", "token_issuances"),
]


def upgrade() -> None:
    # -- 1. Chart of accounts ------------------------------------------------
    op.create_table(
        "chart_of_accounts",
        sa.Column("code", sa.String(30), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("account_type", sa.String(20), nullable=False),
        sa.Column("normal_balance", sa.String(10), nullable=False),
    )

    op.execute(
        "INSERT INTO chart_of_accounts (code, name, account_type,"
        " normal_balance) VALUES"
        " ('OMNIBUS_RESERVE',"
        "  'Omnibus Reserve Account', 'asset', 'debit'),"
        " ('INSTITUTION_LIABILITY',"
        "  'Institution Token Liability', 'liability', 'credit'),"
        " ('FX_NOSTRO_USD',"
        "  'FX Nostro USD Account', 'asset', 'debit'),"
        " ('FX_NOSTRO_EUR',"
        "  'FX Nostro EUR Account', 'asset', 'debit'),"
        " ('FX_NOSTRO_GBP',"
        "  'FX Nostro GBP Account', 'asset', 'debit'),"
        " ('ESCROW_HOLDING',"
        "  'Escrow Holding Account', 'liability', 'credit'),"
        " ('SETTLEMENT_PENDING',"
        "  'Pending Settlement Account', 'liability', 'credit'),"
        " ('FEE_REVENUE',"
        "  'Fee Revenue Account', 'revenue', 'credit')"
    )

    # -- 2. Journal entries --------------------------------------------------
    op.create_table(
        "journal_entries",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "journal_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("accounts.id"),
            nullable=True,
        ),
        sa.Column(
            "coa_code",
            sa.String(30),
            sa.ForeignKey("chart_of_accounts.code"),
            nullable=True,
        ),
        sa.Column("currency", sa.String(10), nullable=True),
        sa.Column(
            "debit",
            sa.Numeric(38, 18),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "credit",
            sa.Numeric(38, 18),
            nullable=False,
            server_default="0",
        ),
        sa.Column("entry_type", sa.String(50), nullable=True),
        sa.Column(
            "reference_id", postgresql.UUID(as_uuid=True), nullable=True
        ),
        sa.Column("narrative", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )

    op.create_index(
        "idx_je_journal_id", "journal_entries", ["journal_id"]
    )
    op.create_index(
        "idx_je_account_id", "journal_entries", ["account_id"]
    )
    op.create_index(
        "idx_je_reference_id", "journal_entries", ["reference_id"]
    )
    op.create_index(
        "idx_je_account_coa_ccy",
        "journal_entries",
        ["account_id", "coa_code", "currency"],
    )

    # -- 3. Outbox events ----------------------------------------------------
    op.create_table(
        "outbox_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "aggregate_id", sa.String(255), nullable=True
        ),
        sa.Column(
            "event_type", sa.String(255), nullable=True
        ),
        sa.Column("payload", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "published_at", sa.DateTime(timezone=True), nullable=True
        ),
    )

    op.create_index(
        "idx_outbox_unpublished",
        "outbox_events",
        ["published_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )

    # -- 4. Escrow holds -----------------------------------------------------
    op.create_table(
        "escrow_holds",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("hold_ref", sa.String(64), nullable=True),
        sa.Column(
            "account_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("accounts.id"),
            nullable=True,
        ),
        sa.Column("currency", sa.String(10), nullable=True),
        sa.Column("amount", sa.Numeric(38, 18), nullable=True),
        sa.Column("hold_type", sa.String(10), nullable=True),
        sa.Column(
            "related_entity_type", sa.String(50), nullable=True
        ),
        sa.Column(
            "related_entity_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )

    op.create_index(
        "idx_eh_hold_ref", "escrow_holds", ["hold_ref"]
    )
    op.create_index(
        "idx_eh_account_ccy_type",
        "escrow_holds",
        ["account_id", "currency", "hold_type"],
    )

    # -- 5. Status history tables --------------------------------------------
    for table_name, fk_col, parent_table in _STATUS_HISTORY_TABLES:
        columns = [
            sa.Column(
                "id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("uuid_generate_v4()"),
            ),
            sa.Column(
                fk_col,
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey(f"{parent_table}.id"),
                nullable=False,
            ),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("detail", postgresql.JSONB, nullable=True),
        ]

        if table_name == "transaction_status_history":
            columns.append(
                sa.Column(
                    "tx_hash", sa.String(255), nullable=True
                )
            )
            columns.append(
                sa.Column(
                    "block_number", sa.BigInteger, nullable=True
                )
            )

        columns.append(
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
            )
        )

        op.create_table(table_name, *columns)

        op.create_index(
            f"idx_{table_name}_{fk_col}",
            table_name,
            [fk_col],
        )

    # -- 6. Immutability triggers --------------------------------------------

    # Journal entries: reject UPDATE and DELETE
    op.execute("""
        CREATE OR REPLACE FUNCTION reject_journal_entry_mutation()
        RETURNS TRIGGER AS $$
        BEGIN
            RAISE EXCEPTION
                'journal_entries rows are immutable — UPDATE and DELETE'
                ' are forbidden';
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_journal_entries_immutable
        BEFORE UPDATE OR DELETE ON journal_entries
        FOR EACH ROW
        EXECUTE FUNCTION reject_journal_entry_mutation();
    """)

    # Outbox events: reject DELETE; reject UPDATE except published_at
    op.execute("""
        CREATE OR REPLACE FUNCTION reject_outbox_event_mutation()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION
                    'outbox_events rows cannot be deleted';
                RETURN NULL;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                IF OLD.id         IS DISTINCT FROM NEW.id
                OR OLD.aggregate_id IS DISTINCT FROM NEW.aggregate_id
                OR OLD.event_type IS DISTINCT FROM NEW.event_type
                OR OLD.payload    IS DISTINCT FROM NEW.payload
                OR OLD.created_at IS DISTINCT FROM NEW.created_at
                THEN
                    RAISE EXCEPTION
                        'outbox_events — only published_at may be'
                        ' updated';
                END IF;
                RETURN NEW;
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_outbox_events_immutable
        BEFORE UPDATE OR DELETE ON outbox_events
        FOR EACH ROW
        EXECUTE FUNCTION reject_outbox_event_mutation();
    """)

    # Escrow holds: reject UPDATE and DELETE
    op.execute("""
        CREATE OR REPLACE FUNCTION reject_escrow_hold_mutation()
        RETURNS TRIGGER AS $$
        BEGIN
            RAISE EXCEPTION
                'escrow_holds rows are immutable — UPDATE and DELETE'
                ' are forbidden';
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_escrow_holds_immutable
        BEFORE UPDATE OR DELETE ON escrow_holds
        FOR EACH ROW
        EXECUTE FUNCTION reject_escrow_hold_mutation();
    """)

    # Status history tables: reject UPDATE and DELETE
    for table_name, _, _ in _STATUS_HISTORY_TABLES:
        fn_name = f"reject_{table_name}_mutation"
        trg_name = f"trg_{table_name}_immutable"
        op.execute(f"""
            CREATE OR REPLACE FUNCTION {fn_name}()
            RETURNS TRIGGER AS $$
            BEGIN
                RAISE EXCEPTION
                    '{table_name} rows are immutable — UPDATE and'
                    ' DELETE are forbidden';
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
        """)
        op.execute(f"""
            CREATE TRIGGER {trg_name}
            BEFORE UPDATE OR DELETE ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION {fn_name}();
        """)

    # -- 7. Status-sync triggers ---------------------------------------------

    _SYNC_CONFIGS = [
        (
            "transaction_status_history",
            "transaction_id",
            "transactions",
        ),
        (
            "rtgs_settlement_status_history",
            "settlement_id",
            "rtgs_settlements",
        ),
        (
            "fx_settlement_status_history",
            "settlement_id",
            "fx_settlements",
        ),
        (
            "escrow_status_history",
            "escrow_id",
            "escrow_contracts",
        ),
        (
            "conditional_payment_status_history",
            "payment_id",
            "conditional_payments",
        ),
        (
            "token_issuance_status_history",
            "issuance_id",
            "token_issuances",
        ),
    ]

    for history_table, fk_col, parent_table in _SYNC_CONFIGS:
        fn_name = f"sync_{parent_table}_status"
        trg_name = f"trg_{history_table}_sync"
        op.execute(f"""
            CREATE OR REPLACE FUNCTION {fn_name}()
            RETURNS TRIGGER AS $$
            BEGIN
                UPDATE {parent_table}
                   SET status = NEW.status
                 WHERE id = NEW.{fk_col};
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        op.execute(f"""
            CREATE TRIGGER {trg_name}
            AFTER INSERT ON {history_table}
            FOR EACH ROW
            EXECUTE FUNCTION {fn_name}();
        """)

    # -- 8. Views -------------------------------------------------------------

    op.execute("""
        CREATE VIEW account_balances AS
        SELECT account_id,
               currency,
               SUM(credit) - SUM(debit) AS balance
          FROM journal_entries
         WHERE coa_code = 'INSTITUTION_LIABILITY'
         GROUP BY account_id, currency;
    """)

    op.execute("""
        CREATE VIEW account_active_holds AS
        SELECT account_id,
               currency,
               SUM(
                   CASE WHEN hold_type = 'reserve'
                        THEN amount
                        ELSE -amount
                   END
               ) AS held
          FROM escrow_holds
         GROUP BY account_id, currency;
    """)

    op.execute("""
        CREATE VIEW account_available_balances AS
        SELECT b.account_id,
               b.currency,
               b.balance,
               COALESCE(h.held, 0) AS held,
               b.balance - COALESCE(h.held, 0) AS available
          FROM account_balances b
          LEFT JOIN account_active_holds h
            ON b.account_id = h.account_id
           AND b.currency   = h.currency;
    """)


def downgrade() -> None:
    # -- Views ---------------------------------------------------------------
    op.execute("DROP VIEW IF EXISTS account_available_balances")
    op.execute("DROP VIEW IF EXISTS account_active_holds")
    op.execute("DROP VIEW IF EXISTS account_balances")

    # -- Status-sync triggers ------------------------------------------------
    _SYNC_CONFIGS = [
        (
            "token_issuance_status_history",
            "token_issuances",
        ),
        (
            "conditional_payment_status_history",
            "conditional_payments",
        ),
        ("escrow_status_history", "escrow_contracts"),
        ("fx_settlement_status_history", "fx_settlements"),
        (
            "rtgs_settlement_status_history",
            "rtgs_settlements",
        ),
        ("transaction_status_history", "transactions"),
    ]

    for history_table, parent_table in _SYNC_CONFIGS:
        trg_name = f"trg_{history_table}_sync"
        fn_name = f"sync_{parent_table}_status"
        op.execute(f"DROP TRIGGER IF EXISTS {trg_name} ON {history_table}")
        op.execute(f"DROP FUNCTION IF EXISTS {fn_name}()")

    # -- Immutability triggers (status history) ------------------------------
    for table_name, _, _ in reversed(_STATUS_HISTORY_TABLES):
        trg_name = f"trg_{table_name}_immutable"
        fn_name = f"reject_{table_name}_mutation"
        op.execute(f"DROP TRIGGER IF EXISTS {trg_name} ON {table_name}")
        op.execute(f"DROP FUNCTION IF EXISTS {fn_name}()")

    # -- Immutability triggers (escrow_holds) --------------------------------
    op.execute(
        "DROP TRIGGER IF EXISTS trg_escrow_holds_immutable"
        " ON escrow_holds"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS reject_escrow_hold_mutation()"
    )

    # -- Immutability triggers (outbox_events) -------------------------------
    op.execute(
        "DROP TRIGGER IF EXISTS trg_outbox_events_immutable"
        " ON outbox_events"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS reject_outbox_event_mutation()"
    )

    # -- Immutability triggers (journal_entries) -----------------------------
    op.execute(
        "DROP TRIGGER IF EXISTS trg_journal_entries_immutable"
        " ON journal_entries"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS reject_journal_entry_mutation()"
    )

    # -- Status history tables -----------------------------------------------
    for table_name, _, _ in reversed(_STATUS_HISTORY_TABLES):
        op.drop_table(table_name)

    # -- Escrow holds --------------------------------------------------------
    op.drop_table("escrow_holds")

    # -- Outbox events -------------------------------------------------------
    op.drop_table("outbox_events")

    # -- Journal entries -----------------------------------------------------
    op.drop_table("journal_entries")

    # -- Chart of accounts ---------------------------------------------------
    op.drop_table("chart_of_accounts")
