"""
0004: RBAC, Audit Trail, Idempotency, Settlement State Machine

Adds:
  - user_role enum type
  - api_keys table (RBAC)
  - processed_events table (Kafka dedup)
  - New settlement statuses: pending, approved, signed, broadcasted, confirmed
  - RTGS approval/signing columns
  - Audit columns (request_id, actor_id, actor_service) on 13 tables
  - Seed demo API keys (one per role)
"""

import hashlib
import uuid
from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

DEMO_KEYS = [
    ("admin-key-demo-001", "Platform Admin", "admin"),
    ("approver-key-demo-001", "Settlement Approver", "approver"),
    ("signer-key-demo-001", "Settlement Signer", "signer"),
    ("trader-key-demo-001", "Institutional Trader", "trader"),
    ("auditor-key-demo-001", "Compliance Auditor", "auditor"),
]


def upgrade():
    # 1. Create user_role enum
    user_role_enum = sa.Enum(
        "admin", "approver", "signer", "trader", "auditor",
        name="user_role",
    )
    user_role_enum.create(op.get_bind(), checkfirst=True)

    # 2. Create api_keys table
    op.create_table(
        "api_keys",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("key_hash", sa.String(64), unique=True, nullable=False),
        sa.Column("actor_id", UUID(as_uuid=True), nullable=False),
        sa.Column("actor_name", sa.String(255), nullable=False),
        sa.Column("role", user_role_enum, nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, default=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_api_keys_key_hash", "api_keys", ["key_hash"])
    op.create_index("ix_api_keys_actor_id", "api_keys", ["actor_id"])

    # 3. Create processed_events table
    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.String(255), primary_key=True),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # 4. Add new settlement statuses
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'pending'")
        op.execute("ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'approved'")
        op.execute("ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'signed'")
        op.execute("ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'broadcasted'")
        op.execute("ALTER TYPE settlement_status ADD VALUE IF NOT EXISTS 'confirmed'")

    # 5. Add approval/signing columns to rtgs_settlements
    op.add_column("rtgs_settlements", sa.Column("approved_by", UUID(as_uuid=True)))
    op.add_column("rtgs_settlements", sa.Column("approved_at", sa.DateTime(timezone=True)))
    op.add_column("rtgs_settlements", sa.Column("signed_by", UUID(as_uuid=True)))
    op.add_column("rtgs_settlements", sa.Column("signed_at", sa.DateTime(timezone=True)))

    # 6. Add audit columns to business tables
    audit_tables = [
        "token_issuances",
        "transactions",
        "rtgs_settlements",
        "fx_settlements",
        "escrow_contracts",
        "conditional_payments",
        "compliance_events",
        "transaction_status_history",
        "rtgs_settlement_status_history",
        "fx_settlement_status_history",
        "escrow_status_history",
        "conditional_payment_status_history",
        "token_issuance_status_history",
    ]

    for table in audit_tables:
        op.add_column(table, sa.Column("request_id", UUID(as_uuid=True)))
        op.add_column(table, sa.Column("actor_id", UUID(as_uuid=True)))
        op.add_column(table, sa.Column("actor_service", sa.String(100)))

    # 7. Seed demo API keys
    api_keys_table = sa.table(
        "api_keys",
        sa.column("id", UUID(as_uuid=True)),
        sa.column("key_hash", sa.String),
        sa.column("actor_id", UUID(as_uuid=True)),
        sa.column("actor_name", sa.String),
        sa.column("role", sa.String),
        sa.column("is_active", sa.Boolean),
    )

    for raw_key, name, role in DEMO_KEYS:
        op.bulk_insert(api_keys_table, [{
            "id": uuid.uuid4(),
            "key_hash": hashlib.sha256(raw_key.encode()).hexdigest(),
            "actor_id": uuid.uuid4(),
            "actor_name": name,
            "role": role,
            "is_active": True,
        }])


def downgrade():
    # Remove audit columns
    audit_tables = [
        "token_issuances",
        "transactions",
        "rtgs_settlements",
        "fx_settlements",
        "escrow_contracts",
        "conditional_payments",
        "compliance_events",
        "transaction_status_history",
        "rtgs_settlement_status_history",
        "fx_settlement_status_history",
        "escrow_status_history",
        "conditional_payment_status_history",
        "token_issuance_status_history",
    ]

    for table in audit_tables:
        op.drop_column(table, "actor_service")
        op.drop_column(table, "actor_id")
        op.drop_column(table, "request_id")

    # Remove RTGS approval columns
    op.drop_column("rtgs_settlements", "signed_at")
    op.drop_column("rtgs_settlements", "signed_by")
    op.drop_column("rtgs_settlements", "approved_at")
    op.drop_column("rtgs_settlements", "approved_by")

    op.drop_table("processed_events")
    op.drop_index("ix_api_keys_actor_id", table_name="api_keys")
    op.drop_index("ix_api_keys_key_hash", table_name="api_keys")
    op.drop_table("api_keys")

    sa.Enum(name="user_role").drop(op.get_bind(), checkfirst=True)
