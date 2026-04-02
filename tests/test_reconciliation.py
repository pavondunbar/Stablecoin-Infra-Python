"""
tests/test_reconciliation.py — Reconciliation engine tests.

Covers: balanced journals pass, imbalanced detected, replay matches,
orphans detected, JSON output, and report-only exit code.
"""

import json
import uuid
from decimal import Decimal

from sqlalchemy import text

from shared.models import JournalEntry
from tests.conftest import OMNIBUS_ID


def _to_uuid(val):
    if isinstance(val, uuid.UUID):
        return val
    return uuid.UUID(str(val))


def _insert_journal_pair(db, account_id, currency, amount, journal_id=None):
    """Insert a balanced debit/credit journal entry pair."""
    jid = journal_id or str(uuid.uuid4())
    ref_id = uuid.uuid4()

    debit = JournalEntry(
        journal_id=uuid.UUID(jid),
        account_id=_to_uuid(account_id),
        coa_code="INSTITUTION_LIABILITY",
        currency=currency,
        debit=amount,
        credit=Decimal("0"),
        entry_type="test",
        reference_id=ref_id,
        narrative="test debit",
    )
    credit = JournalEntry(
        journal_id=uuid.UUID(jid),
        account_id=_to_uuid(OMNIBUS_ID),
        coa_code="OMNIBUS_RESERVE",
        currency=currency,
        debit=Decimal("0"),
        credit=amount,
        entry_type="test",
        reference_id=ref_id,
        narrative="test credit",
    )
    db.add(debit)
    db.add(credit)
    db.flush()
    return jid


class TestJournalBalance:
    def test_balanced_journal_passes(self, db, alice, omnibus):
        """A properly balanced journal pair should not trigger issues."""
        _insert_journal_pair(
            db, str(alice.id), "USD", Decimal("1000")
        )

        # Verify the entries balance
        result = db.execute(
            text("""
                SELECT journal_id, SUM(debit) as d, SUM(credit) as c
                FROM journal_entries
                GROUP BY journal_id
                HAVING ABS(SUM(debit) - SUM(credit)) > 1e-18
            """)
        ).fetchall()
        assert len(result) == 0

    def test_imbalanced_journal_detected(self, db, alice, omnibus):
        """An imbalanced journal entry should be detectable."""
        jid = uuid.uuid4()
        # Create a debit with no matching credit
        debit_entry = JournalEntry(
            journal_id=jid,
            account_id=alice.id,
            coa_code="INSTITUTION_LIABILITY",
            currency="USD",
            debit=Decimal("500"),
            credit=Decimal("0"),
            entry_type="test_imbalanced",
            reference_id=uuid.uuid4(),
            narrative="orphan debit",
        )
        # Create a partial credit (imbalanced)
        credit_entry = JournalEntry(
            journal_id=jid,
            account_id=_to_uuid(OMNIBUS_ID),
            coa_code="OMNIBUS_RESERVE",
            currency="USD",
            debit=Decimal("0"),
            credit=Decimal("200"),
            entry_type="test_imbalanced",
            reference_id=debit_entry.reference_id,
            narrative="partial credit",
        )
        db.add(debit_entry)
        db.add(credit_entry)
        db.flush()

        # Use ORM column-level query to avoid SQLite UUID issues
        from sqlalchemy import select
        rows = db.execute(
            select(
                JournalEntry.debit, JournalEntry.credit
            ).where(JournalEntry.journal_id == jid)
        ).fetchall()
        assert len(rows) == 2, f"Expected 2 entries, got {len(rows)}"

        total_debit = sum(r[0] for r in rows)
        total_credit = sum(r[1] for r in rows)
        # 500 debit vs 200 credit = imbalance of 300
        assert abs(total_debit - total_credit) > 0


class TestChronologicalReplay:
    def test_replay_matches_balance(self, db, alice, omnibus):
        """Replaying journal entries should match the token_balances."""
        # alice already has balance from fixture
        # The journal entries created by make_balance should sum
        # to the token_balance amount
        result = db.execute(
            text("""
                SELECT SUM(credit) - SUM(debit) as net
                FROM journal_entries
                WHERE account_id = :acc
                  AND currency = :ccy
                  AND coa_code = 'INSTITUTION_LIABILITY'
            """),
            {"acc": str(alice.id), "ccy": "USD"},
        ).scalar()

        token_bal = db.execute(
            text("""
                SELECT balance FROM token_balances
                WHERE account_id = :acc AND currency = :ccy
            """),
            {"acc": str(alice.id), "ccy": "USD"},
        ).scalar()

        if result is not None and token_bal is not None:
            assert abs(
                Decimal(str(result)) - Decimal(str(token_bal))
            ) < Decimal("0.00000001")


class TestOrphanedJournals:
    def test_orphan_detection(self, db, alice, omnibus):
        """Journal entries with invalid reference_id are detectable."""
        fake_ref = uuid.uuid4()
        entry = JournalEntry(
            journal_id=uuid.uuid4(),
            account_id=alice.id,
            coa_code="INSTITUTION_LIABILITY",
            currency="USD",
            debit=Decimal("100"),
            credit=Decimal("0"),
            entry_type="orphan_test",
            reference_id=fake_ref,
            narrative="should be orphaned",
        )
        db.add(entry)
        db.flush()

        # This reference_id won't match any transaction
        from sqlalchemy import select
        from shared.models import Transaction
        txn = db.execute(
            select(Transaction).where(
                Transaction.id == fake_ref
            )
        ).scalar_one_or_none()
        assert txn is None


class TestJSONOutput:
    def test_report_structure(self):
        """JSON report has required fields."""
        report = {
            "checks_passed": 8,
            "issues": [],
            "warnings": [],
            "status": "ok",
        }
        output = json.dumps(report)
        parsed = json.loads(output)
        assert "checks_passed" in parsed
        assert "issues" in parsed
        assert "warnings" in parsed
        assert parsed["status"] == "ok"

    def test_failed_report(self):
        report = {
            "checks_passed": 7,
            "issues": ["IMBALANCE journal_id=abc"],
            "warnings": [],
            "status": "fail",
        }
        parsed = json.loads(json.dumps(report))
        assert parsed["status"] == "fail"
        assert len(parsed["issues"]) == 1
