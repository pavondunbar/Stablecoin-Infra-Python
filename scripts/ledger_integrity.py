#!/usr/bin/env python3
"""
scripts/ledger_integrity.py
-----------------------------
Offline ledger integrity checker.

Verifies the double-entry invariants of the entire ledger:
  1. For every txn_ref, sum(debits) == sum(credits)
  2. For every account+currency, balance == sum(credits) - sum(debits)
     from ledger_entries (balance-after cross-check)
  3. token_balances.balance >= 0 (no negative balances)
  4. token_balances.reserved <= token_balances.balance
  5. Orphaned transactions (no matching ledger entries)
  6. Settled RTGS records have a transaction_id
  7. FX settlements have both legs recorded
  8. Active escrow amounts <= depositor reserved balance
  9. Journal entry balance (debit == credit per journal_id)
  10. Journal vs account_balances view cross-check
  11. Orphaned journal entries
  12. Chronological replay of journal entries

Usage:
    DATABASE_URL=postgresql+psycopg2://stablecoin:s3cr3t@localhost:5432/stablecoin_db \
        python scripts/ledger_integrity.py

    python scripts/ledger_integrity.py --report-only
    python scripts/ledger_integrity.py --json
    python scripts/ledger_integrity.py --webhook-url https://alerts.example.com/hook
"""

import argparse
import json as json_mod
import os
import sys
import urllib.request
import urllib.error
from collections import defaultdict
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://stablecoin:s3cr3t@localhost:5432/stablecoin_db",
)

PASS  = "\033[32m✓\033[0m"
FAIL  = "\033[31m✗\033[0m"
WARN  = "\033[33m⚠\033[0m"
BOLD  = "\033[1m"
RESET = "\033[0m"

issues:    list[str] = []
warnings:  list[str] = []
checks_ok: int       = 0
json_mode: bool      = False


def ok(msg: str):
    global checks_ok
    checks_ok += 1
    if not json_mode:
        print(f"  {PASS}  {msg}")


def fail(msg: str):
    issues.append(msg)
    if not json_mode:
        print(f"  {FAIL}  {msg}")


def warn(msg: str):
    warnings.append(msg)
    if not json_mode:
        print(f"  {WARN}  {msg}")


def _check_table_exists(db, table_name: str) -> bool:
    """Check if a table exists (works on both Postgres and SQLite)."""
    try:
        db.execute(text(f"SELECT 1 FROM {table_name} LIMIT 0"))
        return True
    except Exception:
        return False


def run_checks(db) -> None:
    from shared.models import TokenBalance

    # ── 1. Double-entry balance per txn_ref ──
    if not json_mode:
        print(f"\n{BOLD}Check 1: Double-entry balance (debit == credit per txn_ref){RESET}")

    rows = db.execute(
        text("""
            SELECT txn_ref, entry_type, SUM(amount) AS total
            FROM ledger_entries
            GROUP BY txn_ref, entry_type
        """)
    ).fetchall()

    txn_totals: dict[str, dict[str, Decimal]] = defaultdict(dict)
    for row in rows:
        txn_totals[row.txn_ref][row.entry_type] = Decimal(str(row.total))

    imbalances = 0
    for ref, sides in txn_totals.items():
        d = sides.get("debit",  Decimal("0"))
        c = sides.get("credit", Decimal("0"))
        if abs(d - c) > Decimal("0.00000001"):
            fail(f"IMBALANCE txn_ref={ref}  debit={d}  credit={c}  diff={d-c}")
            imbalances += 1

    if imbalances == 0:
        ok(f"All {len(txn_totals)} transactions are double-entry balanced.")

    # ── 2. Running balance cross-check ──
    if not json_mode:
        print(f"\n{BOLD}Check 2: token_balances vs ledger_entries running totals{RESET}")

    balances = db.execute(select(TokenBalance)).scalars().all()
    drift_count = 0
    for b in balances:
        net = db.execute(
            text("""
                SELECT
                    COALESCE(SUM(CASE WHEN entry_type='credit' THEN amount ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN entry_type='debit'  THEN amount ELSE 0 END), 0)
                FROM ledger_entries
                WHERE account_id = :acc AND currency = :ccy
            """),
            {"acc": str(b.account_id), "ccy": str(b.currency.value)},
        ).scalar()

        computed = Decimal(str(net)) if net is not None else Decimal("0")
        recorded = b.balance

        if str(b.account_id).startswith("00000000-0000-0000-0000"):
            continue

        if abs(computed - recorded) > Decimal("0.00000001"):
            fail(
                f"BALANCE DRIFT  account={b.account_id}  currency={b.currency.value}  "
                f"ledger={computed}  recorded={recorded}  diff={recorded - computed}"
            )
            drift_count += 1

    if drift_count == 0:
        ok(f"All {len(balances)} balance records match ledger reconstruction.")

    # ── 3. No negative balances ──
    if not json_mode:
        print(f"\n{BOLD}Check 3: No negative balances{RESET}")

    negatives = db.execute(
        text("SELECT account_id, currency, balance FROM token_balances WHERE balance < 0")
    ).fetchall()

    if negatives:
        for row in negatives:
            fail(f"NEGATIVE BALANCE  account={row.account_id}  currency={row.currency}  balance={row.balance}")
    else:
        ok("No negative balances found.")

    # ── 4. Reserved <= Balance ──
    if not json_mode:
        print(f"\n{BOLD}Check 4: reserved <= balance for all accounts{RESET}")

    over_reserved = db.execute(
        text("SELECT account_id, currency, balance, reserved FROM token_balances WHERE reserved > balance")
    ).fetchall()

    if over_reserved:
        for row in over_reserved:
            fail(
                f"OVER-RESERVED  account={row.account_id}  currency={row.currency}  "
                f"balance={row.balance}  reserved={row.reserved}"
            )
    else:
        ok("All reserved amounts are within balance.")

    # ── 5. Orphaned transactions ──
    if not json_mode:
        print(f"\n{BOLD}Check 5: No orphaned transactions (missing ledger entries){RESET}")

    orphans = db.execute(
        text("""
            SELECT t.txn_ref FROM transactions t
            LEFT JOIN ledger_entries l ON l.txn_ref = t.txn_ref
            WHERE l.id IS NULL
              AND t.status = 'completed'
        """)
    ).fetchall()

    if orphans:
        for row in orphans:
            warn(f"ORPHANED TXN  txn_ref={row.txn_ref}  (completed but no ledger entries)")
    else:
        ok("No orphaned completed transactions.")

    # ── 6. Settlement consistency ──
    if not json_mode:
        print(f"\n{BOLD}Check 6: Settled RTGS records have a transaction_id{RESET}")

    unsettled = db.execute(
        text("""
            SELECT settlement_ref FROM rtgs_settlements
            WHERE status = 'settled' AND transaction_id IS NULL
        """)
    ).fetchall()

    if unsettled:
        for row in unsettled:
            fail(f"SETTLED WITHOUT TXN  settlement_ref={row.settlement_ref}")
    else:
        ok("All settled RTGS records have a linked transaction.")

    # ── 7. FX PvP leg consistency ──
    if not json_mode:
        print(f"\n{BOLD}Check 7: FX settlements have both legs recorded{RESET}")

    missing_legs = db.execute(
        text("""
            SELECT settlement_ref FROM fx_settlements
            WHERE status = 'settled'
              AND (sell_txn_id IS NULL OR buy_txn_id IS NULL)
        """)
    ).fetchall()

    if missing_legs:
        for row in missing_legs:
            fail(f"FX MISSING LEG  settlement_ref={row.settlement_ref}")
    else:
        ok("All settled FX records have both sell and buy legs.")

    # ── 8. Active escrows don't exceed depositor balance ──
    if not json_mode:
        print(f"\n{BOLD}Check 8: Active escrow amounts <= depositor reserved balance{RESET}")

    rows = db.execute(
        text("""
            SELECT e.depositor_account_id, e.currency, SUM(e.amount) as escrow_total,
                   tb.reserved
            FROM escrow_contracts e
            JOIN token_balances tb
              ON tb.account_id = e.depositor_account_id AND tb.currency = e.currency
            WHERE e.status = 'active'
            GROUP BY e.depositor_account_id, e.currency, tb.reserved
        """)
    ).fetchall()

    escrow_issues = 0
    for row in rows:
        escrow_total = Decimal(str(row.escrow_total))
        reserved     = Decimal(str(row.reserved))
        if escrow_total > reserved + Decimal("0.00000001"):
            fail(
                f"ESCROW > RESERVED  account={row.depositor_account_id}  "
                f"currency={row.currency}  escrow_total={escrow_total}  reserved={reserved}"
            )
            escrow_issues += 1

    if escrow_issues == 0:
        ok("All active escrow amounts are within depositor reserved balances.")

    # ── 9. Journal entry balance (debit == credit per journal_id) ──
    if not _check_table_exists(db, "journal_entries"):
        warn("journal_entries table not found, skipping checks 9-12.")
        return

    if not json_mode:
        print(f"\n{BOLD}Check 9: Journal entry balance (debit == credit per journal_id){RESET}")

    j_imbalances = db.execute(
        text("""
            SELECT journal_id, SUM(debit) AS total_debit, SUM(credit) AS total_credit
            FROM journal_entries
            GROUP BY journal_id
            HAVING ABS(SUM(debit) - SUM(credit)) > 1e-18
        """)
    ).fetchall()

    if j_imbalances:
        for row in j_imbalances:
            fail(
                f"JOURNAL IMBALANCE  journal_id={row.journal_id}  "
                f"debit={row.total_debit}  credit={row.total_credit}"
            )
    else:
        journal_count = db.execute(
            text("SELECT COUNT(DISTINCT journal_id) FROM journal_entries")
        ).scalar()
        ok(f"All {journal_count} journal groups are balanced.")

    # ── 10. Journal vs account_balances cross-check ──
    if not json_mode:
        print(f"\n{BOLD}Check 10: Journal entries vs account totals{RESET}")

    j_balances = db.execute(
        text("""
            SELECT account_id, currency,
                   SUM(credit) - SUM(debit) AS journal_balance
            FROM journal_entries
            WHERE coa_code = 'INSTITUTION_LIABILITY'
            GROUP BY account_id, currency
        """)
    ).fetchall()

    j_drift = 0
    for row in j_balances:
        acct_id = str(row.account_id)
        if acct_id.startswith("00000000-0000-0000-0000"):
            continue
        tb = db.execute(
            text("""
                SELECT balance FROM token_balances
                WHERE account_id = :acc AND currency = :ccy
            """),
            {"acc": acct_id, "ccy": row.currency},
        ).scalar()
        if tb is not None:
            journal_bal = Decimal(str(row.journal_balance))
            token_bal = Decimal(str(tb))
            if abs(journal_bal - token_bal) > Decimal("0.00000001"):
                fail(
                    f"JOURNAL/TOKEN DRIFT  account={acct_id}  currency={row.currency}  "
                    f"journal={journal_bal}  token_balance={token_bal}"
                )
                j_drift += 1

    if j_drift == 0:
        ok(f"All {len(j_balances)} journal account balances match token_balances.")

    # ── 11. Orphaned journal entries ──
    if not json_mode:
        print(f"\n{BOLD}Check 11: No orphaned journal entries{RESET}")

    orphaned_journals = db.execute(
        text("""
            SELECT je.id, je.reference_id, je.entry_type
            FROM journal_entries je
            WHERE je.reference_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.id = je.reference_id)
              AND NOT EXISTS (SELECT 1 FROM token_issuances ti WHERE ti.id = je.reference_id)
              AND NOT EXISTS (SELECT 1 FROM rtgs_settlements rs WHERE rs.id = je.reference_id)
              AND NOT EXISTS (SELECT 1 FROM escrow_contracts ec WHERE ec.id = je.reference_id)
            LIMIT 20
        """)
    ).fetchall()

    if orphaned_journals:
        for row in orphaned_journals:
            warn(f"ORPHANED JOURNAL  id={row.id}  reference_id={row.reference_id}  type={row.entry_type}")
    else:
        ok("No orphaned journal entries found.")

    # ── 12. Chronological replay of journal entries ──
    if not json_mode:
        print(f"\n{BOLD}Check 12: Chronological replay balance verification{RESET}")

    all_entries = db.execute(
        text("""
            SELECT account_id, currency, debit, credit
            FROM journal_entries
            WHERE coa_code = 'INSTITUTION_LIABILITY'
            ORDER BY created_at ASC, id ASC
        """)
    ).fetchall()

    replay_balances: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for entry in all_entries:
        key = f"{entry.account_id}:{entry.currency}"
        replay_balances[key] += Decimal(str(entry.credit)) - Decimal(str(entry.debit))

    replay_drift = 0
    for key, replay_bal in replay_balances.items():
        acct_id, currency = key.split(":", 1)
        if acct_id.startswith("00000000-0000-0000-0000"):
            continue
        tb = db.execute(
            text("""
                SELECT balance FROM token_balances
                WHERE account_id = :acc AND currency = :ccy
            """),
            {"acc": acct_id, "ccy": currency},
        ).scalar()
        if tb is not None:
            token_bal = Decimal(str(tb))
            if abs(replay_bal - token_bal) > Decimal("0.00000001"):
                fail(
                    f"REPLAY DRIFT  account={acct_id}  currency={currency}  "
                    f"replay={replay_bal}  token_balance={token_bal}"
                )
                replay_drift += 1

    if replay_drift == 0:
        ok(f"Chronological replay matches for {len(replay_balances)} account/currency pairs.")


def _send_webhook_alert(url: str, report: dict) -> None:
    """POST alert payload to webhook URL on failures."""
    try:
        data = json_mod.dumps(report).encode("utf-8")
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except (urllib.error.URLError, OSError) as exc:
        print(f"  {WARN}  Webhook alert failed: {exc}", file=sys.stderr)


def main():
    global json_mode

    parser = argparse.ArgumentParser(description="Ledger integrity checker")
    parser.add_argument("--report-only", action="store_true",
                        help="Print report and exit 0 even if issues found")
    parser.add_argument("--json", action="store_true",
                        help="Output structured JSON report")
    parser.add_argument("--webhook-url", type=str, default=None,
                        help="POST alert payload to this URL on failures")
    args = parser.parse_args()

    json_mode = args.json
    webhook_url = args.webhook_url or os.environ.get("RECON_WEBHOOK_URL")

    if not json_mode:
        print(f"\n{BOLD}Stablecoin Ledger Integrity Checker{RESET}")
        print(f"Database: {DATABASE_URL.split('@')[-1]}")
        print("─" * 60)

    engine = create_engine(DATABASE_URL, echo=False)
    Session = sessionmaker(bind=engine)
    db = Session()

    try:
        run_checks(db)
    finally:
        db.close()

    report = {
        "checks_passed": checks_ok,
        "issues": issues,
        "warnings": warnings,
        "status": "fail" if issues else "ok",
    }

    if json_mode:
        print(json_mod.dumps(report, indent=2))
    else:
        print(f"\n{'─' * 60}")
        print(f"  Checks passed : {BOLD}{checks_ok}{RESET}")
        print(f"  Issues found  : {BOLD}{len(issues)}{RESET}")
        print(f"  Warnings      : {BOLD}{len(warnings)}{RESET}")

        if issues:
            print(f"\n{BOLD}Issues:{RESET}")
            for i in issues:
                print(f"    {FAIL}  {i}")

        if warnings:
            print(f"\n{BOLD}Warnings:{RESET}")
            for w in warnings:
                print(f"    {WARN}  {w}")

    if webhook_url and issues:
        _send_webhook_alert(webhook_url, report)

    if issues and not args.report_only:
        if not json_mode:
            print(f"\n{BOLD}\033[31mLEDGER INTEGRITY FAILED — {len(issues)} issue(s) require investigation.\033[0m\n")
        sys.exit(1)
    else:
        if not json_mode:
            print(f"\n{BOLD}\033[32mLedger integrity OK.\033[0m\n")


if __name__ == "__main__":
    main()
