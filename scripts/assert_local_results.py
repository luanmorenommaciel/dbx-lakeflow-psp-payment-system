#!/usr/bin/env python3
"""Assert the workshop's deterministic local SDP outputs."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq


def rows(table: str) -> list[dict]:
    paths = sorted(Path("spark-warehouse", table).glob("*.parquet"))
    if not paths:
        raise SystemExit(f"LOCAL_ASSERT=FAIL reason=missing_table table={table}")
    return pq.read_table(paths).to_pylist()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("baseline", "replay"), required=True)
    args = parser.parse_args()

    risk = rows("gold_merchant_risk")
    focus = next((row for row in risk if row["merchant_id"] == "m-007"), None)
    if focus is None:
        raise SystemExit("LOCAL_ASSERT=FAIL reason=focus_merchant_missing")

    expected_chargebacks = 0 if args.phase == "baseline" else 1
    expected_score = 25 if args.phase == "baseline" else 45
    expected_band = "normal" if args.phase == "baseline" else "elevated"
    checks = {
        "merchant_count": len(risk) == 12,
        "quarantine_count": focus["quarantine_count"] == 1209,
        "brl_rejected_count": focus["brl_rejected_count"] == 1204,
        "quality_risk_score": focus["quality_risk_score"] == 25,
        "chargeback_count": focus["chargeback_count"] == expected_chargebacks,
        "chargeback_risk_points": focus["chargeback_risk_points"] == expected_chargebacks * 20,
        "risk_score": focus["risk_score"] == expected_score,
        "risk_band": focus["risk_band"] == expected_band,
        "transaction_quarantine": len(rows("quarantine_transactions")) == 1209,
        "dispute_quarantine": len(rows("quarantine_disputes")) == 1,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(f"LOCAL_ASSERT=FAIL phase={args.phase} checks={','.join(failed)} row={focus}")
    print(
        "LOCAL_ASSERT=PASS "
        f"phase={args.phase} merchant=m-007 quarantined=1209 brl_rejected=1204 "
        f"chargebacks={expected_chargebacks} risk_score={expected_score} risk_band={expected_band}"
    )


if __name__ == "__main__":
    main()
