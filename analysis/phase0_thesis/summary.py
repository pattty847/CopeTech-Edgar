from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

import duckdb

from .config import PHASE0_DIR
from .metrics import compute_quarter_diffs


SUMMARY_CSV = PHASE0_DIR / "manager_summary.csv"
TOP_CHANGES_CSV = PHASE0_DIR / "top_changes.csv"


def money(value: float) -> str:
    abs_value = abs(value)
    sign = "-" if value < 0 else ""
    if abs_value >= 1_000_000_000:
        return f"{sign}${abs_value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{sign}${abs_value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{sign}${abs_value / 1_000:.2f}K"
    return f"{sign}${abs_value:.0f}"


def finviz_lite_stats(prior_rows: list[dict], current_rows: list[dict]) -> dict[str, float]:
    current_total = sum(float(row.get("value_usd") or 0) for row in current_rows)
    prior_total = sum(float(row.get("value_usd") or 0) for row in prior_rows)
    sorted_values = sorted((float(row.get("value_usd") or 0) for row in current_rows), reverse=True)
    diffs = compute_quarter_diffs(prior_rows, current_rows)
    return {
        "market_value_this_q": current_total,
        "market_value_prev_q": prior_total,
        "number_of_holdings": float(len(current_rows)),
        "new_purchases": float(sum(1 for row in diffs if row["direction"] == "added")),
        "added_to": float(sum(1 for row in diffs if row["direction"] == "increased")),
        "reduced": float(sum(1 for row in diffs if row["direction"] == "reduced")),
        "sold_out": float(sum(1 for row in diffs if row["direction"] == "sold_out")),
        "top_10_holdings_pct": sum(sorted_values[:10]) / current_total if current_total else 0.0,
    }


def latest_quarter_rows(con: duckdb.DuckDBPyConnection) -> dict[str, tuple[str, str | None, list[dict], list[dict]]]:
    holdings = con.execute("""
        SELECT h.*, m.display_name
        FROM holdings h
        LEFT JOIN manager_meta m ON m.cik = h.manager_cik
        ORDER BY h.manager_cik, h.report_date
    """).fetchdf().to_dict("records")
    grouped: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    names: dict[str, str | None] = {}
    for row in holdings:
        grouped[row["manager_cik"]][row["report_date"]].append(row)
        names[row["manager_cik"]] = row.get("display_name")

    result = {}
    for manager_cik, quarters in grouped.items():
        report_dates = sorted(quarters)
        if not report_dates:
            continue
        latest = report_dates[-1]
        previous = report_dates[-2] if len(report_dates) >= 2 else None
        result[manager_cik] = (
            latest,
            names.get(manager_cik),
            quarters.get(previous, []) if previous else [],
            quarters[latest],
        )
    return result


def top_changes_for_manager(
    manager_cik: str,
    manager_name: str | None,
    report_date: str,
    prior_rows: list[dict],
    current_rows: list[dict],
    limit: int = 12,
) -> list[dict]:
    diffs = compute_quarter_diffs(prior_rows, current_rows)
    ranked = sorted(diffs, key=lambda row: abs(row["value_change_usd"]), reverse=True)
    return [
        {
            "manager_cik": manager_cik,
            "manager_name": manager_name,
            "report_date": report_date,
            "rank": index + 1,
            "issuer_name": row["issuer_name"],
            "cusip": row["cusip"],
            "put_call": row["put_call"],
            "holding_kind": row["holding_kind"],
            "direction": row["direction"],
            "prior_value_usd": row["prior_value_usd"],
            "current_value_usd": row["current_value_usd"],
            "value_change_usd": row["value_change_usd"],
        }
        for index, row in enumerate(ranked[:limit])
    ]


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def generate_manager_summary(con: duckdb.DuckDBPyConnection) -> tuple[list[dict], list[dict]]:
    summary_rows: list[dict] = []
    change_rows: list[dict] = []
    for manager_cik, (report_date, manager_name, prior_rows, current_rows) in latest_quarter_rows(con).items():
        stats = finviz_lite_stats(prior_rows, current_rows)
        summary_rows.append(
            {
                "manager_cik": manager_cik,
                "manager_name": manager_name,
                "report_date": report_date,
                **stats,
            }
        )
        change_rows.extend(
            top_changes_for_manager(manager_cik, manager_name, report_date, prior_rows, current_rows)
        )

    write_csv(SUMMARY_CSV, summary_rows)
    write_csv(TOP_CHANGES_CSV, change_rows)
    return summary_rows, change_rows


def top_change_mapping_candidates(change_rows: list[dict], limit: int = 250) -> list[dict]:
    seen = set()
    candidates = []
    for row in sorted(change_rows, key=lambda item: abs(float(item.get("value_change_usd") or 0)), reverse=True):
        key = row.get("cusip")
        if not key or key in seen:
            continue
        seen.add(key)
        candidates.append({"cusip": key, "issuer_name": row.get("issuer_name")})
        if len(candidates) >= limit:
            break
    return candidates
