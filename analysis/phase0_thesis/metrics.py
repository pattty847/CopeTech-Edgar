from __future__ import annotations

from collections import defaultdict
from typing import Any

import duckdb

from .db import upsert_rows


def security_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("cusip") or "").upper(),
        str(row.get("put_call") or "").upper(),
        str(row.get("issuer_name") or "").upper(),
    )


def compute_quarter_diffs(prior_rows: list[dict], current_rows: list[dict]) -> list[dict]:
    prior = {security_key(row): row for row in prior_rows}
    current = {security_key(row): row for row in current_rows}
    diffs = []
    for key in sorted(set(prior) | set(current)):
        previous = prior.get(key)
        current_row = current.get(key)
        previous_value = float(previous.get("value_usd") or 0) if previous else 0.0
        current_value = float(current_row.get("value_usd") or 0) if current_row else 0.0
        if previous is None:
            direction = "added"
        elif current_row is None:
            direction = "sold_out"
        elif current_value > previous_value:
            direction = "increased"
        elif current_value < previous_value:
            direction = "reduced"
        else:
            direction = "unchanged"
        source = current_row or previous or {}
        diffs.append(
            {
                "cusip": key[0],
                "put_call": key[1],
                "issuer_name": key[2],
                "holding_kind": source.get("holding_kind", "unknown"),
                "prior_value_usd": previous_value,
                "current_value_usd": current_value,
                "value_change_usd": current_value - previous_value,
                "direction": direction,
            }
        )
    return diffs


def compute_manager_metrics(prior_rows: list[dict], current_rows: list[dict]) -> dict[str, float]:
    total = sum(float(row.get("value_usd") or 0) for row in current_rows)
    prior_total = sum(float(row.get("value_usd") or 0) for row in prior_rows)
    sorted_values = sorted((float(row.get("value_usd") or 0) for row in current_rows), reverse=True)
    options_value = sum(
        float(row.get("value_usd") or 0)
        for row in current_rows
        if row.get("holding_kind") in {"call", "put"}
    )
    diffs = compute_quarter_diffs(prior_rows, current_rows)
    changed_gross = sum(abs(row["value_change_usd"]) for row in diffs)
    return {
        "total_portfolio_value_usd": total,
        "holding_count": float(len(current_rows)),
        "top10_concentration": sum(sorted_values[:10]) / total if total else 0.0,
        "options_ratio": options_value / total if total else 0.0,
        "qoq_turnover": changed_gross / prior_total if prior_total else 0.0,
        "new_position_count": float(sum(1 for row in diffs if row["direction"] == "added")),
        "exit_count": float(sum(1 for row in diffs if row["direction"] == "sold_out")),
    }


def compute_and_store_metrics(con: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = con.execute("SELECT * FROM holdings ORDER BY manager_cik, report_date").fetchdf().to_dict("records")
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        grouped[(row["manager_cik"], row["report_date"])].append(row)

    metric_rows = []
    for manager_cik in sorted({key[0] for key in grouped}):
        report_dates = sorted(date for cik, date in grouped if cik == manager_cik)
        prior_rows: list[dict] = []
        for report_date in report_dates:
            current_rows = grouped[(manager_cik, report_date)]
            metrics = compute_manager_metrics(prior_rows, current_rows)
            metric_rows.extend(
                {
                    "manager_cik": manager_cik,
                    "report_date": report_date,
                    "metric_name": metric_name,
                    "value": value,
                }
                for metric_name, value in metrics.items()
            )
            prior_rows = current_rows

    upsert_rows(con, "manager_metrics", metric_rows)
    return metric_rows
