from __future__ import annotations

import math
from collections import defaultdict

import duckdb

from .db import upsert_rows
from .metrics import compute_quarter_diffs


def zscore(value: float, values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((item - mean) ** 2 for item in values) / len(values)
    std = math.sqrt(variance)
    if std == 0:
        return 0.0
    return (value - mean) / std


def compute_self_deviation(history_values: list[float], current_value: float) -> float | None:
    if len(history_values) < 2:
        return None
    return zscore(current_value, history_values)


def compute_herd_deviation(manager_flow: float, peer_flows: list[float]) -> float | None:
    if len(peer_flows) < 3:
        return None
    return zscore(manager_flow, peer_flows)


def generate_consensus_signals(con: duckdb.DuckDBPyConnection) -> list[dict]:
    holdings = con.execute("""
        SELECT h.*, m.ticker, f.filing_date
        FROM holdings h
        LEFT JOIN cusip_ticker_map m USING (cusip)
        LEFT JOIN filings f
          ON f.manager_cik = h.manager_cik
         AND f.report_date = h.report_date
         AND f.accession_no = h.accession_no
        ORDER BY h.report_date, h.manager_cik
    """).fetchdf().to_dict("records")

    by_manager: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    by_quarter: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in holdings:
        by_manager[row["manager_cik"]][row["report_date"]].append(row)
        by_quarter[row["report_date"]][row["manager_cik"]].append(row)

    signals = []
    for manager_cik, quarters in by_manager.items():
        report_dates = sorted(quarters)
        prior_rows: list[dict] = []
        for report_date in report_dates:
            current_rows = quarters[report_date]
            diffs = compute_quarter_diffs(prior_rows, current_rows)
            current_by_key = {
                (row["cusip"], row["put_call"], row["issuer_name"]): row for row in current_rows
            }
            prior_history = [
                row for prior_date in report_dates
                if prior_date < report_date
                for row in quarters[prior_date]
            ]
            history_by_cusip: dict[str, list[float]] = defaultdict(list)
            for row in prior_history:
                history_by_cusip[row["cusip"]].append(float(row.get("value_usd") or 0))

            quarter_flows_by_cusip: dict[str, list[float]] = defaultdict(list)
            for peer_cik, peer_rows in by_quarter[report_date].items():
                peer_dates = sorted(by_manager[peer_cik])
                prior_date = max((date for date in peer_dates if date < report_date), default=None)
                peer_prior = by_manager[peer_cik][prior_date] if prior_date else []
                for diff in compute_quarter_diffs(peer_prior, peer_rows):
                    quarter_flows_by_cusip[diff["cusip"]].append(diff["value_change_usd"])

            for diff in diffs:
                prior_value = diff["prior_value_usd"]
                current_value = diff["current_value_usd"]
                material_increase = prior_value > 0 and current_value / prior_value > 1.5
                if diff["direction"] != "added" and not material_increase:
                    continue
                row = current_by_key.get((diff["cusip"], diff["put_call"], diff["issuer_name"]))
                if not row:
                    continue
                peer_flows = quarter_flows_by_cusip[diff["cusip"]]
                signals.append(
                    {
                        "manager_cik": manager_cik,
                        "report_date": report_date,
                        "filing_date": row.get("filing_date") or report_date,
                        "cusip": diff["cusip"],
                        "ticker": row.get("ticker"),
                        "self_dev_z": compute_self_deviation(history_by_cusip[diff["cusip"]], current_value),
                        "herd_dev_z": compute_herd_deviation(diff["value_change_usd"], peer_flows),
                        "value_usd": current_value,
                        "direction": diff["direction"],
                        "holding_kind": diff["holding_kind"],
                    }
                )
            prior_rows = current_rows

    con.execute("DELETE FROM consensus_signals")
    upsert_rows(con, "consensus_signals", signals)
    return signals
