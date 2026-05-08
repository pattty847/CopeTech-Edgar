from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from .config import BACKTEST_CSV, REPORT_PATH
from .summary import SUMMARY_CSV, TOP_CHANGES_CSV, money


def corr(df: pd.DataFrame, left: str, right: str) -> float | None:
    subset = df[[left, right]].dropna()
    if len(subset) < 2:
        return None
    return float(subset[left].corr(subset[right]))


def hit_rate(series: pd.Series) -> float | None:
    clean = series.dropna()
    if clean.empty:
        return None
    return float((clean > 0).mean())


def fmt(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.4f}"


def pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value * 100:.2f}%"


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "No rows available."
    columns = [str(column) for column in df.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in df.iterrows():
        values = []
        for column in df.columns:
            value = row[column]
            values.append(fmt(float(value)) if isinstance(value, float) else str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def whole_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{int(value):,}"


def manager_summary_table(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "No manager summary available."
    display = summary.copy()
    display["market_value_this_q"] = display["market_value_this_q"].map(money)
    display["market_value_prev_q"] = display["market_value_prev_q"].map(money)
    display["top_10_holdings_pct"] = display["top_10_holdings_pct"].map(pct)
    for column in ("number_of_holdings", "new_purchases", "added_to", "reduced"):
        display[column] = display[column].map(whole_number)
    columns = [
        "manager_name",
        "report_date",
        "market_value_this_q",
        "market_value_prev_q",
        "number_of_holdings",
        "new_purchases",
        "added_to",
        "reduced",
        "top_10_holdings_pct",
    ]
    return markdown_table(display[columns])


def top_changes_table(changes: pd.DataFrame, limit: int = 20) -> str:
    if changes.empty:
        return "No top changes available."
    display = changes.copy()
    display = display.reindex(display["value_change_usd"].abs().sort_values(ascending=False).index).head(limit)
    display["value_change_usd"] = display["value_change_usd"].map(money)
    columns = [
        column for column in ("manager_name", "ticker", "issuer_name", "put_call", "direction", "value_change_usd")
        if column in display.columns
    ]
    return markdown_table(display[columns])


def write_report(con: duckdb.DuckDBPyConnection, csv_path: Path = BACKTEST_CSV, report_path: Path = REPORT_PATH) -> str:
    backtest = pd.read_csv(csv_path) if csv_path.exists() else pd.DataFrame()
    summary = pd.read_csv(SUMMARY_CSV) if SUMMARY_CSV.exists() else pd.DataFrame()
    top_changes = pd.read_csv(TOP_CHANGES_CSV) if TOP_CHANGES_CSV.exists() else pd.DataFrame()
    if not top_changes.empty:
        mapped = con.execute("SELECT cusip, ticker FROM cusip_ticker_map").fetchdf()
        if not mapped.empty:
            top_changes = top_changes.merge(mapped, on="cusip", how="left")
    counts = {
        "managers": con.execute("SELECT COUNT(*) FROM manager_meta").fetchone()[0],
        "filings": con.execute("SELECT COUNT(*) FROM filings").fetchone()[0],
        "holdings": con.execute("SELECT COUNT(*) FROM holdings").fetchone()[0],
        "mapped": con.execute("SELECT COUNT(*) FROM cusip_ticker_map WHERE ticker IS NOT NULL").fetchone()[0],
        "unmapped": con.execute("SELECT COUNT(*) FROM cusip_ticker_map WHERE ticker IS NULL").fetchone()[0],
        "signals": con.execute("SELECT COUNT(*) FROM consensus_signals").fetchone()[0],
    }

    correlations = []
    for score in ("self_dev_z", "herd_dev_z"):
        for ret in ("return_30d", "return_60d", "return_90d"):
            correlations.append((score, ret, corr(backtest, score, ret) if not backtest.empty else None))

    top_decile = pd.DataFrame()
    if not backtest.empty and "self_dev_z" in backtest:
        threshold = backtest["self_dev_z"].quantile(0.9)
        top_decile = backtest[backtest["self_dev_z"] >= threshold]
    baseline_hit = hit_rate(backtest["return_60d"]) if not backtest.empty and "return_60d" in backtest else None
    top_hit = hit_rate(top_decile["return_60d"]) if not top_decile.empty else None

    manager_breakdown = (
        backtest.groupby("manager_cik")["return_60d"].agg(["count", "mean"]).reset_index()
        if not backtest.empty else pd.DataFrame()
    )
    kind_breakdown = (
        backtest.groupby("holding_kind")["return_60d"].agg(["count", "mean"]).reset_index()
        if not backtest.empty else pd.DataFrame()
    )

    max_corr = max((abs(value) for _, _, value in correlations if value is not None), default=0)
    if backtest.empty:
        assessment = (
            "Summary-only run complete: use the Finviz-lite manager summary and top-change tables for the demo, "
            "then run the full backtest only after choosing a narrower mapped signal set."
        )
    elif max_corr >= 0.05:
        assessment = "Proceed cautiously: at least one correlation cleared 0.05, but this is still a tiny curated sample."
    else:
        assessment = "Pause/rethink: correlations are weak in this Phase 0 run; do not overbuild before improving signal design."

    content = [
        "# 13F Phase 0 Thesis Report",
        "",
        "## Counts",
        *(f"- {key}: {value}" for key, value in counts.items()),
        "",
        "## Correlations",
        *(f"- {score} vs {ret}: {fmt(value)}" for score, ret, value in correlations),
        "",
        "## Hit Rate",
        f"- baseline 60d positive-return rate: {fmt(baseline_hit)}",
        f"- top-decile self_dev_z 60d positive-return rate: {fmt(top_hit)}",
        "",
        "## Finviz-Lite Manager Summary",
        manager_summary_table(summary),
        "",
        "## Top Quarter Changes",
        top_changes_table(top_changes),
        "",
        "## Manager Breakdown",
        markdown_table(manager_breakdown) if not manager_breakdown.empty else "No manager breakdown available.",
        "",
        "## Holding Kind Breakdown",
        markdown_table(kind_breakdown) if not kind_breakdown.empty else "No holding-kind breakdown available.",
        "",
        "## Honest Assessment",
        assessment,
        "",
        "Sector breadth was skipped in Phase 0 unless ticker enrichment is added later.",
    ]
    report_path.write_text("\n".join(content), encoding="utf-8")
    return str(report_path)
