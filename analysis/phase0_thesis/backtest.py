from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import yfinance as yf

from .config import BACKTEST_CSV


def nearest_close(history: pd.DataFrame, target_date: datetime) -> float | None:
    if history.empty:
        return None
    normalized = history.copy()
    normalized.index = pd.to_datetime(normalized.index).tz_localize(None)
    future = normalized[normalized.index >= target_date]
    if future.empty:
        return None
    value = future.iloc[0].get("Close")
    return float(value) if pd.notna(value) else None


def forward_returns(ticker: str, filing_date: str) -> dict[str, float | None]:
    start = datetime.fromisoformat(filing_date[:10])
    end = start + timedelta(days=130)
    history = yf.download(ticker, start=start.date().isoformat(), end=end.date().isoformat(), progress=False, auto_adjust=False)
    base = nearest_close(history, start)
    if not base:
        return {"return_30d": None, "return_60d": None, "return_90d": None}
    result = {}
    for days in (30, 60, 90):
        close = nearest_close(history, start + timedelta(days=days))
        result[f"return_{days}d"] = (close - base) / base if close is not None else None
    return result


def run_backtest(con: duckdb.DuckDBPyConnection, output_path: Path = BACKTEST_CSV) -> pd.DataFrame:
    signals = con.execute("""
        SELECT manager_cik, ticker, report_date, filing_date, self_dev_z, herd_dev_z, value_usd, direction, holding_kind
        FROM consensus_signals
        WHERE ticker IS NOT NULL AND ticker != ''
    """).fetchdf()
    rows = []
    cache: dict[tuple[str, str], dict[str, float | None]] = {}
    for row in signals.to_dict("records"):
        key = (row["ticker"], row["filing_date"])
        if key not in cache:
            cache[key] = forward_returns(row["ticker"], row["filing_date"])
        rows.append({**row, **cache[key], "sector": None})

    output = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path, index=False)
    return output


async def run_backtest_async(con: duckdb.DuckDBPyConnection, output_path: Path = BACKTEST_CSV) -> pd.DataFrame:
    return await asyncio.to_thread(run_backtest, con, output_path)
