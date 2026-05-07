from __future__ import annotations

from pathlib import Path

import duckdb

from .config import DB_PATH


def connect(db_path: Path | str = DB_PATH) -> duckdb.DuckDBPyConnection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS manager_meta (
            cik TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            archetype_seed TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS filings (
            manager_cik TEXT NOT NULL,
            accession_no TEXT NOT NULL,
            form_type TEXT NOT NULL,
            report_date TEXT NOT NULL,
            filing_date TEXT NOT NULL,
            is_canonical BOOLEAN NOT NULL,
            PRIMARY KEY (manager_cik, accession_no)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS holdings (
            manager_cik TEXT NOT NULL,
            report_date TEXT NOT NULL,
            accession_no TEXT NOT NULL,
            cusip TEXT NOT NULL,
            issuer_name TEXT NOT NULL,
            put_call TEXT NOT NULL,
            shares DOUBLE,
            value_usd DOUBLE,
            holding_kind TEXT NOT NULL,
            PRIMARY KEY (manager_cik, report_date, cusip, put_call, issuer_name)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS cusip_ticker_map (
            cusip TEXT PRIMARY KEY,
            ticker TEXT,
            issuer_name TEXT,
            mapping_confidence TEXT NOT NULL,
            source_updated_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manager_metrics (
            manager_cik TEXT NOT NULL,
            report_date TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            value DOUBLE,
            PRIMARY KEY (manager_cik, report_date, metric_name)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS consensus_signals (
            manager_cik TEXT NOT NULL,
            report_date TEXT NOT NULL,
            filing_date TEXT NOT NULL,
            cusip TEXT NOT NULL,
            ticker TEXT,
            self_dev_z DOUBLE,
            herd_dev_z DOUBLE,
            value_usd DOUBLE,
            direction TEXT NOT NULL,
            holding_kind TEXT NOT NULL,
            PRIMARY KEY (manager_cik, report_date, cusip, direction, holding_kind)
        )
    """)


def upsert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(columns))
    column_sql = ", ".join(columns)
    con.executemany(
        f"INSERT OR REPLACE INTO {table} ({column_sql}) VALUES ({placeholders})",
        [tuple(row.get(column) for column in columns) for row in rows],
    )
