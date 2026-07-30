"""SQLite persistence for immutable SEC financial facts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import aiosqlite


class FinancialSeriesStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    async def initialize(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    cik TEXT,
                    entity_name TEXT,
                    metric TEXT NOT NULL,
                    taxonomy TEXT NOT NULL,
                    concept TEXT NOT NULL,
                    concept_priority INTEGER NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT NOT NULL,
                    period_start TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    duration_days INTEGER NOT NULL,
                    fiscal_year INTEGER,
                    fiscal_period TEXT,
                    form TEXT NOT NULL,
                    filed TEXT NOT NULL,
                    accession_number TEXT NOT NULL,
                    frame TEXT,
                    retrieved_at TEXT NOT NULL,
                    normalization_version INTEGER NOT NULL,
                    quality_flags TEXT NOT NULL,
                    UNIQUE (
                        symbol, metric, taxonomy, concept, unit,
                        period_start, period_end, accession_number
                    )
                )
                """
            )
            await db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_financial_facts_query
                ON financial_facts (symbol, metric, filed, period_end)
                """
            )
            await db.commit()

    async def upsert_facts(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        await self.initialize()
        values = [
            (
                row["symbol"],
                str(row.get("cik") or ""),
                row.get("entity_name"),
                row["metric"],
                row["taxonomy"],
                row["concept"],
                row["concept_priority"],
                row["value"],
                row["unit"],
                row["period_start"],
                row["period_end"],
                row["duration_days"],
                row.get("fiscal_year"),
                row.get("fiscal_period"),
                row["form"],
                row["filed"],
                row["accession_number"],
                row.get("frame"),
                row["retrieved_at"],
                row["normalization_version"],
                json.dumps(row.get("quality_flags") or []),
            )
            for row in rows
        ]
        async with aiosqlite.connect(self.db_path) as db:
            await db.executemany(
                """
                INSERT INTO financial_facts (
                    symbol, cik, entity_name, metric, taxonomy, concept,
                    concept_priority, value, unit, period_start, period_end,
                    duration_days, fiscal_year, fiscal_period, form, filed,
                    accession_number, frame, retrieved_at, normalization_version,
                    quality_flags
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT (
                    symbol, metric, taxonomy, concept, unit,
                    period_start, period_end, accession_number
                ) DO UPDATE SET
                    value=excluded.value,
                    filed=excluded.filed,
                    form=excluded.form,
                    frame=excluded.frame,
                    retrieved_at=excluded.retrieved_at,
                    normalization_version=excluded.normalization_version,
                    quality_flags=excluded.quality_flags
                """,
                values,
            )
            await db.commit()

    async def load_facts(self, symbol: str, metric: str) -> list[dict[str, Any]]:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM financial_facts
                WHERE symbol = ? AND metric = ?
                ORDER BY period_end, filed, accession_number
                """,
                (symbol.upper(), metric),
            ) as cursor:
                rows = await cursor.fetchall()
        output: list[dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            row["quality_flags"] = json.loads(row.get("quality_flags") or "[]")
            output.append(row)
        return output

    async def count_facts(self, symbol: str, metric: str) -> int:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM financial_facts WHERE symbol = ? AND metric = ?",
                (symbol.upper(), metric),
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0
