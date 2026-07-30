"""SQLite persistence for immutable, versioned SEC financial facts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import aiosqlite


FACT_IDENTITY_FIELDS = (
    "cik",
    "metric",
    "taxonomy",
    "concept",
    "unit",
    "period_start",
    "period_end",
    "accession_number",
)
FACT_CONTENT_FIELDS = (
    *FACT_IDENTITY_FIELDS,
    "value",
    "duration_days",
    "fiscal_year",
    "fiscal_period",
    "form",
    "filed",
    "frame",
    "concept_priority",
    "quality_flags",
)


def financial_fact_content_hash(row: dict[str, Any]) -> str:
    """Return a stable hash of normalized fact content, excluding acquisition time."""

    material = {
        field: sorted(row.get(field) or [])
        if field == "quality_flags"
        else row.get(field)
        for field in FACT_CONTENT_FIELDS
    }
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class FinancialSeriesStore:
    """Append-only fact ledger with a compatibility migration from the 0.1 table."""

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    async def initialize(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            # Retain the original table during 0.2.x so existing installations can be
            # migrated without destroying their only locally acquired evidence.
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
                CREATE TABLE IF NOT EXISTS financial_fact_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    cik TEXT NOT NULL,
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
                    acquired_at TEXT NOT NULL,
                    normalization_version INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    quality_flags TEXT NOT NULL,
                    UNIQUE (
                        cik, metric, taxonomy, concept, unit, period_start,
                        period_end, accession_number, normalization_version,
                        content_hash
                    )
                )
                """
            )
            await db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_financial_fact_versions_query
                ON financial_fact_versions (
                    symbol, metric, normalization_version, filed, period_end
                )
                """
            )
            await self._migrate_legacy_rows(db)
            await db.commit()

    async def _migrate_legacy_rows(self, db: aiosqlite.Connection) -> None:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM financial_facts ORDER BY id") as cursor:
            legacy_rows = [dict(row) for row in await cursor.fetchall()]
        if not legacy_rows:
            return
        for row in legacy_rows:
            row["quality_flags"] = json.loads(row.get("quality_flags") or "[]")
        await self._insert_versions(db, legacy_rows)

    async def append_facts(self, rows: Iterable[dict[str, Any]]) -> int:
        """Append unseen versions and return the number of ledger rows inserted."""

        materialized = list(rows)
        if not materialized:
            return 0
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as db:
            before = db.total_changes
            await self._insert_versions(db, materialized)
            inserted = db.total_changes - before
            await db.commit()
        return inserted

    async def upsert_facts(self, rows: list[dict[str, Any]]) -> None:
        """Compatibility alias; facts are append-only as of normalization v2."""

        await self.append_facts(rows)

    async def _insert_versions(
        self,
        db: aiosqlite.Connection,
        rows: Iterable[dict[str, Any]],
    ) -> None:
        values = []
        for row in rows:
            content_hash = row.get("content_hash") or financial_fact_content_hash(row)
            values.append(
                (
                    row["symbol"].upper(),
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
                    row.get("acquired_at") or row["retrieved_at"],
                    row["normalization_version"],
                    content_hash,
                    json.dumps(sorted(row.get("quality_flags") or [])),
                )
            )
        await db.executemany(
            """
            INSERT OR IGNORE INTO financial_fact_versions (
                symbol, cik, entity_name, metric, taxonomy, concept,
                concept_priority, value, unit, period_start, period_end,
                duration_days, fiscal_year, fiscal_period, form, filed,
                accession_number, frame, acquired_at, normalization_version,
                content_hash, quality_flags
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            values,
        )

    async def load_facts(self, symbol: str, metric: str) -> list[dict[str, Any]]:
        """Load the newest normalization for each immutable SEC fact identity."""

        await self.initialize()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                cik, metric, taxonomy, concept, unit,
                                period_start, period_end, accession_number
                            ORDER BY
                                normalization_version DESC,
                                acquired_at DESC,
                                id DESC
                        ) AS version_rank
                    FROM financial_fact_versions
                    WHERE symbol = ? AND metric = ?
                )
                SELECT * FROM ranked
                WHERE version_rank = 1
                ORDER BY period_end, filed, accession_number
                """,
                (symbol.upper(), metric),
            ) as cursor:
                rows = await cursor.fetchall()
        output: list[dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            row.pop("version_rank", None)
            row["quality_flags"] = json.loads(row.get("quality_flags") or "[]")
            # Preserve the established internal key while exposing acquisition semantics.
            row["retrieved_at"] = row["acquired_at"]
            output.append(row)
        return output

    async def count_facts(self, symbol: str, metric: str) -> int:
        return len(await self.load_facts(symbol, metric))

    async def count_versions(self, symbol: str, metric: str) -> int:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT COUNT(*) FROM financial_fact_versions
                WHERE symbol = ? AND metric = ?
                """,
                (symbol.upper(), metric),
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0
