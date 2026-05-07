from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from typing import Any

from copetech_sec.sec_api import SECDataFetcher
from copetech_sec.thirteenf_processor import THIRTEENF_FORMS, ThirteenFProcessor, normalize_cik

from .config import END_DATE, MANAGER_SET, START_DATE, ManagerSeed
from .db import connect, init_schema, upsert_rows


def select_filings_window(filings: list[dict], start_date: str = START_DATE, end_date: str = END_DATE) -> list[dict]:
    return [
        filing for filing in filings
        if filing.get("form") in THIRTEENF_FORMS
        and start_date <= (filing.get("report_date") or "") < end_date
    ]


def canonicalize_filings(filings: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for filing in filings:
        grouped[filing.get("report_date") or ""].append(filing)

    canonicalized: list[dict] = []
    for report_date, group in grouped.items():
        if not report_date:
            continue
        canonical = max(
            group,
            key=lambda filing: (
                filing.get("filing_date") or "",
                1 if str(filing.get("form", "")).endswith("/A") else 0,
                filing.get("accession_no") or "",
            ),
        )
        for filing in group:
            next_filing = dict(filing)
            next_filing["is_canonical"] = filing.get("accession_no") == canonical.get("accession_no")
            canonicalized.append(next_filing)
    return sorted(canonicalized, key=lambda filing: (filing["report_date"], filing["filing_date"]))


def infer_holding_kind(holding: dict[str, Any]) -> str:
    put_call = (holding.get("put_call") or "").strip().lower()
    if put_call == "call":
        return "call"
    if put_call == "put":
        return "put"

    issuer = (holding.get("issuer") or "").upper()
    title = (holding.get("title_of_class") or "").upper()
    etf_tokens = ["ETF", "TRUST", "SPDR", "ISHARES", "VANGUARD", "INVESCO", "QQQ", "S&P", "INDEX"]
    if any(token in issuer or token in title for token in etf_tokens):
        return "etf_or_index"
    if holding.get("shares") is not None and holding.get("cusip"):
        return "common"
    return "unknown"


def normalize_holding(manager_cik: str, report_date: str, accession_no: str, holding: dict[str, Any]) -> dict[str, Any]:
    put_call = (holding.get("put_call") or "").strip().upper()
    return {
        "manager_cik": normalize_cik(manager_cik),
        "report_date": report_date,
        "accession_no": accession_no,
        "cusip": (holding.get("cusip") or "").strip().upper(),
        "issuer_name": (holding.get("issuer") or "").strip().upper(),
        "put_call": put_call,
        "shares": holding.get("shares"),
        "value_usd": holding.get("value_thousands"),
        "holding_kind": infer_holding_kind(holding),
    }


def dedupe_holdings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            row["manager_cik"],
            row["report_date"],
            row["cusip"],
            row["put_call"],
            row["issuer_name"],
        )
        deduped[key] = row
    return list(deduped.values())


async def fetch_historical_manager(fetcher: SECDataFetcher, manager: ManagerSeed) -> tuple[list[dict], list[dict]]:
    filings = await fetcher.get_13f_filings(manager.cik, days_back=3650)
    windowed = select_filings_window(filings)
    canonicalized = canonicalize_filings(windowed)
    canonical_filings = [filing for filing in canonicalized if filing["is_canonical"]]
    holdings: list[dict] = []

    processor: ThirteenFProcessor = fetcher.thirteenf_processor
    for filing in canonical_filings:
        documents = await get_filing_documents_from_url(processor, filing)
        document_name = processor.choose_information_table_document(documents or [])
        if not document_name:
            logging.warning("No information table found for %s %s", manager.cik, filing["accession_no"])
            continue
        raw_xml = await download_filing_document_from_url(processor, filing, document_name)
        if not raw_xml:
            logging.warning("Could not download %s for %s", document_name, filing["accession_no"])
            continue
        parsed = processor.parse_information_table_xml(raw_xml)
        holdings.extend(
            normalize_holding(manager.cik, filing["report_date"], filing["accession_no"], row)
            for row in parsed
        )

    return canonicalized, dedupe_holdings(holdings)


async def get_filing_documents_from_url(processor: ThirteenFProcessor, filing: dict) -> list[dict] | None:
    index_url = f"{filing['url'].rstrip('/')}/index.json"
    index_data = await processor.http_client.make_archive_request(index_url, is_json=True)
    if not isinstance(index_data, dict):
        return None
    return [
        {
            "name": item.get("name"),
            "type": item.get("type"),
            "size": item.get("size"),
            "last_modified": item.get("last_modified"),
        }
        for item in index_data.get("directory", {}).get("item", [])
        if item.get("name")
    ]


async def download_filing_document_from_url(processor: ThirteenFProcessor, filing: dict, document_name: str) -> str | None:
    document_url = f"{filing['url'].rstrip('/')}/{document_name}"
    content = await processor.http_client.make_archive_request(document_url, is_json=False)
    return content if isinstance(content, str) else None


async def ingest_managers(db_path=None, managers: list[ManagerSeed] | None = None) -> dict[str, int]:
    managers = managers or MANAGER_SET
    con = connect(db_path) if db_path else connect()
    init_schema(con)
    upsert_rows(con, "manager_meta", [
        {"cik": manager.cik, "display_name": manager.display_name, "archetype_seed": manager.archetype_seed}
        for manager in managers
    ])

    fetcher = SECDataFetcher(user_agent=os.environ.get("SEC_API_USER_AGENT"))
    filing_count = 0
    holding_count = 0
    try:
        for manager in managers:
            logging.info("Ingesting 13F history for %s %s", manager.display_name, manager.cik)
            filings, holdings = await fetch_historical_manager(fetcher, manager)
            filing_rows = [
                {
                    "manager_cik": normalize_cik(manager.cik),
                    "accession_no": filing["accession_no"],
                    "form_type": filing["form"],
                    "report_date": filing["report_date"],
                    "filing_date": filing["filing_date"],
                    "is_canonical": bool(filing["is_canonical"]),
                }
                for filing in filings
            ]
            upsert_rows(con, "filings", filing_rows)
            upsert_rows(con, "holdings", holdings)
            filing_count += len(filing_rows)
            holding_count += len(holdings)
    finally:
        await fetcher.close()
        con.close()

    return {"managers": len(managers), "filings": filing_count, "holdings": holding_count}


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    result = asyncio.run(ingest_managers())
    print(result)


if __name__ == "__main__":
    main()
