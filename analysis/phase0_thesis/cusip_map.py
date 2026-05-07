from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Protocol

import duckdb

from .db import upsert_rows


OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
OPENFIGI_ANONYMOUS_BATCH_SIZE = 10
OPENFIGI_KEYED_BATCH_SIZE = 100
MANUAL_CUSIP_TICKERS = {
    "037833100": "AAPL",
    "30303M102": "META",
    "46090E103": "QQQ",
    "464287655": "IWM",
    "57636Q104": "MA",
    "594972408": "MSTR",
    "64110L106": "NFLX",
    "67066G104": "NVDA",
    "78462F103": "SPY",
    "78463V107": "GLD",
    "88160R101": "TSLA",
}


class FigiClient(Protocol):
    def map_cusips(self, cusips: list[str]) -> dict[str, str | None]:
        ...


class OpenFigiClient:
    def __init__(self, api_key: str | None = None, sleep_seconds: float | None = None, max_retries: int = 3):
        self.api_key = api_key or os.environ.get("OPENFIGI_API_KEY")
        self.sleep_seconds = sleep_seconds if sleep_seconds is not None else (0.35 if self.api_key else 2.5)
        self.max_retries = max_retries

    def map_cusips(self, cusips: list[str]) -> dict[str, str | None]:
        if not cusips:
            return {}
        payload = json.dumps([{"idType": "ID_CUSIP", "idValue": cusip} for cusip in cusips]).encode("utf-8")
        request = urllib.request.Request(
            OPENFIGI_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                **({"X-OPENFIGI-APIKEY": self.api_key} if self.api_key else {}),
            },
            method="POST",
        )
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    data = json.loads(response.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as error:
                if error.code != 429 or attempt >= self.max_retries:
                    raise
                retry_after = float(error.headers.get("ratelimit-reset") or error.headers.get("Retry-After") or 60)
                time.sleep(max(retry_after, self.sleep_seconds))
        time.sleep(self.sleep_seconds)
        mapped: dict[str, str | None] = {}
        for cusip, item in zip(cusips, data):
            rows = item.get("data") or []
            mapped[cusip] = rows[0].get("ticker") if rows else None
        return mapped


def normalize_name(value: str) -> str:
    return " ".join(
        value.upper()
        .replace(",", " ")
        .replace(".", " ")
        .replace("-", " ")
        .split()
    )


def sec_name_match(issuer_name: str, company_rows: list[dict]) -> str | None:
    issuer = normalize_name(issuer_name)
    if not issuer:
        return None
    best_ticker = None
    best_score = 0
    issuer_tokens = set(issuer.split()) - {"INC", "CORP", "CORPORATION", "CO", "LTD", "PLC", "CLASS", "COM"}
    for row in company_rows:
        title = normalize_name(str(row.get("title", "")))
        ticker = row.get("ticker")
        title_tokens = set(title.split()) - {"INC", "CORP", "CORPORATION", "CO", "LTD", "PLC", "CLASS", "COM"}
        if not ticker or not title_tokens:
            continue
        score = len(issuer_tokens & title_tokens)
        if score > best_score and (score >= 2 or title in issuer or issuer in title):
            best_score = score
            best_ticker = str(ticker).upper()
    return best_ticker


def load_sec_company_rows() -> list[dict]:
    url = "https://www.sec.gov/files/company_tickers.json"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": os.environ.get("SEC_API_USER_AGENT") or "CopeTech-Edgar phase0 contact@example.com"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
    return list(data.values())


class CusipMapper:
    def __init__(self, con: duckdb.DuckDBPyConnection, figi_client: FigiClient | None = None):
        self.con = con
        self.figi_client = figi_client or OpenFigiClient()

    def cached(self, cusips: list[str]) -> dict[str, str | None]:
        if not cusips:
            return {}
        placeholders = ",".join(["?"] * len(cusips))
        rows = self.con.execute(
            f"SELECT cusip, ticker FROM cusip_ticker_map WHERE cusip IN ({placeholders})",
            cusips,
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def map_holdings(self, holdings: list[dict], company_rows: list[dict] | None = None) -> dict[str, str | None]:
        unique = sorted({str(row.get("cusip", "")).strip().upper() for row in holdings if row.get("cusip")})
        result = self.cached(unique)
        now = datetime.now(timezone.utc).isoformat()
        by_cusip = {str(row.get("cusip", "")).strip().upper(): row for row in holdings}
        manual_rows = []
        for cusip in unique:
            if cusip not in MANUAL_CUSIP_TICKERS:
                continue
            result[cusip] = MANUAL_CUSIP_TICKERS[cusip]
            manual_rows.append(
                {
                    "cusip": cusip,
                    "ticker": result[cusip],
                    "issuer_name": by_cusip.get(cusip, {}).get("issuer_name"),
                    "mapping_confidence": "manual_override",
                    "source_updated_at": now,
                }
            )
        upsert_rows(self.con, "cusip_ticker_map", manual_rows)

        missing = [cusip for cusip in unique if cusip not in result]
        figi_result: dict[str, str | None] = {}
        batch_size = OPENFIGI_KEYED_BATCH_SIZE if getattr(self.figi_client, "api_key", None) else OPENFIGI_ANONYMOUS_BATCH_SIZE
        for index in range(0, len(missing), batch_size):
            batch = missing[index:index + batch_size]
            figi_result.update(self.figi_client.map_cusips(batch))
        company_rows = company_rows if company_rows is not None else load_sec_company_rows()
        rows = []

        for cusip in missing:
            ticker = figi_result.get(cusip)
            confidence = "openfigi" if ticker else "unmapped"
            if not ticker:
                ticker = sec_name_match(str(by_cusip.get(cusip, {}).get("issuer_name", "")), company_rows)
                confidence = "sec_name_match" if ticker else "unmapped"
            result[cusip] = ticker
            rows.append(
                {
                    "cusip": cusip,
                    "ticker": ticker,
                    "issuer_name": by_cusip.get(cusip, {}).get("issuer_name"),
                    "mapping_confidence": confidence,
                    "source_updated_at": now,
                }
            )

        upsert_rows(self.con, "cusip_ticker_map", rows)
        return result
