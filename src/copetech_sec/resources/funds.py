"""SEC mutual-fund series and class ticker resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..identifiers import Cik, Ticker

if TYPE_CHECKING:
    from ..sec_api import SECDataFetcher


class FundsResource:
    """Resolve class tickers from SEC's ``company_tickers_mf.json`` directory."""

    FUND_TICKERS_ENDPOINT = "https://www.sec.gov/files/company_tickers_mf.json"

    def __init__(self, fetcher: SECDataFetcher):
        self._fetcher = fetcher

    async def get(
        self,
        symbol: str,
        *,
        use_cache: bool = True,
    ) -> dict[str, Any] | None:
        ticker = Ticker(symbol)
        records = await self._records(use_cache=use_cache)
        return next(
            (record for record in records if record["ticker"] == str(ticker)),
            None,
        )

    async def for_cik(
        self,
        cik: str,
        *,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        normalized_cik = str(Cik(cik))
        records = await self._records(use_cache=use_cache)
        return [record for record in records if record["cik"] == normalized_cik]

    async def _records(self, *, use_cache: bool) -> list[dict[str, Any]]:
        if use_cache:
            cached = await self._fetcher.cache_manager.load_fund_map()
            if isinstance(cached, dict) and isinstance(cached.get("records"), list):
                return cached["records"]

        payload = await self._fetcher.http_client.make_request(
            self.FUND_TICKERS_ENDPOINT,
            is_json=True,
        )
        records = self._normalize_payload(payload)
        await self._fetcher.cache_manager.save_fund_map(records)
        return records

    @staticmethod
    def _normalize_payload(payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            raise TypeError(
                f"SEC fund ticker map must be a dict, got {type(payload).__name__}."
            )
        fields = payload.get("fields")
        rows = payload.get("data")
        if not isinstance(fields, list) or not isinstance(rows, list):
            raise ValueError("SEC fund ticker map must contain fields and data arrays.")
        required = ("cik", "seriesId", "classId", "symbol")
        if any(field not in fields for field in required):
            raise ValueError("SEC fund ticker map is missing a required field.")
        indexes = {field: fields.index(field) for field in required}

        records: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < len(fields):
                continue
            symbol = row[indexes["symbol"]]
            if not symbol:
                continue
            records.append(
                {
                    "ticker": str(Ticker(symbol)),
                    "cik": str(Cik(row[indexes["cik"]])),
                    "series_id": str(row[indexes["seriesId"]]),
                    "class_id": str(row[indexes["classId"]]),
                    "source": FundsResource.FUND_TICKERS_ENDPOINT,
                }
            )
        return records
