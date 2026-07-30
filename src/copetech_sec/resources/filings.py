"""Complete SEC submission-history queries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from ..sec_api import SECDataFetcher


class FilingsResource:
    def __init__(self, fetcher: SECDataFetcher):
        self._fetcher = fetcher

    async def query(
        self,
        ticker: str,
        forms: str | Iterable[str],
        *,
        days_back: int = 90,
        use_cache: bool = True,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return await self._fetcher.get_filings_page(
            ticker,
            forms,
            days_back=days_back,
            use_cache=use_cache,
            limit=limit,
            cursor=cursor,
        )

    async def annual(
        self,
        ticker: str,
        *,
        days_back: int = 730,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        return await self._fetcher.fetch_annual_reports(
            ticker,
            days_back=days_back,
            use_cache=use_cache,
        )

    async def quarterly(
        self,
        ticker: str,
        *,
        days_back: int = 365,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        return await self._fetcher.fetch_quarterly_reports(
            ticker,
            days_back=days_back,
            use_cache=use_cache,
        )

    async def current(
        self,
        ticker: str,
        *,
        days_back: int = 90,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        return await self._fetcher.fetch_current_reports(
            ticker,
            days_back=days_back,
            use_cache=use_cache,
        )
