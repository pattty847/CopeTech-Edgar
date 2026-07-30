"""Company identity, submissions, and raw XBRL resources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..sec_api import SECDataFetcher


class CompaniesResource:
    def __init__(self, fetcher: SECDataFetcher):
        self._fetcher = fetcher

    async def get(self, ticker: str, *, use_cache: bool = True) -> dict[str, Any] | None:
        return await self._fetcher.get_company_info(ticker, use_cache=use_cache)

    async def submissions(
        self,
        ticker: str,
        *,
        use_cache: bool = True,
    ) -> dict[str, Any] | None:
        return await self._fetcher.get_company_submissions(ticker, use_cache=use_cache)

    async def facts(
        self,
        ticker: str,
        *,
        use_cache: bool = True,
    ) -> dict[str, Any] | None:
        return await self._fetcher.get_company_facts(ticker, use_cache=use_cache)

    async def cik(self, ticker: str) -> str | None:
        return await self._fetcher.get_cik_for_ticker(ticker)
