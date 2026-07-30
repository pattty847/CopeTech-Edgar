"""Institutional-manager filing and holdings resources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..sec_api import SECDataFetcher


class InstitutionsResource:
    def __init__(self, fetcher: SECDataFetcher):
        self._fetcher = fetcher

    async def filings(
        self,
        cik: str,
        *,
        days_back: int = 1095,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        return await self._fetcher.get_13f_filings(
            cik,
            days_back=days_back,
            use_cache=use_cache,
        )

    async def latest_holdings(
        self,
        cik: str,
        *,
        days_back: int = 1095,
        use_cache: bool = True,
        row_limit: int | None = None,
    ) -> dict[str, Any] | None:
        return await self._fetcher.get_latest_13f_holdings(
            cik,
            days_back=days_back,
            use_cache=use_cache,
            row_limit=row_limit,
        )

    async def changes(
        self,
        cik: str,
        *,
        days_back: int = 1095,
        use_cache: bool = True,
        top_n: int = 25,
    ) -> dict[str, Any] | None:
        return await self._fetcher.get_13f_holdings_changes(
            cik,
            days_back=days_back,
            use_cache=use_cache,
            top_n=top_n,
        )
