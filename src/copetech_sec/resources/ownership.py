"""Insider ownership filings, normalized events, and signals."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..sec_api import SECDataFetcher


class OwnershipResource:
    def __init__(self, fetcher: SECDataFetcher):
        self._fetcher = fetcher

    async def transactions(
        self,
        ticker: str,
        *,
        days_back: int = 90,
        use_cache: bool = True,
        filing_limit: int = 10,
    ) -> list[dict[str, Any]]:
        return await self._fetcher.get_recent_insider_transactions(
            ticker,
            days_back=days_back,
            use_cache=use_cache,
            filing_limit=filing_limit,
        )

    async def signals(
        self,
        ticker: str,
        *,
        days_back: int = 180,
        use_cache: bool = True,
        filing_limit: int = 40,
        anchor_type: str = "filing_date",
    ) -> dict[str, Any]:
        return await self._fetcher.get_insider_signal_payload(
            ticker,
            days_back=days_back,
            use_cache=use_cache,
            filing_limit=filing_limit,
            anchor_type=anchor_type,
        )

    async def refresh_signals(
        self,
        ticker: str,
        *,
        days_back: int = 180,
        filing_limit: int = 40,
        anchor_type: str = "filing_date",
    ) -> dict[str, Any]:
        return await self._fetcher.refresh_insider_signal_payload(
            ticker,
            days_back=days_back,
            filing_limit=filing_limit,
            anchor_type=anchor_type,
        )
