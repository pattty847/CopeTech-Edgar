"""Canonical financial and valuation series."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..sec_api import SECDataFetcher


class FinancialsResource:
    def __init__(self, fetcher: SECDataFetcher):
        self._fetcher = fetcher

    def metrics(self) -> list[dict[str, Any]]:
        return self._fetcher.list_supported_financial_metrics()

    async def series(
        self,
        ticker: str,
        *,
        metric: str = "revenue",
        frequency: str = "quarterly",
        basis: str = "canonical",
        alignment: str = "availability",
        as_of: str | None = None,
        start: str | None = None,
        end: str | None = None,
        refresh: bool = False,
        include_provenance: bool = True,
        split_events: list[tuple[str, float]] | None = None,
    ) -> dict[str, Any] | None:
        return await self._fetcher.get_financial_series(
            ticker,
            metric=metric,
            frequency=frequency,
            basis=basis,
            alignment=alignment,
            as_of=as_of,
            start=start,
            end=end,
            refresh=refresh,
            include_provenance=include_provenance,
            split_events=split_events,
        )

    async def valuation(
        self,
        ticker: str,
        *,
        price_observations: list[dict[str, Any]],
        split_events: list[tuple[str, float]] | None = None,
        price_source: str = "caller",
        price_basis: str = "split_adjusted",
        refresh: bool = False,
        include_provenance: bool = True,
    ) -> dict[str, Any] | None:
        return await self._fetcher.get_valuation_series(
            ticker,
            price_observations=price_observations,
            split_events=split_events,
            price_source=price_source,
            price_basis=price_basis,
            refresh=refresh,
            include_provenance=include_provenance,
        )

    async def summary(
        self,
        ticker: str,
        *,
        use_cache: bool = True,
    ) -> dict[str, Any] | None:
        return await self._fetcher.get_financial_summary(ticker, use_cache=use_cache)

    async def trend(
        self,
        ticker: str,
        *,
        periods: int = 8,
        use_cache: bool = True,
    ) -> dict[str, Any] | None:
        return await self._fetcher.get_financial_trend(
            ticker,
            periods=periods,
            use_cache=use_cache,
        )
