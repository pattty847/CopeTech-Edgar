"""Namespaced public client for CopeTech-Edgar."""

from __future__ import annotations

from typing import Any

from .resources import (
    CompaniesResource,
    FilingsResource,
    FinancialsResource,
    InstitutionsResource,
    OwnershipResource,
)
from .sec_api import SECDataFetcher


class EdgarClient:
    """Stable public API grouped by EDGAR domain.

    ``SECDataFetcher`` remains available as a 0.2.x compatibility facade, but new
    integrations should enter through these explicit resource namespaces.
    """

    def __init__(
        self,
        user_agent: str | None = None,
        cache_dir: str = "data/edgar",
        rate_limit_sleep: float = 0.1,
        *,
        _fetcher: SECDataFetcher | None = None,
    ):
        self._fetcher = _fetcher or SECDataFetcher(
            user_agent=user_agent,
            cache_dir=cache_dir,
            rate_limit_sleep=rate_limit_sleep,
        )
        self.companies = CompaniesResource(self._fetcher)
        self.filings = FilingsResource(self._fetcher)
        self.ownership = OwnershipResource(self._fetcher)
        self.institutions = InstitutionsResource(self._fetcher)
        self.financials = FinancialsResource(self._fetcher)

    async def close(self) -> None:
        await self._fetcher.close()

    async def __aenter__(self) -> EdgarClient:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.close()
