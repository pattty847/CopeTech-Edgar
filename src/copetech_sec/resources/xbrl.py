"""Bounded SEC XBRL Company Concept and Frames acquisition."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from ..errors import SecNotFoundError
from ..identifiers import Cik, Ticker

if TYPE_CHECKING:
    from ..sec_api import SECDataFetcher


_PATH_SEGMENT = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")
_UNIT = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*(?:-per-[A-Za-z][A-Za-z0-9_-]*)?$")
_FRAME = re.compile(r"^CY\d{4}(?:Q[1-4]I?)?$")


def _validated_segment(value: str, *, name: str) -> str:
    candidate = str(value).strip()
    if not _PATH_SEGMENT.fullmatch(candidate):
        raise ValueError(
            f"{name} must start with a letter and contain only letters, digits, "
            "underscores, or hyphens."
        )
    return candidate


def _validated_unit(value: str) -> str:
    candidate = str(value).strip()
    if not _UNIT.fullmatch(candidate):
        raise ValueError(
            "unit must be an XBRL measure such as USD, shares, pure, or "
            "USD-per-shares."
        )
    return candidate


def _validated_frame(value: str) -> str:
    candidate = str(value).strip().upper()
    if not _FRAME.fullmatch(candidate):
        raise ValueError(
            "period must be CY####, CY####Q#, or CY####Q#I."
        )
    return candidate


class XbrlResource:
    """Fetch narrow XBRL queries without downloading a full Company Facts blob."""

    COMPANY_CONCEPT_ENDPOINT = (
        "https://data.sec.gov/api/xbrl/companyconcept/"
        "CIK{cik}/{taxonomy}/{concept}.json"
    )
    FRAME_ENDPOINT = (
        "https://data.sec.gov/api/xbrl/frames/"
        "{taxonomy}/{concept}/{unit}/{period}.json"
    )

    def __init__(self, fetcher: SECDataFetcher):
        self._fetcher = fetcher

    async def company_concept(
        self,
        ticker: str,
        *,
        taxonomy: str,
        concept: str,
        use_cache: bool = True,
    ) -> dict[str, Any] | None:
        symbol = Ticker(ticker)
        normalized_taxonomy = _validated_segment(taxonomy, name="taxonomy")
        normalized_concept = _validated_segment(concept, name="concept")
        cik = await self._fetcher.get_cik_for_ticker(symbol)
        if cik is None:
            return None

        cache_key = f"CIK{Cik(cik)}"
        cache_kwargs = {
            "taxonomy": normalized_taxonomy,
            "concept": normalized_concept,
        }
        if use_cache:
            cached = await self._fetcher.cache_manager.load_data(
                cache_key,
                "xbrl_concepts",
                **cache_kwargs,
            )
            if isinstance(cached, dict):
                return cached

        try:
            payload = await self._fetcher.http_client.make_request(
                self.COMPANY_CONCEPT_ENDPOINT.format(
                    cik=Cik(cik),
                    taxonomy=normalized_taxonomy,
                    concept=normalized_concept,
                ),
                is_json=True,
            )
        except SecNotFoundError:
            return None
        if not isinstance(payload, dict):
            raise TypeError(
                "SEC Company Concept response must be a dict, "
                f"got {type(payload).__name__}."
            )
        await self._fetcher.cache_manager.save_data(
            cache_key,
            "xbrl_concepts",
            payload,
            **cache_kwargs,
        )
        return payload

    async def frame(
        self,
        *,
        taxonomy: str,
        concept: str,
        unit: str,
        period: str,
        use_cache: bool = True,
    ) -> dict[str, Any] | None:
        normalized_taxonomy = _validated_segment(taxonomy, name="taxonomy")
        normalized_concept = _validated_segment(concept, name="concept")
        normalized_unit = _validated_unit(unit)
        normalized_period = _validated_frame(period)
        cache_kwargs = {
            "taxonomy": normalized_taxonomy,
            "concept": normalized_concept,
            "unit": normalized_unit,
            "period": normalized_period,
        }
        if use_cache:
            cached = await self._fetcher.cache_manager.load_data(
                "GLOBAL",
                "xbrl_frames",
                **cache_kwargs,
            )
            if isinstance(cached, dict):
                return cached

        try:
            payload = await self._fetcher.http_client.make_request(
                self.FRAME_ENDPOINT.format(**cache_kwargs),
                is_json=True,
            )
        except SecNotFoundError:
            return None
        if not isinstance(payload, dict):
            raise TypeError(
                f"SEC XBRL Frame response must be a dict, got {type(payload).__name__}."
            )
        await self._fetcher.cache_manager.save_data(
            "GLOBAL",
            "xbrl_frames",
            payload,
            **cache_kwargs,
        )
        return payload
