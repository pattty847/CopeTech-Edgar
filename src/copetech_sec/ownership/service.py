"""Ownership acquisition and cache-fingerprint boundary."""

from __future__ import annotations

import hashlib
from typing import Any

from ..document_handler import FilingDocumentHandler, RawFilingResolver


SIGNAL_PAYLOAD_VERSION = 2


def ownership_payload_fingerprint(
    accessions: list[str],
    days_back: int,
    filing_limit: int,
    anchor_type: str,
) -> str:
    material = "|".join(
        [
            str(SIGNAL_PAYLOAD_VERSION),
            f"{int(days_back)}d",
            str(int(filing_limit)),
            str(anchor_type),
            *sorted(str(accession) for accession in accessions),
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class OwnershipFilingService:
    def __init__(
        self,
        document_handler: FilingDocumentHandler,
        cache_manager: Any | None,
    ):
        self.raw_filings = RawFilingResolver(document_handler, cache_manager)

    async def xml(self, accession_number: str, *, ticker: str | None = None) -> str | None:
        return await self.raw_filings.get_xml(accession_number, ticker=ticker)
