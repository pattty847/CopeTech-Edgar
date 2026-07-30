"""Insider ownership filings, normalized events, and signals."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..sec_api import SECDataFetcher


OWNERSHIP_FORMS = frozenset({"3", "3/A", "4", "4/A", "5", "5/A"})


class OwnershipResource:
    def __init__(self, fetcher: SECDataFetcher):
        self._fetcher = fetcher

    @staticmethod
    def _normalize_forms(forms: str | list[str] | tuple[str, ...]) -> list[str]:
        requested = [forms] if isinstance(forms, str) else list(forms)
        unknown = sorted(set(requested) - OWNERSHIP_FORMS)
        if unknown:
            raise ValueError(
                f"Unsupported ownership forms: {', '.join(unknown)}. "
                f"Expected one or more of {', '.join(sorted(OWNERSHIP_FORMS))}."
            )
        if not requested:
            raise ValueError("At least one ownership form is required.")
        return requested

    async def filings(
        self,
        ticker: str,
        *,
        forms: str | list[str] | tuple[str, ...] = tuple(sorted(OWNERSHIP_FORMS)),
        days_back: int = 730,
        use_cache: bool = True,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Discover Forms 3/4/5 and amendments as one ownership timeline."""
        return await self._fetcher.get_filings_page(
            ticker,
            self._normalize_forms(forms),
            days_back=days_back,
            use_cache=use_cache,
            limit=limit,
            cursor=cursor,
        )

    async def entries(
        self,
        ticker: str,
        *,
        forms: str | list[str] | tuple[str, ...] = tuple(sorted(OWNERSHIP_FORMS)),
        days_back: int = 730,
        use_cache: bool = True,
        filing_limit: int = 50,
        include_transactions: bool = True,
        include_holdings: bool = True,
    ) -> dict[str, Any]:
        """Parse first-class transaction and holding rows from Forms 3/4/5."""
        if filing_limit <= 0:
            raise ValueError("filing_limit must be greater than zero.")
        if not include_transactions and not include_holdings:
            raise ValueError(
                "At least one of include_transactions or include_holdings must be true."
            )
        requested_forms = self._normalize_forms(forms)
        page = await self.filings(
            ticker,
            forms=requested_forms,
            days_back=days_back,
            use_cache=use_cache,
            limit=filing_limit,
        )
        filings = page["items"]
        entries: list[dict[str, Any]] = []
        warnings = list(page.get("metadata", {}).get("warnings") or [])
        for filing in filings:
            rows = await self._fetcher.form4_processor.process_form4_filing(
                filing["accession_no"],
                ticker,
            )
            if not rows:
                warnings.append(f"ownership_document_empty:{filing['accession_no']}")
                continue
            for row in rows:
                is_holding = bool(row.get("is_holding"))
                if is_holding and not include_holdings:
                    continue
                if not is_holding and not include_transactions:
                    continue
                entries.append(
                    {
                        **row,
                        "accession_no": filing["accession_no"],
                        "filing_date": filing["filing_date"],
                        "report_date": filing.get("report_date"),
                        "filing_form": filing["form"],
                        "source_url": filing.get("url"),
                    }
                )
        return {
            "symbol": ticker.upper(),
            "forms": requested_forms,
            "filings": filings,
            "entries": entries,
            "metadata": {
                **page.get("metadata", {}),
                "warnings": sorted(set(warnings)),
            },
        }

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
