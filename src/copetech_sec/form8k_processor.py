"""Form 8-K (Current Report) item-code parser.

8-K filings declare one or more "items" — structured event types with codes
like 2.02 (results) or 5.02 (exec change). The SEC publishes these per-filing
in `submissions.recent.items` as a comma-separated string. This processor
turns that string into structured records keyed by code, with human labels
and a coarse category for routing/filtering.

No HTML parsing required for the basic event stream. A future extension
could pull body snippets per item from the primary document.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Optional


# SEC-defined 8-K item codes. Categories let callers filter or weight by event
# type (e.g. surface only exec changes / results / material agreements).
ITEM_CODE_MAP: Dict[str, Dict[str, str]] = {
    "1.01": {"label": "Entry into a Material Definitive Agreement", "category": "material_agreement"},
    "1.02": {"label": "Termination of a Material Definitive Agreement", "category": "material_agreement"},
    "1.03": {"label": "Bankruptcy or Receivership", "category": "distress"},
    "1.04": {"label": "Mine Safety - Reporting of Shutdowns and Patterns of Violations", "category": "regulatory"},
    "2.01": {"label": "Completion of Acquisition or Disposition of Assets", "category": "m_and_a"},
    "2.02": {"label": "Results of Operations and Financial Condition", "category": "financial_results"},
    "2.03": {"label": "Creation of a Direct Financial Obligation or Off-Balance Sheet Arrangement", "category": "financing"},
    "2.04": {"label": "Triggering Events That Accelerate or Increase a Direct Financial Obligation", "category": "distress"},
    "2.05": {"label": "Costs Associated with Exit or Disposal Activities", "category": "restructuring"},
    "2.06": {"label": "Material Impairments", "category": "distress"},
    "3.01": {"label": "Notice of Delisting or Failure to Satisfy a Continued Listing Rule", "category": "distress"},
    "3.02": {"label": "Unregistered Sales of Equity Securities", "category": "capital_structure"},
    "3.03": {"label": "Material Modification to Rights of Security Holders", "category": "capital_structure"},
    "4.01": {"label": "Changes in Registrant's Certifying Accountant", "category": "governance"},
    "4.02": {"label": "Non-Reliance on Previously Issued Financial Statements", "category": "distress"},
    "5.01": {"label": "Changes in Control of Registrant", "category": "m_and_a"},
    "5.02": {"label": "Departure or Election of Directors / Officers", "category": "exec_change"},
    "5.03": {"label": "Amendments to Articles of Incorporation or Bylaws", "category": "governance"},
    "5.04": {"label": "Temporary Suspension of Trading Under Employee Benefit Plans", "category": "governance"},
    "5.05": {"label": "Amendments to the Registrant's Code of Ethics", "category": "governance"},
    "5.06": {"label": "Change in Shell Company Status", "category": "governance"},
    "5.07": {"label": "Submission of Matters to a Vote of Security Holders", "category": "governance"},
    "5.08": {"label": "Shareholder Director Nominations", "category": "governance"},
    "6.01": {"label": "ABS Informational and Computational Material", "category": "regulatory"},
    "6.02": {"label": "Change of Servicer or Trustee", "category": "regulatory"},
    "6.03": {"label": "Change in Credit Enhancement or Other External Support", "category": "regulatory"},
    "6.04": {"label": "Failure to Make a Required Distribution", "category": "distress"},
    "6.05": {"label": "Securities Act Updating Disclosure", "category": "regulatory"},
    "7.01": {"label": "Regulation FD Disclosure", "category": "disclosure"},
    "8.01": {"label": "Other Events", "category": "other"},
    "9.01": {"label": "Financial Statements and Exhibits", "category": "exhibits"},
}

# High-signal categories worth surfacing prominently in dashboards.
HIGH_SIGNAL_CATEGORIES = {
    "exec_change",
    "financial_results",
    "m_and_a",
    "material_agreement",
    "distress",
    "restructuring",
}


class Form8KProcessor:
    """Parses 8-K item-code strings (e.g. "5.02,9.01") into structured event records."""

    def __init__(
        self,
        fetch_filings_func: Callable[..., Awaitable[List[Dict]]],
    ):
        """
        Args:
            fetch_filings_func: Returns 8-K filing metadata for a ticker. Each filing
                must include an 'items' key (comma-separated codes from submissions JSON).
                Typically `SECDataFetcher.fetch_current_reports`.
        """
        self.fetch_filings_metadata = fetch_filings_func

    @staticmethod
    def parse_items_string(raw: Optional[str]) -> List[Dict[str, str]]:
        """Split a comma-separated item string into structured records.

        Unknown codes are kept (label='Unknown item', category='unknown') so callers
        can still see them. Whitespace and empty segments are dropped.
        """
        if not raw:
            return []
        parsed: List[Dict[str, str]] = []
        seen: set[str] = set()
        for raw_code in str(raw).split(","):
            code = raw_code.strip()
            if not code or code in seen:
                continue
            seen.add(code)
            descriptor = ITEM_CODE_MAP.get(code)
            if descriptor is None:
                parsed.append({"code": code, "label": "Unknown item", "category": "unknown"})
            else:
                parsed.append({"code": code, "label": descriptor["label"], "category": descriptor["category"]})
        return parsed

    @classmethod
    def has_high_signal(cls, items: List[Dict[str, str]]) -> bool:
        return any(item.get("category") in HIGH_SIGNAL_CATEGORIES for item in items)

    async def get_8k_events(
        self,
        ticker: str,
        days_back: int = 180,
        use_cache: bool = True,
        filing_limit: int = 50,
        categories: Optional[List[str]] = None,
    ) -> Dict:
        """Build a payload of 8-K events for a ticker.

        Args:
            ticker: Issuer ticker symbol.
            days_back: Filings within this rolling window (days).
            use_cache: Pass through to the underlying filings fetch.
            filing_limit: Max number of filings to include.
            categories: Optional category whitelist (e.g. ['exec_change', 'financial_results']).
                If provided, events with no matching item are excluded.

        Returns:
            {
                'symbol': 'AAPL',
                'window': {'days_back': N, 'filing_limit': M},
                'as_of': ISO timestamp,
                'events': [<event>, ...],   # newest first
                'totals': {
                    'event_count': int,
                    'high_signal_count': int,
                    'category_counts': {category: count, ...},
                },
            }
        """
        ticker = ticker.upper()
        filings_meta = await self.fetch_filings_metadata(ticker, days_back=days_back, use_cache=use_cache)
        if filing_limit > 0:
            filings_meta = filings_meta[:filing_limit]

        category_filter = set(categories) if categories else None

        events: List[Dict] = []
        category_counts: Dict[str, int] = {}
        high_signal_count = 0

        for filing_meta in filings_meta:
            items = self.parse_items_string(filing_meta.get("items"))
            if category_filter is not None:
                items = [item for item in items if item["category"] in category_filter]
                if not items:
                    continue

            high_signal = self.has_high_signal(items)
            if high_signal:
                high_signal_count += 1
            for item in items:
                category_counts[item["category"]] = category_counts.get(item["category"], 0) + 1

            events.append(
                {
                    "accession_no": filing_meta.get("accession_no"),
                    "filing_date": filing_meta.get("filing_date"),
                    "report_date": filing_meta.get("report_date"),
                    "form": filing_meta.get("form"),
                    "url": filing_meta.get("url"),
                    "primary_document": filing_meta.get("primary_document"),
                    "items": items,
                    "item_count": len(items),
                    "high_signal": high_signal,
                }
            )

        events.sort(
            key=lambda event: event.get("filing_date") or "",
            reverse=True,
        )

        if not events:
            logging.debug("No 8-K events found for %s in last %d days.", ticker, days_back)

        return {
            "symbol": ticker,
            "window": {"days_back": days_back, "filing_limit": filing_limit},
            "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "events": events,
            "totals": {
                "event_count": len(events),
                "high_signal_count": high_signal_count,
                "category_counts": category_counts,
            },
        }
