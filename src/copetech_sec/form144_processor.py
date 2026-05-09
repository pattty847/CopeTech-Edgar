"""Form 144 (Notice of Proposed Sale of Securities) parser.

Form 144 is the *intent* to sell restricted/control securities — the leading
indicator that pairs with Form 4's *executed* sale. This processor mirrors
`Form4Processor` in shape: parse XML into normalized records, fetch + parse a
batch of recent filings for a ticker, expose a simple list payload.

Each record represents a single planned-sale block (a Form 144 may declare
multiple security classes; each becomes its own record).
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Optional

from .document_handler import FilingDocumentHandler


def _strip_namespace(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _find_first(root: ET.Element, local_name: str) -> Optional[ET.Element]:
    for element in root.iter():
        if _strip_namespace(element.tag) == local_name:
            return element
    return None


def _find_all(root: ET.Element, local_name: str) -> List[ET.Element]:
    return [element for element in root.iter() if _strip_namespace(element.tag) == local_name]


def _child_text(element: Optional[ET.Element], local_name: str) -> Optional[str]:
    if element is None:
        return None
    for child in element.iter():
        if _strip_namespace(child.tag) == local_name:
            text = (child.text or "").strip()
            return text or None
    return None


def _to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    cleaned = value.replace(",", "").replace("$", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _to_int(value: Optional[str]) -> Optional[int]:
    parsed = _to_float(value)
    return int(parsed) if parsed is not None else None


class Form144Processor:
    """Parses Form 144 XML filings into structured planned-sale records."""

    def __init__(
        self,
        document_handler: FilingDocumentHandler,
        fetch_filings_func: Callable[..., Awaitable[List[Dict]]],
    ):
        """
        Args:
            document_handler: For downloading the form XML.
            fetch_filings_func: Returns Form 144 filing metadata for a ticker.
                Typically `SECDataFetcher.fetch_planned_sale_filings`.
        """
        self.document_handler = document_handler
        self.fetch_filings_metadata = fetch_filings_func

    def parse_form144_xml(self, xml_content: str) -> List[Dict]:
        """Parse a Form 144 XML payload into a list of planned-sale records.

        Form 144 may declare multiple `secsToBeSold` blocks (one per security class).
        Each block becomes its own record so callers can aggregate at any granularity.

        Returns an empty list if the XML cannot be parsed.
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as exc:
            logging.error("Form 144 XML parse error: %s", exc)
            return []

        issuer_info = _find_first(root, "issuerInfo")
        issuer_cik = _child_text(issuer_info, "issuerCik")
        issuer_name = _child_text(issuer_info, "issuerName")
        issuer_symbol = _child_text(issuer_info, "issuerTradingSymbol")
        account_name = _child_text(issuer_info, "nameOfPersonForWhoseAccount")

        signature_block = _find_first(root, "signature")
        signature_date = _child_text(signature_block, "signatureDate")
        signer = None
        if signature_block is not None:
            for child in list(signature_block):
                if _strip_namespace(child.tag) == "signature":
                    signer = (child.text or "").strip() or None
                    break

        relationship = self._extract_relationship(root)
        recent_sales = self._extract_recent_sales(root)

        records: List[Dict] = []
        for block in _find_all(root, "secsToBeSold"):
            shares = _to_int(_child_text(block, "noOfUnitsSold"))
            aggregate_value = _to_float(_child_text(block, "aggregateMarketValue"))
            shares_outstanding = _to_int(_child_text(block, "noOfUnitsOutstanding"))
            implied_price = (
                aggregate_value / shares if (shares and aggregate_value and shares > 0) else None
            )
            records.append(
                {
                    "issuer_cik": issuer_cik,
                    "issuer_name": issuer_name,
                    "issuer_symbol": issuer_symbol,
                    "account_name": account_name,
                    "signer": signer,
                    "signature_date": signature_date,
                    "relationship": relationship,
                    "security_class": _child_text(block, "securitiesClassTitleOfIssuer"),
                    "broker_name": _child_text(block, "brokerName"),
                    "planned_shares": shares,
                    "aggregate_market_value": aggregate_value,
                    "implied_price_per_share": (
                        round(implied_price, 4) if implied_price is not None else None
                    ),
                    "shares_outstanding": shares_outstanding,
                    "approx_sale_date": _child_text(block, "approxSaleDate"),
                    "exchange": _child_text(block, "natOfSecuritiesExchange"),
                    "acquired_from_issuer": self._yes_no(_child_text(block, "acquiredFromIssuer")),
                    "recent_sales": recent_sales,
                }
            )

        if not records:
            logging.debug("Form 144 XML had no secsToBeSold blocks.")
        return records

    @staticmethod
    def _extract_relationship(root: ET.Element) -> Optional[str]:
        relationship_block = _find_first(root, "relationshipWithIssuer")
        if relationship_block is None:
            return None
        flags: List[str] = []
        if _child_text(relationship_block, "isOfficer") in {"1", "true", "Y"}:
            officer_title = _child_text(relationship_block, "officerTitle")
            flags.append(f"Officer ({officer_title})" if officer_title else "Officer")
        if _child_text(relationship_block, "isDirector") in {"1", "true", "Y"}:
            flags.append("Director")
        if _child_text(relationship_block, "isTenPercentOwner") in {"1", "true", "Y"}:
            flags.append("10% Owner")
        if _child_text(relationship_block, "isOther") in {"1", "true", "Y"}:
            other_desc = _child_text(relationship_block, "natureOfRelationship")
            flags.append(f"Other ({other_desc})" if other_desc else "Other")
        return ", ".join(flags) or None

    @staticmethod
    def _extract_recent_sales(root: ET.Element) -> List[Dict]:
        sales: List[Dict] = []
        for block in _find_all(root, "securitiesSoldInPast3Months"):
            sales.append(
                {
                    "sale_date": _child_text(block, "saleDate"),
                    "shares_sold": _to_int(_child_text(block, "amountOfSecuritiesSold")),
                    "gross_proceeds": _to_float(_child_text(block, "grossProceeds")),
                }
            )
        return sales

    @staticmethod
    def _yes_no(value: Optional[str]) -> Optional[bool]:
        if value is None:
            return None
        cleaned = value.strip().lower()
        if cleaned in {"1", "true", "y", "yes"}:
            return True
        if cleaned in {"0", "false", "n", "no"}:
            return False
        return None

    async def process_form144_filing(self, accession_no: str, ticker: Optional[str] = None) -> List[Dict]:
        """Download and parse a single Form 144 filing."""
        xml_content = await self.document_handler.download_form_xml(accession_no, ticker=ticker)
        if not xml_content:
            logging.warning("Could not download Form 144 XML for %s (ticker: %s)", accession_no, ticker)
            return []
        return self.parse_form144_xml(xml_content)

    async def get_planned_insider_sales(
        self,
        ticker: str,
        days_back: int = 90,
        use_cache: bool = True,
        filing_limit: int = 25,
    ) -> Dict:
        """Build a payload of recent planned-sale records for a ticker.

        Returns:
            {
                'symbol': 'AAPL',
                'window': {'days_back': N, 'filing_limit': M},
                'as_of': ISO timestamp,
                'records': [<record>, ...],   # newest first by signature_date
                'totals': {
                    'record_count': int,
                    'planned_shares': int,
                    'aggregate_market_value': float,
                    'unique_filers': int,
                },
            }
        """
        ticker = ticker.upper()
        filings_meta = await self.fetch_filings_metadata(ticker, days_back=days_back, use_cache=use_cache)
        if filing_limit > 0:
            filings_meta = filings_meta[:filing_limit]

        records: List[Dict] = []
        for filing_meta in filings_meta:
            accession_no = filing_meta.get("accession_no")
            if not accession_no:
                continue
            parsed = await self.process_form144_filing(accession_no, ticker=ticker)
            for record in parsed:
                record["accession_no"] = accession_no
                record["filing_date"] = filing_meta.get("filing_date")
                record["form_url"] = filing_meta.get("url")
                records.append(record)

        records.sort(
            key=lambda record: (record.get("signature_date") or record.get("filing_date") or ""),
            reverse=True,
        )

        totals = {
            "record_count": len(records),
            "planned_shares": sum(int(record.get("planned_shares") or 0) for record in records),
            "aggregate_market_value": round(
                sum(float(record.get("aggregate_market_value") or 0.0) for record in records), 2
            ),
            "unique_filers": len(
                {record.get("account_name") or record.get("signer") for record in records if record.get("account_name") or record.get("signer")}
            ),
        }

        return {
            "symbol": ticker,
            "window": {"days_back": days_back, "filing_limit": filing_limit},
            "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "records": records,
            "totals": totals,
        }
