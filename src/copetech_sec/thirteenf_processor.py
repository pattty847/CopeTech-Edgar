import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .cache_manager import SecCacheManager
from .document_handler import FilingDocumentHandler
from .http_client import SecHttpClient


THIRTEENF_FORMS = {"13F-HR", "13F-HR/A"}
SIG_CIK = "0001446194"


def normalize_cik(cik: str) -> str:
    cleaned = cik.strip().lstrip("0") or "0"
    if not cleaned.isdigit() or len(cleaned) > 10:
        raise ValueError("CIK must be 1-10 digits.")
    return cleaned.zfill(10)


def _strip_namespace(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned or None


def _to_int(value: Optional[str]) -> Optional[int]:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    try:
        return int(float(cleaned.replace(",", "")))
    except ValueError:
        return None


class ThirteenFProcessor:
    """Fetches and parses Form 13F-HR information tables for one institutional manager."""

    SUBMISSIONS_ENDPOINT = "https://data.sec.gov/submissions/CIK{cik}.json"

    def __init__(
        self,
        http_client: SecHttpClient,
        cache_manager: SecCacheManager,
        document_handler: FilingDocumentHandler,
    ):
        self.http_client = http_client
        self.cache_manager = cache_manager
        self.document_handler = document_handler

    async def get_13f_filings(
        self,
        cik: str,
        days_back: int = 365 * 3,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        normalized_cik = normalize_cik(cik)
        cache_key = f"CIK{normalized_cik}"
        submissions = None
        if use_cache:
            submissions = await self.cache_manager.load_data(cache_key, "submissions")

        if not isinstance(submissions, dict):
            submissions_url = self.SUBMISSIONS_ENDPOINT.format(cik=normalized_cik)
            submissions = await self.http_client.make_request(submissions_url, is_json=True)
            if not isinstance(submissions, dict):
                logging.warning("No SEC submissions payload returned for CIK %s.", normalized_cik)
                return []
            await self.cache_manager.save_data(cache_key, "submissions", submissions)

        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        recent = submissions.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        accession_numbers = recent.get("accessionNumber", [])
        report_dates = recent.get("reportDate", [])
        primary_documents = recent.get("primaryDocument", [])
        primary_descriptions = recent.get("primaryDocDescription", [])
        min_len = min(len(forms), len(filing_dates), len(accession_numbers), len(report_dates))

        filings: List[Dict[str, Any]] = []
        for index in range(min_len):
            form = forms[index]
            filing_date = filing_dates[index]
            if form not in THIRTEENF_FORMS or filing_date < cutoff_date:
                continue

            accession_no = accession_numbers[index]
            accession_clean = accession_no.replace("-", "")
            primary_document = primary_documents[index] if index < len(primary_documents) else None
            primary_description = primary_descriptions[index] if index < len(primary_descriptions) else None
            filings.append(
                {
                    "accession_no": accession_no,
                    "filing_date": filing_date,
                    "form": form,
                    "report_date": report_dates[index],
                    "url": f"https://www.sec.gov/Archives/edgar/data/{normalized_cik.lstrip('0')}/{accession_clean}/",
                    "primary_document": primary_document,
                    "primary_document_description": primary_description,
                }
            )

        return filings

    async def get_latest_13f_holdings(
        self,
        cik: str,
        days_back: int = 365 * 3,
        use_cache: bool = True,
        row_limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_cik = normalize_cik(cik)
        filings = await self.get_13f_filings(normalized_cik, days_back=days_back, use_cache=use_cache)
        if not filings:
            return {
                "manager_cik": normalized_cik,
                "manager_name": None,
                "filing": None,
                "information_table_document": None,
                "holdings_count": 0,
                "total_value": 0,
                "holdings": [],
            }

        latest = filings[0]
        documents = await self.document_handler.get_filing_documents_list(
            latest["accession_no"], cik=normalized_cik
        )
        information_table_document = self.choose_information_table_document(documents or [])
        if not information_table_document:
            raise ValueError(f"No 13F information table XML found for {latest['accession_no']}.")

        raw_xml = await self.document_handler.download_form_document(
            latest["accession_no"],
            information_table_document,
            cik=normalized_cik,
        )
        if not raw_xml:
            raise ValueError(f"Could not download 13F information table {information_table_document}.")

        holdings = self.parse_information_table_xml(raw_xml)
        limited_holdings = holdings[:row_limit] if row_limit else holdings
        total_value = sum(row.get("value") or 0 for row in holdings)

        return {
            "manager_cik": normalized_cik,
            "manager_name": await self._get_manager_name(normalized_cik, use_cache=use_cache),
            "filing": latest,
            "information_table_document": information_table_document,
            "holdings_count": len(holdings),
            "total_value": total_value,
            "holdings": limited_holdings,
        }

    async def _get_manager_name(self, cik: str, use_cache: bool = True) -> Optional[str]:
        cache_key = f"CIK{cik}"
        submissions = await self.cache_manager.load_data(cache_key, "submissions") if use_cache else None
        if not isinstance(submissions, dict):
            submissions = await self.http_client.make_request(
                self.SUBMISSIONS_ENDPOINT.format(cik=cik),
                is_json=True,
            )
        if isinstance(submissions, dict):
            return submissions.get("name")
        return None

    @staticmethod
    def choose_information_table_document(documents: List[Dict[str, Any]]) -> Optional[str]:
        xml_documents = [
            document for document in documents
            if str(document.get("name", "")).lower().endswith(".xml")
        ]
        if not xml_documents:
            return None

        def score(document: Dict[str, Any]) -> int:
            name = str(document.get("name", "")).lower()
            doc_type = str(document.get("type", "")).lower()
            value = 0
            if "information" in doc_type and "table" in doc_type:
                value += 100
            if "infotable" in name or "informationtable" in name or "form13f" in name:
                value += 50
            if "xsl" in name or "primary" in name:
                value -= 20
            return value

        chosen = max(xml_documents, key=score)
        return chosen.get("name")

    @staticmethod
    def parse_information_table_xml(xml_content: str) -> List[Dict[str, Any]]:
        root = ThirteenFProcessor._parse_xml_root(xml_content)
        info_tables = [
            element for element in root.iter()
            if _strip_namespace(element.tag) == "infoTable"
        ]

        holdings: List[Dict[str, Any]] = []
        for info_table in info_tables:
            value_thousands = _to_int(ThirteenFProcessor._child_text(info_table, "value"))
            holding = {
                "issuer": _clean_text(ThirteenFProcessor._child_text(info_table, "nameOfIssuer")),
                "title_of_class": _clean_text(ThirteenFProcessor._child_text(info_table, "titleOfClass")),
                "cusip": _clean_text(ThirteenFProcessor._child_text(info_table, "cusip")),
                "value_thousands": value_thousands,
                "value": value_thousands * 1000 if value_thousands is not None else None,
                "shares": _to_int(ThirteenFProcessor._nested_child_text(info_table, "shrsOrPrnAmt", "sshPrnamt")),
                "share_type": _clean_text(ThirteenFProcessor._nested_child_text(info_table, "shrsOrPrnAmt", "sshPrnamtType")),
                "put_call": _clean_text(ThirteenFProcessor._child_text(info_table, "putCall")),
                "discretion": _clean_text(ThirteenFProcessor._child_text(info_table, "investmentDiscretion")),
                "voting_authority": {
                    "sole": _to_int(ThirteenFProcessor._nested_child_text(info_table, "votingAuthority", "Sole")),
                    "shared": _to_int(ThirteenFProcessor._nested_child_text(info_table, "votingAuthority", "Shared")),
                    "none": _to_int(ThirteenFProcessor._nested_child_text(info_table, "votingAuthority", "None")),
                },
            }
            holdings.append(holding)

        return holdings

    @staticmethod
    def _holding_key(row: Dict[str, Any]) -> tuple:
        return (
            str(row.get("cusip") or "").upper(),
            str(row.get("put_call") or "").upper(),
            str(row.get("title_of_class") or "").upper(),
        )

    @staticmethod
    def compute_quarter_changes(
        prior_holdings: List[Dict[str, Any]],
        current_holdings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Compute QoQ deltas between two parsed 13F-HR information tables.

        Both inputs are lists shaped like `parse_information_table_xml` output:
          {issuer, title_of_class, cusip, value, value_thousands, shares, put_call, ...}

        Returns a dict with categorized change lists (each ranked by abs value change)
        plus aggregate stats:
          {
            'new_positions': [...],      # in current, not in prior
            'increased':     [...],      # in both, current value > prior value
            'reduced':       [...],      # in both, current value < prior value
            'sold_out':      [...],      # in prior, not in current
            'unchanged_count': int,
            'totals': {
              'prior_value': int, 'current_value': int, 'value_change': int,
              'turnover_pct': float | None,  # gross changed / prior_total
              'top10_concentration': float | None,
            },
          }
        """
        prior_by_key = {ThirteenFProcessor._holding_key(row): row for row in prior_holdings}
        current_by_key = {ThirteenFProcessor._holding_key(row): row for row in current_holdings}

        new_positions: List[Dict[str, Any]] = []
        increased: List[Dict[str, Any]] = []
        reduced: List[Dict[str, Any]] = []
        sold_out: List[Dict[str, Any]] = []
        unchanged_count = 0

        for key in set(prior_by_key) | set(current_by_key):
            previous = prior_by_key.get(key)
            current = current_by_key.get(key)
            previous_value = int(previous.get("value") or 0) if previous else 0
            current_value = int(current.get("value") or 0) if current else 0
            previous_shares = int(previous.get("shares") or 0) if previous else 0
            current_shares = int(current.get("shares") or 0) if current else 0
            source = current or previous or {}

            entry = {
                "cusip": key[0],
                "put_call": key[1] or None,
                "issuer": source.get("issuer"),
                "title_of_class": source.get("title_of_class"),
                "prior_value": previous_value,
                "current_value": current_value,
                "value_change": current_value - previous_value,
                "prior_shares": previous_shares,
                "current_shares": current_shares,
                "share_change": current_shares - previous_shares,
            }

            if previous is None:
                new_positions.append(entry)
            elif current is None:
                sold_out.append(entry)
            elif current_value > previous_value:
                increased.append(entry)
            elif current_value < previous_value:
                reduced.append(entry)
            else:
                unchanged_count += 1

        for bucket in (new_positions, increased, reduced, sold_out):
            bucket.sort(key=lambda row: abs(row["value_change"]), reverse=True)

        prior_total = sum(int(row.get("value") or 0) for row in prior_holdings)
        current_total = sum(int(row.get("value") or 0) for row in current_holdings)
        gross_change = (
            sum(row["value_change"] for row in increased)
            + sum(abs(row["value_change"]) for row in reduced)
            + sum(row["current_value"] for row in new_positions)
            + sum(row["prior_value"] for row in sold_out)
        )
        sorted_current = sorted(
            (int(row.get("value") or 0) for row in current_holdings), reverse=True
        )
        top10_concentration = (
            sum(sorted_current[:10]) / current_total if current_total else None
        )

        return {
            "new_positions": new_positions,
            "increased": increased,
            "reduced": reduced,
            "sold_out": sold_out,
            "unchanged_count": unchanged_count,
            "totals": {
                "prior_value": prior_total,
                "current_value": current_total,
                "value_change": current_total - prior_total,
                "turnover_pct": (gross_change / prior_total) if prior_total else None,
                "top10_concentration": top10_concentration,
            },
        }

    async def get_holdings_changes(
        self,
        cik: str,
        days_back: int = 365 * 3,
        use_cache: bool = True,
        top_n: int = 25,
    ) -> Dict[str, Any]:
        """Fetch the latest two 13F-HR filings and compute QoQ deltas.

        Returns a payload with each change bucket truncated to `top_n`. If only one
        filing is available, all current holdings are returned as new positions.
        """
        normalized_cik = normalize_cik(cik)
        filings = await self.get_13f_filings(normalized_cik, days_back=days_back, use_cache=use_cache)
        if not filings:
            return {
                "manager_cik": normalized_cik,
                "manager_name": await self._get_manager_name(normalized_cik, use_cache=use_cache),
                "current_filing": None,
                "prior_filing": None,
                "changes": None,
            }

        latest = filings[0]
        prior = filings[1] if len(filings) > 1 else None

        latest_holdings = await self._fetch_holdings_for_filing(latest, cik=normalized_cik)
        prior_holdings = (
            await self._fetch_holdings_for_filing(prior, cik=normalized_cik) if prior else []
        )

        diff = self.compute_quarter_changes(prior_holdings, latest_holdings)
        for bucket_key in ("new_positions", "increased", "reduced", "sold_out"):
            diff[bucket_key] = diff[bucket_key][:top_n]

        return {
            "manager_cik": normalized_cik,
            "manager_name": await self._get_manager_name(normalized_cik, use_cache=use_cache),
            "current_filing": latest,
            "prior_filing": prior,
            "changes": diff,
        }

    async def _fetch_holdings_for_filing(
        self, filing: Dict[str, Any], cik: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        documents = await self.document_handler.get_filing_documents_list(
            filing["accession_no"], cik=cik
        )
        information_table_document = self.choose_information_table_document(documents or [])
        if not information_table_document:
            logging.warning(
                "No 13F information table XML found for %s.", filing["accession_no"]
            )
            return []
        raw_xml = await self.document_handler.download_form_document(
            filing["accession_no"], information_table_document, cik=cik
        )
        if not raw_xml:
            logging.warning(
                "Could not download 13F information table %s.", information_table_document
            )
            return []
        return self.parse_information_table_xml(raw_xml)

    @staticmethod
    def _parse_xml_root(xml_content: str) -> ET.Element:
        try:
            return ET.fromstring(xml_content)
        except ET.ParseError:
            match = re.search(r"(<informationTable[\s\S]*?</informationTable>)", xml_content)
            if not match:
                raise
            return ET.fromstring(match.group(1))

    @staticmethod
    def _child_text(element: ET.Element, child_name: str) -> Optional[str]:
        for child in list(element):
            if _strip_namespace(child.tag) == child_name:
                return child.text
        return None

    @staticmethod
    def _nested_child_text(element: ET.Element, parent_name: str, child_name: str) -> Optional[str]:
        for child in list(element):
            if _strip_namespace(child.tag) != parent_name:
                continue
            return ThirteenFProcessor._child_text(child, child_name)
        return None
