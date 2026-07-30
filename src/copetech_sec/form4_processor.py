import hashlib
import logging
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, TYPE_CHECKING, Callable, Awaitable, Any

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional dependency in lightweight environments
    pd = None

# Import FilingDocumentHandler for dependency injection
from .document_handler import FilingDocumentHandler, RawFilingResolver

# Parser/payload version, part of the payload fingerprint. Derived payloads are valid
# only while (parser version, source accession set, window config) all match — bump this
# when the parser or payload shape changes and payloads re-derive from the local raw
# filing store (zero SEC traffic).
SIGNAL_PAYLOAD_VERSION = 2


def _parse_decimal(raw: Optional[str]) -> Optional[float]:
    """Parse a numeric value out of an ownership-form `<value>` element.

    The previous guard was `raw.replace('.', '', 1).isdigit()`, which silently returned 0.0
    for anything that was not a bare unsigned integer/decimal: values surrounded by
    whitespace (pretty-printed XML from some filer agents), negative values, thousands
    separators, and scientific notation all became zero shares at zero dollars. A parse
    failure must be distinguishable from a real zero, so this returns None.
    """
    if raw is None:
        return None
    cleaned = str(raw).strip().replace(",", "").replace("$", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        logging.debug(f"Non-numeric ownership value {raw!r}; treating as missing.")
        return None


def _element_text(node, path: str) -> Optional[str]:
    """`findtext` for an ownership `<x><value>…</value></x>` wrapper, whitespace-stripped."""
    if node is None:
        return None
    text = node.findtext(path)
    if text is None:
        return None
    stripped = text.strip()
    return stripped or None


def _is_flag_set(node, tag: str) -> bool:
    """Ownership booleans appear as 1/0 and occasionally true/false or Y/N."""
    if node is None:
        return False
    raw = node.findtext(tag)
    if raw is None:
        # Some filers wrap the flag in the <value> pattern used elsewhere in the schema.
        raw = node.findtext(f"{tag}/value")
    return (raw or "").strip().lower() in {"1", "true", "y", "yes"}


def _payload_fingerprint(accessions: List[str], days_back: int, filing_limit: int, anchor_type: str) -> str:
    """Deterministic identity of a derived payload's inputs. If this matches, the cached
    payload is byte-for-byte what a rebuild would produce — regardless of its age."""
    material = "|".join(
        [str(SIGNAL_PAYLOAD_VERSION), f"{int(days_back)}d", str(int(filing_limit)), str(anchor_type)]
        + sorted(str(a) for a in accessions)
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()

# Use TYPE_CHECKING block to avoid circular imports at runtime
# SECDataFetcher is needed for fetching filing metadata (get_filings_by_form)
if TYPE_CHECKING:
    from .sec_api import SECDataFetcher # type: ignore

class Form4Processor:
    """
    Handles the specialized processing, parsing, and analysis of SEC Form 4 filings (Insider Transactions).

    This class encapsulates all logic related to Form 4 data. Its core responsibilities include:
    - Parsing the XML structure of Form 4 filings to extract transaction details
      for both non-derivative and derivative securities.
    - Mapping transaction codes (e.g., 'P', 'S') to human-readable descriptions (e.g., 'Purchase', 'Sale').
    - Classifying transactions as acquisitions or dispositions based on codes.
    - Orchestrating the retrieval of recent Form 4 filing metadata (via an injected function).
    - Downloading the corresponding Form 4 XML documents (using the injected `FilingDocumentHandler`).
    - Processing multiple recent filings to compile a list of transactions formatted for display or analysis.
    - Performing basic quantitative analysis on the compiled transactions (e.g., buy/sell counts, net value).

    It depends on an injected `FilingDocumentHandler` for XML downloads and a function
    (usually from `SECDataFetcher`) to retrieve the list of recent Form 4 filing accession numbers.
    """

    # Transaction codes per the SEC Form 4 instructions (Table I/II "Transaction Code"
    # column). Several codes are direction-neutral — the filing's own
    # transactionAcquiredDisposedCode is the authority on direction, not this table.
    TRANSACTION_CODE_MAP = {
        # General transactions
        'P': 'Open market or private purchase',
        'S': 'Open market or private sale',
        'V': 'Voluntary early report',
        # Rule 16b-3 transactions
        'A': 'Grant, award or other acquisition',
        'D': 'Disposition to the issuer',
        'F': 'Payment of exercise price or tax by delivering/withholding securities',
        'I': 'Discretionary transaction',
        'M': 'Exercise or conversion of derivative security exempt under Rule 16b-3',
        # Derivative securities transactions
        'C': 'Conversion of derivative security',
        'E': 'Expiration of short derivative position',
        'H': 'Expiration (or cancellation) of long derivative position',
        'O': 'Exercise of out-of-the-money derivative security',
        'X': 'Exercise of in-the-money or at-the-money derivative security',
        # Other transactions
        'G': 'Bona fide gift',
        'J': 'Other acquisition or disposition',
        'K': 'Transaction in equity swap or similar instrument',
        'L': 'Small acquisition under Rule 16a-6',
        'U': 'Disposition pursuant to a tender of shares',
        'W': 'Acquisition or disposition by will or the laws of descent and distribution',
        'Z': 'Deposit into or withdrawal from voting trust',
    }

    # Retained only as a last-resort hint for filings that omit
    # transactionAcquiredDisposedCode. Direction is a per-transaction fact, not a property
    # of the code: 'M' is an acquisition on the non-derivative leg and a disposition on the
    # derivative leg of the same exercise, and G/J/W/K/Z/I run either way.
    ACQUISITION_CODE_HINTS = frozenset({'P', 'A', 'L'})
    DISPOSITION_CODE_HINTS = frozenset({'S', 'D', 'F', 'U', 'E', 'H'})
    # Deprecated aliases; kept so existing consumers importing them do not break.
    ACQUISITION_CODES = sorted(ACQUISITION_CODE_HINTS)
    DISPOSITION_CODES = sorted(DISPOSITION_CODE_HINTS)
    SIGNAL_CLASS_MAP = {
        'P': 'open_market_buy',
        'S': 'open_market_sell',
        'F': 'tax_sale',
        'M': 'option_exercise',
        'A': 'award_or_grant',
        'G': 'gift',
        'D': 'gift',
        'C': 'derivative_conversion',
        'W': 'derivative_conversion',
    }
    ECONOMIC_INTENT_MAP = {
        'open_market_buy': 'bullish',
        'open_market_sell': 'bearish',
        'tax_sale': 'neutral',
        'option_exercise': 'neutral',
        'award_or_grant': 'compensation',
        'gift': 'neutral',
        'derivative_conversion': 'neutral',
        'planned_sale_10b5_1': 'bearish',
        'holding': 'neutral',
        'other': 'neutral',
    }

    def __init__(self, 
                 document_handler: FilingDocumentHandler, 
                 fetch_filings_func: Callable[..., Awaitable[List[Dict]]],
                 cache_manager: Any | None = None):
        """
        Initializes the Form 4 Processor.

        Args:
            document_handler (FilingDocumentHandler): An instance of the
                `FilingDocumentHandler` used specifically for downloading the
                XML content of Form 4 filings.
            fetch_filings_func (Callable[..., Awaitable[List[Dict]]]): An awaitable
                function that retrieves the metadata for recent Form 4 filings
                (e.g., accession number, filing date) for a given ticker.
                This is typically bound to `SECDataFetcher.fetch_insider_filings`.
        """
        self.document_handler = document_handler
        self.fetch_filings_metadata = fetch_filings_func # e.g., SECDataFetcher.fetch_insider_filings
        self.cache_manager = cache_manager
        self.raw_filings = RawFilingResolver(document_handler, cache_manager)

    def parse_form4_xml(self, xml_content: str) -> List[Dict]:
        """
        Parses the XML content of a single SEC Form 4 filing into structured transaction data.

        Uses `xml.etree.ElementTree` to navigate the standard Form 4 XML structure.
        Extracts details for both `nonDerivativeTransaction` and `derivativeTransaction` elements.
        Handles potential missing fields gracefully and attempts basic type conversion (e.g., float for shares/price).
        Adds derived fields like 'transaction_type', 'is_acquisition', 'is_disposition', and calculated 'value'.

        Args:
            xml_content (str): A string containing the complete XML content of a Form 4 filing.

        Returns:
            List[Dict]: A list of dictionaries, where each dictionary represents a single
                transaction (either non-derivative or derivative) parsed from the form.
                Returns an empty list if the XML is malformed or cannot be parsed.
                Keys in the dictionary include 'ticker', 'owner_name', 'transaction_date',
                'transaction_code', 'transaction_type', 'shares', 'price_per_share',
                'value', 'is_derivative', 'is_acquisition', 'is_disposition', etc.
        """
        transactions: List[Dict] = []
        try:
            root = ET.fromstring(xml_content)

            issuer = root.find('.//issuer')
            issuer_cik = _element_text(issuer, 'issuerCik') or 'N/A'
            issuer_name = _element_text(issuer, 'issuerName') or 'N/A'
            issuer_symbol = _element_text(issuer, 'issuerTradingSymbol') or 'N/A'

            document_type = _element_text(root, 'documentType')
            period_of_report = _element_text(root, 'periodOfReport')
            footnotes = self._parse_footnotes(root)

            # Per the EDGAR Ownership XML Technical Specification (v5.5), `aff10b5One` is a
            # required *document-level* element on Forms 4 and 5 — a sibling of <issuer> and
            # <reportingOwner>, not a per-transaction field. It carries the Rule 10b5-1(c)
            # trading-plan checkbox. Filers emit either 1/0 or true/false.
            rule_10b5_1_plan = _is_flag_set(root, 'aff10b5One')
            remarks = _element_text(root, 'remarks')
            not_subject_to_section_16 = _is_flag_set(root, 'notSubjectToSection16')

            # A single ownership form may name up to 10 reporting owners (joint filings: a
            # person plus a family trust, co-filing 10% owners, an estate) — Ownership XML
            # Technical Specification, `reportingOwner` maxOccurs="10". The parser used to
            # read only the first via findtext('.//reportingOwner/...') and dropped the rest.
            #
            # The schema carries no per-row owner attribution: Table I/II rows describe the
            # reported transactions once, and every listed owner is a filer of that report.
            # So exactly one record is emitted per table row — never one per owner-row pair,
            # which would multiply reported shares and value by the owner count. `owner_*`
            # holds the primary owner (backward compatible); `reporting_owners` holds all.
            owners = self._parse_reporting_owners(root)
            primary_owner = owners[0]

            shared_fields = {
                'ticker': issuer_symbol,
                'issuer_cik': issuer_cik,
                'issuer_name': issuer_name,
                'document_type': document_type,
                'period_of_report': period_of_report,
                'reporting_owners': owners,
                'owner_count': len(owners),
                'is_joint_filing': len(owners) > 1,
                'rule_10b5_1_plan': rule_10b5_1_plan,
                'remarks': remarks,
                'not_subject_to_section_16': not_subject_to_section_16,
            }

            for table_path, is_derivative, is_holding in (
                ('.//nonDerivativeTransaction', False, False),
                ('.//nonDerivativeHolding', False, True),
                ('.//derivativeTransaction', True, False),
                ('.//derivativeHolding', True, True),
            ):
                for node in root.findall(table_path):
                    try:
                        transactions.append(
                            self._parse_ownership_row(
                                node,
                                owner=primary_owner,
                                shared_fields=shared_fields,
                                is_derivative=is_derivative,
                                is_holding=is_holding,
                                footnotes=footnotes,
                            )
                        )
                    except Exception as exc:
                        logging.warning(
                            f"Error parsing ownership row ({table_path}): {exc} - "
                            f"XML: {ET.tostring(node, encoding='unicode')[:200]}"
                        )

            return transactions

        except ET.ParseError as e:
            logging.error(f"XML Parse Error in Form 4: {e} - Content length: {len(xml_content)}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error parsing Form 4 XML: {e}", exc_info=True)
            return []

    @staticmethod
    def _parse_footnotes(root: ET.Element) -> Dict[str, str]:
        """id -> text for every <footnote>. Form 4 footnotes carry material qualifications
        ("no shares were sold", "sold under a Rule 10b5-1 plan adopted on ...") that are
        otherwise invisible to consumers."""
        notes: Dict[str, str] = {}
        for note in root.findall('.//footnotes/footnote'):
            note_id = (note.get('id') or '').strip()
            text = ' '.join((note.text or '').split())
            if note_id and text:
                notes[note_id] = text
        return notes

    @staticmethod
    def _row_footnote_ids(node: ET.Element) -> List[str]:
        return [
            ref.get('id').strip()
            for ref in node.iter('footnoteId')
            if (ref.get('id') or '').strip()
        ]

    def _parse_reporting_owners(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Every <reportingOwner> in the document, with its own relationship flags."""
        owners: List[Dict[str, Any]] = []
        for owner_node in root.findall('.//reportingOwner'):
            identity = owner_node.find('reportingOwnerId')
            relationship = owner_node.find('reportingOwnerRelationship')

            positions: List[str] = []
            officer_title = None
            if _is_flag_set(relationship, 'isDirector'):
                positions.append('Director')
            if _is_flag_set(relationship, 'isOfficer'):
                officer_title = _element_text(relationship, 'officerTitle')
                positions.append(f'Officer ({officer_title})' if officer_title else 'Officer')
            if _is_flag_set(relationship, 'isTenPercentOwner'):
                positions.append('10% Owner')
            if _is_flag_set(relationship, 'isOther'):
                other_text = _element_text(relationship, 'otherText')
                positions.append(f'Other ({other_text})' if other_text else 'Other')

            owners.append({
                'owner_cik': _element_text(identity, 'rptOwnerCik') or 'N/A',
                'owner_name': _element_text(identity, 'rptOwnerName') or 'N/A',
                'owner_position': ', '.join(positions) or 'N/A',
                'owner_is_director': _is_flag_set(relationship, 'isDirector'),
                'owner_is_officer': _is_flag_set(relationship, 'isOfficer'),
                'owner_is_ten_percent_owner': _is_flag_set(relationship, 'isTenPercentOwner'),
                'owner_officer_title': officer_title,
            })

        if not owners:
            logging.warning("Ownership document contained no <reportingOwner> block.")
            owners.append({
                'owner_cik': 'N/A',
                'owner_name': 'N/A',
                'owner_position': 'N/A',
                'owner_is_director': False,
                'owner_is_officer': False,
                'owner_is_ten_percent_owner': False,
                'owner_officer_title': None,
            })
        return owners

    def _parse_ownership_row(
        self,
        node: ET.Element,
        *,
        owner: Dict[str, Any],
        shared_fields: Dict[str, Any],
        is_derivative: bool,
        is_holding: bool,
        footnotes: Dict[str, str],
    ) -> Dict[str, Any]:
        """Normalize one Table I/II row (transaction or holding) into a flat record.

        Both row types produce the same key set so downstream consumers (and
        `pandas.DataFrame`) see one stable schema. Previously derivative rows lacked
        `price_per_share`/`value` while non-derivative rows lacked
        `conversion_exercise_price`, so any consumer indexing a column raised KeyError on
        half the rows and DataFrames came out ragged.
        """
        amounts = node.find('transactionAmounts')
        post = node.find('postTransactionAmounts')

        tx_code = _element_text(node, 'transactionCoding/transactionCode')
        acq_disp_code = _element_text(amounts, 'transactionAcquiredDisposedCode/value')

        shares = _parse_decimal(_element_text(amounts, 'transactionShares/value'))
        price = _parse_decimal(_element_text(amounts, 'transactionPricePerShare/value'))
        conversion_price = _parse_decimal(_element_text(node, 'conversionOrExercisePrice/value'))

        # Direction comes from the filing's own A/D code. Deriving it from the transaction
        # code alone mislabels every direction-neutral code: the derivative leg of an
        # option exercise (M, acquired/disposed = D) was reported as an acquisition, so both
        # legs of one exercise counted as acquisitions and net insider value was overstated.
        is_acquisition, is_disposition = self._resolve_direction(acq_disp_code, tx_code)

        # value is only meaningful when a real per-share price was reported. A missing price
        # (gifts, awards) is unknown, not zero — reporting 0.0 made a $10M grant look free.
        gross_value = shares * price if (shares is not None and price is not None) else None

        footnote_ids = self._row_footnote_ids(node)

        record: Dict[str, Any] = {
            **shared_fields,
            'owner_cik': owner['owner_cik'],
            'owner_name': owner['owner_name'],
            'owner_position': owner['owner_position'],
            'owner_is_director': owner['owner_is_director'],
            'owner_is_officer': owner['owner_is_officer'],
            'owner_is_ten_percent_owner': owner['owner_is_ten_percent_owner'],
            'owner_officer_title': owner['owner_officer_title'],
            'security_title': _element_text(node, 'securityTitle/value') or 'N/A',
            'transaction_date': _element_text(node, 'transactionDate/value') or 'N/A',
            'transaction_code': tx_code or 'N/A',
            'transaction_type': self.TRANSACTION_CODE_MAP.get(tx_code or '', 'Unknown'),
            'transaction_form_type': _element_text(node, 'transactionCoding/transactionFormType'),
            'equity_swap_involved': _is_flag_set(node.find('transactionCoding'), 'equitySwapInvolved'),
            'acq_disp_code': acq_disp_code or 'N/A',
            'is_acquisition': is_acquisition,
            'is_disposition': is_disposition,
            'shares': shares,
            'price_per_share': price,
            'value': gross_value,
            'conversion_exercise_price': conversion_price,
            'exercise_date': _element_text(node, 'exerciseDate/value'),
            'expiration_date': _element_text(node, 'expirationDate/value'),
            'underlying_title': _element_text(node, 'underlyingSecurity/underlyingSecurityTitle/value'),
            'underlying_shares': _parse_decimal(
                _element_text(node, 'underlyingSecurity/underlyingSecurityShares/value')
            ),
            'shares_owned_after': _parse_decimal(
                _element_text(post, 'sharesOwnedFollowingTransaction/value')
            ),
            'direct_indirect': _element_text(node, 'ownershipNature/directOrIndirectOwnership/value') or 'N/A',
            'indirect_ownership_nature': _element_text(node, 'ownershipNature/natureOfOwnership/value'),
            'is_derivative': is_derivative,
            # Table rows are either executed transactions or static holdings. Holdings rows
            # (all of Form 3, and holdings-only Form 4/5 rows) used to be dropped silently.
            'is_holding': is_holding,
            'footnote_ids': footnote_ids,
            'footnotes': [footnotes[fid] for fid in footnote_ids if fid in footnotes],
        }
        return record

    @classmethod
    def _resolve_direction(cls, acq_disp_code: Optional[str], tx_code: Optional[str]) -> tuple:
        """(is_acquisition, is_disposition) from the filing's A/D code, code table as fallback."""
        code = (acq_disp_code or '').strip().upper()
        if code == 'A':
            return True, False
        if code == 'D':
            return False, True
        hint = (tx_code or '').strip().upper()
        if hint in cls.ACQUISITION_CODE_HINTS:
            return True, False
        if hint in cls.DISPOSITION_CODE_HINTS:
            return False, True
        return False, False

    async def process_form4_filing(self, accession_no: str, ticker: str = None) -> List[Dict]:
        """
        Downloads and parses a single Form 4 XML filing identified by its accession number.

        This method orchestrates the two main steps for processing one filing:
        1. Calls `document_handler.download_form_xml` to get the XML content.
        2. Calls `parse_form4_xml` to parse the downloaded content.

        Args:
            accession_no (str): The accession number of the Form 4 filing to process.
            ticker (str, optional): The stock ticker symbol of the *issuer*. This is passed
                to the `download_form_xml` method as a hint for constructing the correct
                download URL. Defaults to None.

        Returns:
            List[Dict]: A list of transaction dictionaries parsed from the filing.
                Returns an empty list if the XML download or parsing fails.
        """
        # Use document_handler to download the XML
        xml_content = await self.raw_filings.get_xml(accession_no, ticker=ticker)

        if not xml_content:
            logging.warning(f"Could not download Form 4 XML for {accession_no} (ticker: {ticker})")
            return []

        # Parse the XML
        return self.parse_form4_xml(xml_content)

    def _classify_signal(self, transaction: Dict[str, Any]) -> str:
        tx_code = str(transaction.get('transaction_code') or '').upper()
        price = transaction.get('price_per_share')
        is_derivative = bool(transaction.get('is_derivative'))

        # A holdings row is a position statement, not an event.
        if transaction.get('is_holding'):
            return 'holding'

        # Rule 10b5-1 plan sales. The authoritative source is the `aff10b5One` flag SEC
        # added to the ownership schema in 2023, plus the footnote text where filers
        # describe the plan. The previous check searched `security_title` for "10b5-1",
        # which only ever holds values like "Common Stock", so the branch was unreachable.
        if tx_code == 'S' and self._indicates_10b5_1_plan(transaction):
            return 'planned_sale_10b5_1'

        if tx_code == 'P' and not is_derivative and price not in (None, 0, 0.0):
            return 'open_market_buy'
        if tx_code == 'S' and not is_derivative and price not in (None, 0, 0.0):
            return 'open_market_sell'
        if tx_code == 'F':
            return 'tax_sale'
        if tx_code == 'M':
            return 'option_exercise'
        if tx_code in ('A',):
            return 'award_or_grant'
        if tx_code in ('G', 'D'):
            return 'gift'
        if tx_code in ('C', 'W'):
            return 'derivative_conversion'
        return self.SIGNAL_CLASS_MAP.get(tx_code, 'other')

    @staticmethod
    def _indicates_10b5_1_plan(transaction: Dict[str, Any]) -> bool:
        if transaction.get('rule_10b5_1_plan'):
            return True
        haystack = ' '.join(str(note) for note in (transaction.get('footnotes') or [])).lower()
        return '10b5-1' in haystack or '10b5‑1' in haystack

    def _economic_intent(self, signal_class: str) -> str:
        return self.ECONOMIC_INTENT_MAP.get(signal_class, 'neutral')

    def _event_identity(self, transaction: Dict[str, Any]) -> str:
        price = transaction.get('price_per_share')
        if price is None:
            price = transaction.get('conversion_exercise_price')
        return "|".join([
            str(transaction.get('issuer_cik') or ''),
            str(transaction.get('owner_cik') or ''),
            str(transaction.get('transaction_date') or ''),
            str(transaction.get('transaction_code') or ''),
            str(transaction.get('shares') or 0),
            str(price or 0),
        ])

    def _base_event_identity(self, transaction: Dict[str, Any]) -> str:
        return "|".join([
            str(transaction.get('issuer_cik') or ''),
            str(transaction.get('owner_cik') or ''),
            str(transaction.get('transaction_date') or ''),
            str(transaction.get('transaction_code') or ''),
        ])

    def _role_weight(self, role: str) -> float:
        role_l = (role or '').lower()
        if 'chief executive' in role_l or 'ceo' in role_l:
            return 1.0
        if 'chief financial' in role_l or 'cfo' in role_l:
            return 0.9
        if 'president' in role_l or 'chief operating' in role_l or 'coo' in role_l:
            return 0.85
        if 'director' in role_l:
            return 0.7
        # Compared against the lower-cased role: the parser emits '10% Owner', so matching
        # against the original-case string made this branch unreachable.
        if '10% owner' in role_l:
            return 0.65
        if 'officer' in role_l:
            return 0.75
        return 0.5

    def _safe_date(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(str(value)[:10], '%Y-%m-%d')
        except ValueError:
            return None

    def _normalize_signal_event(self, transaction: Dict[str, Any], filing_meta: Dict[str, Any], ticker: str) -> Dict[str, Any]:
        signal_class = self._classify_signal(transaction)
        price = transaction.get('price_per_share')
        if price is None:
            price = transaction.get('conversion_exercise_price')
        gross_value = transaction.get('value')
        if gross_value in (None, 0, 0.0) and price not in (None, 0, 0.0):
            try:
                gross_value = float(transaction.get('shares') or 0) * float(price)
            except (TypeError, ValueError):
                gross_value = 0.0

        form_name = str(filing_meta.get('form') or '4')
        is_amendment = form_name.endswith('/A')
        filing_date = filing_meta.get('filing_date')
        transaction_date = transaction.get('transaction_date') or filing_date
        anchor_timestamp = filing_date or transaction_date

        event = {
            'ticker': ticker.upper(),
            'issuer_cik': transaction.get('issuer_cik'),
            'issuer_name': transaction.get('issuer_name'),
            'owner_cik': transaction.get('owner_cik'),
            'owner_name': transaction.get('owner_name'),
            'owner_role': transaction.get('owner_position'),
            'transaction_date': transaction_date,
            'filing_date': filing_date,
            'event_anchor_type': 'filing_date',
            'event_anchor_timestamp': anchor_timestamp,
            'transaction_code': transaction.get('transaction_code'),
            'transaction_type': transaction.get('transaction_type'),
            'shares': transaction.get('shares'),
            'price_per_share': transaction.get('price_per_share'),
            'gross_value': float(gross_value or 0.0),
            'value': float(gross_value or 0.0),
            'is_derivative': bool(transaction.get('is_derivative')),
            'is_acquisition': bool(transaction.get('is_acquisition')),
            'is_disposition': bool(transaction.get('is_disposition')),
            'signal_class': signal_class,
            'economic_intent': self._economic_intent(signal_class),
            'accession_no': filing_meta.get('accession_no'),
            'form_url': filing_meta.get('url'),
            'primary_document': filing_meta.get('primary_document'),
            'primary_document_description': filing_meta.get('primary_document_description'),
            'form': form_name,
            'is_amendment': is_amendment,
            'amends_accession': None,
            'event_identity': '',
            'event_identity_base': '',
            'direct_indirect': transaction.get('direct_indirect'),
            'security_title': transaction.get('security_title'),
        }
        event['event_identity'] = self._event_identity(event)
        event['event_identity_base'] = self._base_event_identity(event)
        return event

    def _dedupe_and_apply_amendments(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ordered = sorted(
            events,
            key=lambda event: (
                event.get('filing_date') or '',
                event.get('accession_no') or '',
                event.get('transaction_date') or '',
            )
        )
        effective: List[Dict[str, Any]] = []
        seen_identity = set()

        for event in ordered:
            identity = event.get('event_identity')
            if identity in seen_identity and not event.get('is_amendment'):
                continue

            if event.get('is_amendment'):
                base_identity = event.get('event_identity_base')
                effective = [existing for existing in effective if existing.get('event_identity_base') != base_identity]
                seen_identity = {existing.get('event_identity') for existing in effective}

            if identity in seen_identity:
                continue

            effective.append(event)
            seen_identity.add(identity)

        return effective

    def _score_aggregate(self, aggregate: Dict[str, Any]) -> None:
        net_open_market_value = float(aggregate.get('net_open_market_value') or 0.0)
        unique_insiders = int(aggregate.get('unique_insiders') or 0)
        avg_role_weight = float(aggregate.get('avg_role_weight') or 0.0)
        clustered_buy_count = int(aggregate.get('clustered_buy_count') or 0)
        derivative_count = int(aggregate.get('derivative_event_count') or 0)
        tax_sale_count = int(aggregate.get('tax_sale_count') or 0)
        total_event_count = max(1, int(aggregate.get('total_event_count') or 1))
        open_market_activity = int(aggregate.get('open_market_buy_count') or 0) + int(aggregate.get('open_market_sell_count') or 0)

        reasons: List[str] = []
        score = 0.0

        if net_open_market_value > 0:
            positive = min(1.0, net_open_market_value / 1_000_000.0)
            score += positive * 0.45
            reasons.append(f"net_open_market_value={net_open_market_value:.0f}")
        elif net_open_market_value < 0:
            negative = min(1.0, abs(net_open_market_value) / 1_000_000.0)
            score -= negative * 0.45
            reasons.append(f"net_open_market_value={net_open_market_value:.0f}")

        if unique_insiders > 0 and open_market_activity > 0:
            insider_bonus = min(1.0, unique_insiders / 4.0) * 0.2
            score += insider_bonus if net_open_market_value >= 0 else -insider_bonus
            reasons.append(f"unique_insiders={unique_insiders}")

        if avg_role_weight > 0 and open_market_activity > 0:
            role_bonus = avg_role_weight * 0.15
            score += role_bonus if net_open_market_value >= 0 else -role_bonus
            reasons.append(f"role_weight={avg_role_weight:.2f}")

        if clustered_buy_count > 1:
            cluster_bonus = min(0.15, clustered_buy_count * 0.05)
            score += cluster_bonus
            reasons.append(f"clustered_buys={clustered_buy_count}")

        derivative_ratio = derivative_count / total_event_count
        if derivative_ratio > 0.5:
            score -= 0.1
            reasons.append('derivative_heavy')

        if tax_sale_count == total_event_count:
            score = min(score - 0.15, -0.05)
            reasons.append('tax_only')

        aggregate['signal_strength_score'] = round(max(-1.0, min(1.0, score)), 3)
        aggregate['signal_strength_reason'] = reasons

    def _build_daily_aggregates(self, ticker: str, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events_by_anchor: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        events_by_owner: Dict[str, List[datetime]] = defaultdict(list)

        for event in events:
            if event.get('signal_class') == 'open_market_buy':
                key = f"{event.get('owner_cik') or event.get('owner_name')}"
                event_date = self._safe_date(event.get('transaction_date'))
                if event_date:
                    events_by_owner[key].append(event_date)

        for event in events:
            anchor = str(event.get('event_anchor_timestamp') or '')[:10]
            if anchor:
                events_by_anchor[anchor].append(event)

        aggregates: List[Dict[str, Any]] = []
        for anchor_date, day_events in sorted(events_by_anchor.items()):
            open_buy = [event for event in day_events if event.get('signal_class') == 'open_market_buy']
            open_sell = [event for event in day_events if event.get('signal_class') == 'open_market_sell']
            tax_sales = [event for event in day_events if event.get('signal_class') == 'tax_sale']
            option_events = [event for event in day_events if event.get('signal_class') == 'option_exercise']
            gifts = [event for event in day_events if event.get('signal_class') == 'gift']
            non_economic = [event for event in day_events if event.get('economic_intent') in ('neutral', 'compensation')]

            unique_filing_links = []
            filing_seen = set()
            for event in day_events:
                link = event.get('form_url')
                if link and link not in filing_seen:
                    unique_filing_links.append(link)
                    filing_seen.add(link)

            event_date = self._safe_date(anchor_date)
            clustered_buys = 0
            if event_date:
                for event in open_buy:
                    owner_key = f"{event.get('owner_cik') or event.get('owner_name')}"
                    owner_dates = events_by_owner.get(owner_key, [])
                    clustered_buys += sum(1 for candidate in owner_dates if abs((candidate - event_date).days) <= 3)

            key_events = sorted(
                day_events,
                key=lambda event: (
                    abs(float(event.get('gross_value') or 0.0)),
                    self._role_weight(str(event.get('owner_role') or '')),
                ),
                reverse=True,
            )[:3]

            aggregate = {
                'ticker': ticker,
                'event_anchor_type': 'filing_date',
                'event_anchor_timestamp': anchor_date,
                'transaction_date': anchor_date,
                'filing_date': anchor_date,
                'total_event_count': len(day_events),
                'open_market_buy_count': len(open_buy),
                'open_market_sell_count': len(open_sell),
                'tax_sale_count': len(tax_sales),
                'option_exercise_count': len(option_events),
                'gift_count': len(gifts),
                'non_economic_event_count': len(non_economic),
                'derivative_event_count': sum(1 for event in day_events if event.get('is_derivative')),
                'unique_insiders': len({event.get('owner_cik') or event.get('owner_name') for event in day_events}),
                'net_open_market_value': sum(float(event.get('gross_value') or 0.0) for event in open_buy)
                    - sum(float(event.get('gross_value') or 0.0) for event in open_sell),
                'net_value': sum(float(event.get('gross_value') or 0.0) for event in open_buy)
                    - sum(float(event.get('gross_value') or 0.0) for event in open_sell),
                'net_shares': sum(float(event.get('shares') or 0.0) for event in open_buy)
                    - sum(float(event.get('shares') or 0.0) for event in open_sell),
                'avg_role_weight': (
                    sum(self._role_weight(str(event.get('owner_role') or '')) for event in day_events) / len(day_events)
                    if day_events else 0.0
                ),
                'clustered_buy_count': clustered_buys,
                'signal_strength_score': 0.0,
                'signal_strength_reason': [],
                'key_events': [
                    {
                        'owner_name': event.get('owner_name'),
                        'role': event.get('owner_role'),
                        'signal_class': event.get('signal_class'),
                        'gross_value': event.get('gross_value'),
                        'importance_score': round(
                            abs(float(event.get('gross_value') or 0.0)) / 1_000_000.0
                            + self._role_weight(str(event.get('owner_role') or '')),
                            3,
                        ),
                        'reason': [
                            f"signal_class={event.get('signal_class')}",
                            f"gross_value={float(event.get('gross_value') or 0.0):.0f}",
                        ],
                        'filing_url': event.get('form_url'),
                    }
                    for event in key_events
                ],
                'filing_links': unique_filing_links[:3],
            }
            self._score_aggregate(aggregate)
            aggregates.append(aggregate)

        return aggregates

    def detect_cluster_buys(
        self,
        events: List[Dict[str, Any]],
        window_days: int = 14,
        min_unique_insiders: int = 3,
    ) -> List[Dict[str, Any]]:
        """Detect windows where multiple distinct insiders made open-market buys.

        Slides a window of `window_days` over the open-market buy stream. Each window
        anchored at a buy event is evaluated; if at least `min_unique_insiders` distinct
        owners bought within the window, a cluster is emitted. Clusters are then
        consolidated greedily so that overlapping windows merge into one (the merged
        cluster spans the union of dates and combines all participating insiders).

        Returns a list ranked by `total_value` descending. Each cluster:
            {
                'window_start': 'YYYY-MM-DD',
                'window_end': 'YYYY-MM-DD',
                'unique_insiders': int,
                'event_count': int,
                'total_value': float,   # sum of gross_value (USD)
                'total_shares': float,
                'insiders': [{'owner_name', 'owner_role', 'gross_value', 'transaction_date'}, ...],
                'filing_urls': [...],
            }
        """
        if window_days < 1 or min_unique_insiders < 2:
            return []

        buys: List[Dict[str, Any]] = []
        for event in events:
            if event.get('signal_class') != 'open_market_buy':
                continue
            tx_date = self._safe_date(event.get('transaction_date'))
            if tx_date is None:
                continue
            buys.append({**event, '_tx_date': tx_date})

        if len(buys) < min_unique_insiders:
            return []

        buys.sort(key=lambda buy: buy['_tx_date'])

        raw_clusters: List[Dict[str, Any]] = []
        for index, anchor in enumerate(buys):
            window_end_date = anchor['_tx_date'] + timedelta(days=window_days)
            members = [
                buy for buy in buys[index:]
                if buy['_tx_date'] <= window_end_date
            ]
            unique_owner_keys = {buy.get('owner_cik') or buy.get('owner_name') for buy in members}
            if len(unique_owner_keys) < min_unique_insiders:
                continue
            raw_clusters.append(
                {
                    'start': members[0]['_tx_date'],
                    'end': members[-1]['_tx_date'],
                    'members': members,
                }
            )

        if not raw_clusters:
            return []

        merged: List[Dict[str, Any]] = []
        current = raw_clusters[0]
        for candidate in raw_clusters[1:]:
            if candidate['start'] <= current['end']:
                # Overlap: merge the windows and event sets (dedupe by event_identity)
                merged_end = max(current['end'], candidate['end'])
                seen = {member.get('event_identity') for member in current['members']}
                combined = list(current['members'])
                for member in candidate['members']:
                    if member.get('event_identity') not in seen:
                        combined.append(member)
                        seen.add(member.get('event_identity'))
                current = {'start': current['start'], 'end': merged_end, 'members': combined}
            else:
                merged.append(current)
                current = candidate
        merged.append(current)

        clusters: List[Dict[str, Any]] = []
        for cluster in merged:
            members = cluster['members']
            unique_owner_keys = {member.get('owner_cik') or member.get('owner_name') for member in members}
            total_value = sum(float(member.get('gross_value') or 0.0) for member in members)
            total_shares = sum(float(member.get('shares') or 0.0) for member in members)
            insiders_summary = [
                {
                    'owner_name': member.get('owner_name'),
                    'owner_role': member.get('owner_role'),
                    'gross_value': round(float(member.get('gross_value') or 0.0), 2),
                    'transaction_date': member.get('transaction_date'),
                }
                for member in sorted(
                    members,
                    key=lambda member: float(member.get('gross_value') or 0.0),
                    reverse=True,
                )
            ]
            filing_urls: List[str] = []
            for member in members:
                url = member.get('form_url')
                if url and url not in filing_urls:
                    filing_urls.append(url)

            clusters.append(
                {
                    'window_start': cluster['start'].strftime('%Y-%m-%d'),
                    'window_end': cluster['end'].strftime('%Y-%m-%d'),
                    'unique_insiders': len(unique_owner_keys),
                    'event_count': len(members),
                    'total_value': round(total_value, 2),
                    'total_shares': total_shares,
                    'insiders': insiders_summary,
                    'filing_urls': filing_urls,
                }
            )

        clusters.sort(key=lambda cluster: cluster['total_value'], reverse=True)
        return clusters

    def _build_llm_digest(self, ticker: str, events: List[Dict[str, Any]], aggregates: List[Dict[str, Any]], anchor_type: str) -> Dict[str, Any]:
        open_buy_events = [event for event in events if event.get('signal_class') == 'open_market_buy']
        open_sell_events = [event for event in events if event.get('signal_class') == 'open_market_sell']
        total_filings = len({event.get('accession_no') for event in events if event.get('accession_no')})
        unique_insiders = len({event.get('owner_cik') or event.get('owner_name') for event in events})
        total_open_buy_value = sum(float(event.get('gross_value') or 0.0) for event in open_buy_events)
        total_open_sell_value = sum(float(event.get('gross_value') or 0.0) for event in open_sell_events)
        buy_sell_ratio = round(total_open_buy_value / total_open_sell_value, 3) if total_open_sell_value > 0 else None

        ranked_events = sorted(
            events,
            key=lambda event: (
                abs(float(event.get('gross_value') or 0.0)),
                self._role_weight(str(event.get('owner_role') or '')),
            ),
            reverse=True,
        )[:5]

        anomalies: List[str] = []
        if sum(1 for aggregate in aggregates if aggregate.get('open_market_buy_count', 0) > 0) >= 2:
            anomalies.append('clustered_buying')
        if len({event.get('owner_name') for event in open_buy_events + open_sell_events}) >= 3:
            anomalies.append('repeated_insider_activity')
        if ranked_events and abs(float(ranked_events[0].get('gross_value') or 0.0)) >= 1_000_000.0:
            anomalies.append('unusually_large_trade')

        caveats: List[str] = []
        if events and sum(1 for event in events if event.get('is_derivative')) / max(1, len(events)) > 0.5:
            caveats.append('derivative_heavy_period')
        if events and all(event.get('signal_class') == 'tax_sale' for event in events):
            caveats.append('mostly_tax_sales')
        if not open_buy_events and not open_sell_events:
            caveats.append('limited_open_market_activity')

        return {
            'summary': {
                'ticker': ticker,
                'total_filings': total_filings,
                'net_value': round(total_open_buy_value - total_open_sell_value, 2),
                'unique_insiders': unique_insiders,
                'buy_sell_ratio': buy_sell_ratio,
                'as_of': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'anchor_type': anchor_type,
            },
            'key_events': [
                {
                    'owner_name': event.get('owner_name'),
                    'role': event.get('owner_role'),
                    'signal_class': event.get('signal_class'),
                    'gross_value': round(float(event.get('gross_value') or 0.0), 2),
                    'importance_score': round(
                        abs(float(event.get('gross_value') or 0.0)) / 1_000_000.0
                        + self._role_weight(str(event.get('owner_role') or '')),
                        3,
                    ),
                    'reason': [
                        f"signal_class={event.get('signal_class')}",
                        f"transaction_type={event.get('transaction_type')}",
                    ],
                    'filing_url': event.get('form_url'),
                }
                for event in ranked_events
            ],
            'anomalies': anomalies,
            'caveats': caveats,
        }

    async def get_insider_signal_payload(self, ticker: str, days_back: int = 180,
                                         use_cache: bool = True, filing_limit: int = 40,
                                         anchor_type: str = 'filing_date') -> Dict[str, Any]:
        """Canonical insider payload. Delta lives at the accession-index layer only:
        the (day-cached) filing metadata names the source accessions; if the cached
        payload's fingerprint matches (parser version + accession set + config) it is
        served regardless of age, otherwise the payload is re-derived — parsing local
        raw filings and downloading only accessions the raw store has never seen."""
        ticker = ticker.upper()
        filings_meta = await self.fetch_filings_metadata(ticker, days_back=days_back, use_cache=use_cache)
        if filing_limit > 0:
            filings_meta = filings_meta[:filing_limit]
        accessions = [meta.get('accession_no') for meta in filings_meta if meta.get('accession_no')]
        fingerprint = _payload_fingerprint(accessions, days_back, filing_limit, anchor_type)

        if self.cache_manager is not None:
            cached = await self.cache_manager.load_data(
                ticker,
                'insider_signals',
                days_back=days_back,
                filing_limit=filing_limit,
                anchor_type=anchor_type,
            )
            if isinstance(cached, dict) and cached.get('fingerprint') == fingerprint:
                return cached

        normalized_events: List[Dict[str, Any]] = []
        for filing_meta in filings_meta:
            accession_no = filing_meta.get('accession_no')
            if not accession_no:
                continue
            parsed_transactions = await self.process_form4_filing(accession_no, ticker=ticker)
            for transaction in parsed_transactions:
                # Holdings rows are position statements, not events. They are parsed and
                # available on `parse_form4_xml` output (Form 3 consists almost entirely of
                # them), but including them here would inflate the event and aggregate
                # counts this payload's consumers treat as transaction activity.
                if transaction.get('is_holding'):
                    continue
                normalized_events.append(self._normalize_signal_event(transaction, filing_meta, ticker))

        payload = self._build_signal_payload(ticker, normalized_events, days_back, filing_limit, anchor_type)
        payload['fingerprint'] = fingerprint
        if self.cache_manager is not None:
            await self.cache_manager.save_data(
                ticker,
                'insider_signals',
                payload,
                days_back=days_back,
                filing_limit=filing_limit,
                anchor_type=anchor_type,
            )
        return payload

    async def refresh_insider_signal_payload(self, ticker: str, days_back: int = 180,
                                             filing_limit: int = 40,
                                             anchor_type: str = 'filing_date') -> Dict[str, Any]:
        """Live accession-index check ("Check SEC now"). The raw filing store makes the
        rebuild local except for genuinely new filings, so this no longer needs its own
        merge logic — it's the normal path with a fresh index."""
        return await self.get_insider_signal_payload(
            ticker,
            days_back=days_back,
            use_cache=False,
            filing_limit=filing_limit,
            anchor_type=anchor_type,
        )

    def _build_signal_payload(self, ticker: str, normalized_events: List[Dict[str, Any]],
                              days_back: int, filing_limit: int, anchor_type: str) -> Dict[str, Any]:
        effective_events = self._dedupe_and_apply_amendments(normalized_events)
        daily_aggregates = self._build_daily_aggregates(ticker, effective_events)
        clusters = self.detect_cluster_buys(effective_events)
        llm_digest = self._build_llm_digest(ticker, effective_events, daily_aggregates, anchor_type)
        if clusters:
            llm_digest.setdefault('anomalies', []).append('insider_cluster_buy')

        return {
            'symbol': ticker,
            'payload_version': SIGNAL_PAYLOAD_VERSION,
            'window': {
                'days_back': days_back,
                'filing_limit': filing_limit,
            },
            'as_of': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'events': effective_events,
            'daily_aggregates': daily_aggregates,
            'clusters': clusters,
            'llm_digest': llm_digest,
        }

    async def get_recent_insider_transactions(self, ticker: str, days_back: int = 90,
                                         use_cache: bool = True, filing_limit: int = 10) -> List[Dict]:
        """
        Fetches metadata, downloads, parses, and formats recent Form 4 transactions.

        This is a high-level method designed to retrieve a list of recent insider
        transactions suitable for direct use (e.g., displaying in a UI table).

        Workflow:
        1. Calls the injected `fetch_filings_metadata` function to get a list of recent
           Form 4 filings (accession numbers, dates, etc.) for the `ticker`.
        2. Iterates through the fetched metadata (up to `filing_limit`).
        3. For each filing, calls `process_form4_filing` to download and parse its XML.
        4. Formats the relevant fields from the parsed transactions into a simplified
           dictionary structure commonly needed for display.
        5. Appends these formatted dictionaries to a final list.

        Args:
            ticker (str): The stock ticker symbol of the issuer.
            days_back (int, optional): How many days back to look for Form 4 filings.
                Passed to the `fetch_filings_metadata` function. Defaults to 90.
            use_cache (bool, optional): Whether the `fetch_filings_metadata` function
                should attempt to use cached filing metadata. Defaults to True.
            filing_limit (int, optional): The maximum number of the most recent filings
                to download and parse. This helps limit processing time and API usage.
                Defaults to 10.

        Returns:
            List[Dict]: A list of dictionaries, each representing a formatted insider
                transaction ready for display. Keys typically include 'filer' (owner name),
                'date', 'type' (e.g., Purchase, Sale), 'shares', 'price', 'value',
                'form_url', 'primary_document'. Returns an empty list if no filings
                are found or no transactions can be parsed.
        """
        ticker = ticker.upper()

        # Get Form 4 filing metadata using the injected function
        filings_meta = await self.fetch_filings_metadata(ticker, days_back=days_back, use_cache=use_cache)

        if not filings_meta:
            logging.info(f"No recent Form 4 filing metadata found for {ticker}")
            return []

        all_ui_transactions = []
        logging.info(f"Processing up to {filing_limit} most recent Form 4 filings for {ticker}...")

        processed_count = 0
        for filing_meta in filings_meta:
            if processed_count >= filing_limit:
                 logging.info(f"Reached processing limit of {filing_limit} filings for {ticker}.")
                 break

            accession_no = filing_meta.get('accession_no')
            filing_url = filing_meta.get('url', 'N/A') # Get URL from metadata if available
            primary_doc_name = filing_meta.get('primary_document') # Get primary doc name

            if not accession_no:
                logging.warning(f"Skipping filing for {ticker} due to missing accession number in metadata: {filing_meta}")
                continue

            # Process the XML to get detailed transactions
            parsed_transactions = await self.process_form4_filing(accession_no, ticker=ticker)

            if not parsed_transactions:
                logging.debug(f"No transactions parsed for {ticker}, accession: {accession_no}")
                continue # Move to the next filing

            processed_count += 1

            # Format each parsed transaction for the UI
            for tx in parsed_transactions:
                if tx.get('is_holding'):
                    continue
                ui_transaction = {
                    'filer': tx.get('owner_name', 'N/A'),
                    'position': tx.get('owner_position', 'N/A'),
                    'date': tx.get('transaction_date', 'N/A'),
                    'type': tx.get('transaction_type', 'Unknown'),
                    'shares': tx.get('shares'),
                    'price': tx.get('price_per_share') if not tx.get('is_derivative') else tx.get('conversion_exercise_price'),
                    'value': tx.get('value') if not tx.get('is_derivative') else None, # Value calculation for derivatives is complex
                    'form_url': filing_url, # Use URL from metadata
                    'primary_document': primary_doc_name # Use filename from metadata
                }

                # Recalculate value for non-derivatives if needed
                if not tx.get('is_derivative') and ui_transaction['value'] is None:
                     shares = ui_transaction.get('shares')
                     price = ui_transaction.get('price')
                     if shares is not None and price is not None:
                          try: ui_transaction['value'] = float(shares) * float(price)
                          except (ValueError, TypeError): ui_transaction['value'] = None

                all_ui_transactions.append(ui_transaction)

        logging.info(f"Completed processing {processed_count} filings for {ticker}. Found {len(all_ui_transactions)} transactions.")
        return all_ui_transactions

    async def analyze_insider_transactions(self, ticker: str, days_back: int = 90, use_cache: bool = True) -> Dict:
        """
        Performs a basic quantitative analysis of recent insider transactions for a ticker.

        Downloads and parses recent Form 4 filings (similar to
        `get_recent_insider_transactions` but retrieves the full parsed data),
        converts the data into a pandas DataFrame, and calculates summary statistics.

        Workflow:
        1. Fetches recent Form 4 filing metadata for the `ticker`.
        2. Processes each filing using `process_form4_filing` to get detailed transactions.
        3. Concatenates all parsed transactions into a single list.
        4. Converts the list into a pandas DataFrame.
        5. Calculates metrics like:
           - Total number of transactions.
           - Number of buy vs. sell transactions (based on transaction codes).
           - Total value of buy vs. sell transactions.
           - Net transaction value (Total Buy Value - Total Sell Value).
           - Number of unique owners involved.
           - List of unique owners involved.
           - Optionally includes the raw DataFrame.

        Args:
            ticker (str): The stock ticker symbol of the issuer.
            days_back (int, optional): How many days back to look for Form 4 filings.
                Defaults to 90.
            use_cache (bool, optional): Whether to use cached filing metadata when fetching
                the list of filings to process. Defaults to True.

        Returns:
            Dict: A dictionary containing the analysis results. Keys include
                'ticker', 'analysis_period_days', 'total_transactions', 'buy_count',
                'sell_count', 'total_buy_value', 'total_sell_value', 'net_value',
                'involved_owners_count', 'involved_owners_list'. Includes an 'error'
                key if fetching or analysis fails. May include 'dataframe' if successful.
        """
        ticker = ticker.upper()

        # Get *parsed* transactions first (not UI formatted)
        # Need a method that fetches filings and processes them without UI formatting
        # Let's adapt process_form4_filing to run over multiple filings

        logging.info(f"Analyzing insider transactions for {ticker} ({days_back} days back)...")
        filings_meta = await self.fetch_filings_metadata(ticker, days_back=days_back, use_cache=use_cache)
        if not filings_meta:
             return {'ticker': ticker, 'error': "No filing metadata found."} 

        all_parsed_transactions = []
        # No limit for analysis, process all filings in the period
        for filing_meta in filings_meta:
             accession_no = filing_meta.get('accession_no')
             if not accession_no:
                 continue
             parsed = await self.process_form4_filing(accession_no, ticker=ticker)
             all_parsed_transactions.extend(
                 row for row in parsed if not row.get('is_holding')
             )

        if not all_parsed_transactions:
            return {
                'ticker': ticker,
                'error': "No transactions found in filings.",
                'total_transactions': 0
             }

        if pd is None:
            return {
                'ticker': ticker,
                'error': "pandas is required for transaction analysis.",
                'total_transactions_parsed': len(all_parsed_transactions),
            }

        df = pd.DataFrame(all_parsed_transactions)

        try:
            # Ensure required columns exist and handle potential NaNs
            df['is_acquisition'] = df['is_acquisition'].fillna(False)
            df['is_disposition'] = df['is_disposition'].fillna(False)
            df['value'] = pd.to_numeric(df['value'], errors='coerce').fillna(0)
            df['shares'] = pd.to_numeric(df['shares'], errors='coerce').fillna(0)
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

            # Filter out rows where conversion failed
            df = df.dropna(subset=['transaction_date'])

            # Separate buys/sells based on the boolean flags
            # Consider only non-derivative transactions for simple value analysis
            non_deriv_df = df[~df['is_derivative'].fillna(False)]
            buys = non_deriv_df[non_deriv_df['is_acquisition'] == True]
            sells = non_deriv_df[non_deriv_df['is_disposition'] == True]

            # Summary statistics
            result = {
                'ticker': ticker,
                'analysis_period_days': days_back,
                'total_filings_processed': len(filings_meta),
                'total_transactions_parsed': len(df),
                'buy_transaction_count': len(buys),
                'sell_transaction_count': len(sells),
                'total_buy_value': buys['value'].sum(),
                'total_sell_value': sells['value'].sum(),
                'net_value': buys['value'].sum() - sells['value'].sum(),
                'unique_filers': df['owner_name'].nunique(),
                'involved_filers': df['owner_name'].unique().tolist(),
                'analysis_start_date': df['transaction_date'].min().strftime('%Y-%m-%d') if not df.empty else None,
                'analysis_end_date': df['transaction_date'].max().strftime('%Y-%m-%d') if not df.empty else None,
                # Optional: More detailed stats
                # 'transactions_by_type': df['transaction_type'].value_counts().to_dict(),
                # 'top_buyers_by_value': buys.groupby('owner_name')['value'].sum().nlargest(5).to_dict(),
                # 'top_sellers_by_value': sells.groupby('owner_name')['value'].sum().nlargest(5).to_dict(),
            }

            logging.info(f"Analysis complete for {ticker}. Net value: {result['net_value']:.2f}")
            return result

        except Exception as e:
            logging.error(f"Error analyzing insider transactions for {ticker}: {e}", exc_info=True)
            return {
                'ticker': ticker,
                'error': f"Analysis error: {str(e)}",
                'total_transactions_parsed': len(df)
            } 
