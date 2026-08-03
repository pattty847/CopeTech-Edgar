import json
import os
import logging
import asyncio
import re

from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from typing import List, Dict, Optional, Union, Tuple, Any, Callable, Awaitable, Iterable
from .http_client import SecHttpClient
from .errors import SecNotFoundError
from .cache_manager import SecCacheManager
from .document_handler import FilingDocumentHandler
from .form4_processor import Form4Processor
from .form8k_processor import Form8KProcessor
from .form144_processor import Form144Processor
from .financial_processor import FinancialDataProcessor
from .financial_series_service import FinancialSeriesService
from .supply_chain_parser import SupplyChainParser
from .sql_cache_manager import SqlCacheManager
from .thirteenf_processor import ThirteenFProcessor
from .identifiers import Cik, Ticker
from .submissions import SubmissionsResource


def load_dotenv_settings(**kwargs) -> bool:
    """Load a `.env` file into `os.environ`.

    Importing this module used to call `load_dotenv()` at module scope, so
    `import copetech_sec` mutated the host process's environment as a side effect — a
    library should not do that to its embedder. Applications that want the behavior
    (`app.py`, the CLI wrappers) can call this explicitly.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        logging.debug("python-dotenv is not installed; skipping .env load.")
        return False
    return load_dotenv(**kwargs)


class SECDataFetcher:
    """
    Deprecated 0.2.x compatibility facade for fetching and processing SEC data.

    New integrations should use :class:`copetech_sec.EdgarClient`, whose resource
    namespaces expose the same contracts without growing this orchestrator further.
    Initializes and delegates tasks to specialized handler/processor classes.
    """

    # Base endpoints remain here as they are fundamental
    SUBMISSIONS_ENDPOINT = "https://data.sec.gov/submissions/CIK{cik}.json"
    COMPANY_FACTS_ENDPOINT = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    COMPANY_CONCEPT_ENDPOINT = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/{taxonomy}/{concept}.json"

    # General constants like FORM_TYPES can stay here or move to a central constants file
    FORM_TYPES = {
        "4": "Statement of changes in beneficial ownership (insider transactions)",
        "3": "Initial statement of beneficial ownership",
        "5": "Annual statement of beneficial ownership",
        "8-K": "Current report",
        "10-K": "Annual report",
        "10-Q": "Quarterly report",
        "13F": "Institutional investment manager holdings report",
        "SC 13G": "Beneficial ownership report",
        "SC 13D": "Beneficial ownership report (active)",
        "DEF 14A": "Definitive proxy statement",
    }

    # Constants moved to specific processors:
    # - TRANSACTION_CODE_MAP, ACQUISITION_CODES, DISPOSITION_CODES -> Form4Processor
    # - KEY_FINANCIAL_SUMMARY_METRICS -> FinancialDataProcessor

    def __init__(self, user_agent: str = None, cache_dir: str = "data/edgar",
                 rate_limit_sleep: float = 0.1):
        """
        Initialize the SECDataFetcher orchestrator.

        Args:
            user_agent (str, optional): User-Agent string for SEC API requests.
                             Format: "Name (first/last) your@email.com".
                             Defaults to None, attempts to read from 'SEC_API_USER_AGENT' env var.
            cache_dir (str, optional): Directory for storing cached data. Defaults to "data/edgar".
            rate_limit_sleep (float, optional): Seconds to wait between API requests for the HTTP client.
                                               Defaults to 0.1.
        """
        # Initialize HttpClient, CacheManager, and other processors here
        resolved_user_agent = user_agent or os.environ.get('SEC_API_USER_AGENT')
        self.http_client = SecHttpClient(user_agent=resolved_user_agent, rate_limit_sleep=rate_limit_sleep)
        self.cache_manager = SecCacheManager(cache_dir=cache_dir)
        self.sql_manager = SqlCacheManager() # Initialize SQL manager
        self.document_handler = FilingDocumentHandler(http_client=self.http_client, cik_lookup_func=self.get_cik_for_ticker)
        self.form4_processor = Form4Processor(
            document_handler=self.document_handler,
            fetch_filings_func=self.fetch_insider_filings, # Pass the method directly
            cache_manager=self.cache_manager,
        )
        self.form144_processor = Form144Processor(
            document_handler=self.document_handler,
            fetch_filings_func=self.fetch_planned_sale_filings,
            cache_manager=self.cache_manager,
        )
        self.form8k_processor = Form8KProcessor(
            fetch_filings_func=self.fetch_current_reports,
            cache_manager=self.cache_manager,
        )
        self.financial_processor = FinancialDataProcessor(
            fetch_facts_func=self.get_company_facts # Pass the method directly
        )
        self.financial_series = FinancialSeriesService(
            fetch_facts=self.get_company_facts,
            store_path=os.path.join(cache_dir, "financial_series.sqlite3"),
        )
        self.submissions_resource = SubmissionsResource(
            http_client=self.http_client,
            cache_manager=self.cache_manager,
        )
        self.thirteenf_processor = ThirteenFProcessor(
            http_client=self.http_client,
            cache_manager=self.cache_manager,
            document_handler=self.document_handler,
            submissions_resource=self.submissions_resource,
        )
        self.supply_chain_parser = SupplyChainParser() # Initialize Parser


    # === CORE ORCHESTRATION METHODS ===

    async def get_cik_for_ticker(self, ticker: str) -> Optional[str]:
        """Retrieves the 10-digit CIK for a given stock ticker symbol."""
        ticker = Ticker(ticker)
        cik = await self.cache_manager.load_cik(ticker)
        if cik: return cik

        logging.info(f"CIK for {ticker} not in cache. Fetching map...")
        success = await self._fetch_and_cache_cik_map()
        if success:
            cik = await self.cache_manager.load_cik(ticker)
            if cik: return cik

        logging.error(f"Could not find or fetch CIK for {ticker}.")
        return None

    async def _fetch_and_cache_cik_map(self) -> bool:
        """Fetches the official Ticker-CIK map from SEC and caches it."""
        url = "https://www.sec.gov/files/company_tickers.json"
        logging.info(f"Fetching Ticker-CIK map from {url}")
        # Prepare headers for www.sec.gov
        temp_headers = self.http_client.default_headers.copy()
        temp_headers['Host'] = 'www.sec.gov'

        sec_map_data = await self.http_client.make_request(url, headers=temp_headers, is_json=True)
        if not isinstance(sec_map_data, dict):
            raise TypeError(f"Ticker-CIK map must be a dict, got {type(sec_map_data).__name__}.")

        ticker_to_cik = {}
        for _index, company_info in sec_map_data.items():
            ticker_val = company_info.get('ticker')
            cik_int = company_info.get('cik_str')
            if ticker_val and cik_int:
                    cik_str = str(Cik(cik_int))
                    ticker_to_cik[str(Ticker(ticker_val))] = cik_str

        await self.cache_manager.save_cik_map(ticker_to_cik)
        return True

    async def get_company_info(self, ticker: str, use_cache: bool = True) -> Optional[Dict]:
        """Fetches basic company information using the SEC submissions endpoint."""
        ticker = Ticker(ticker)
        cik = await self.get_cik_for_ticker(ticker)
        if not cik:
            return None
        cache_key = f"CIK{cik}"
        if use_cache:
            cache_data = await self.cache_manager.load_data(cache_key, 'company_info')
            if (
                isinstance(cache_data, dict)
                and "tickers" in cache_data
                and "former_names" in cache_data
            ):
                return {**cache_data, "ticker": str(ticker)}

        submissions_url = self.SUBMISSIONS_ENDPOINT.format(cik=cik)
        try:
            response = await self.http_client.make_request(submissions_url, is_json=True)
        except SecNotFoundError:
            return None
        if not isinstance(response, dict):
            raise TypeError(f"Company submissions must be a dict, got {type(response).__name__}.")

        company_info = {
            'ticker': str(ticker),
            'cik': cik,
            'name': response.get('name'),
            'sic': response.get('sic'),
            'sic_description': response.get('sicDescription'),
            'address': response.get('addresses', {}).get('mailing'),
            'phone': response.get('phone'),
            'exchange': response.get('exchanges'),
            'tickers': list(response.get('tickers') or [str(ticker)]),
            'exchanges': list(response.get('exchanges') or []),
            'share_classes': [
                {'ticker': class_ticker, 'exchange': exchange}
                for class_ticker, exchange in zip_longest(
                    response.get('tickers') or [str(ticker)],
                    response.get('exchanges') or [],
                )
            ],
            'former_names': list(response.get('formerNames') or []),
        }
        await self.cache_manager.save_data(cache_key, 'company_info', company_info)
        return company_info

    async def get_company_submissions(self, ticker: str, use_cache: bool = True) -> Optional[Dict]:
        """ Fetches the complete submissions data for a company."""
        ticker = Ticker(ticker)
        cik = await self.get_cik_for_ticker(ticker)
        if not cik:
            return None
        cache_key = f"CIK{cik}"
        if use_cache:
            cache_data = await self.cache_manager.load_data(cache_key, 'submissions')
            if cache_data: return cache_data

        submissions_url = self.SUBMISSIONS_ENDPOINT.format(cik=cik)
        try:
            response = await self.http_client.make_request(submissions_url, is_json=True)
        except SecNotFoundError:
            return None
        if not isinstance(response, dict):
            raise TypeError(f"Company submissions must be a dict, got {type(response).__name__}.")

        await self.cache_manager.save_data(cache_key, 'submissions', response)
        return response

    async def get_company_facts(self, ticker: str, use_cache: bool = True) -> Optional[Dict]:
        """[Orchestrator] Fetches company facts (XBRL data) needed by FinancialProcessor."""
        ticker = Ticker(ticker)
        cik = await self.get_cik_for_ticker(ticker)
        if not cik:
            return None
        cache_key = f"CIK{cik}"
        if use_cache:
            cached_data = await self.cache_manager.load_data(cache_key, 'facts')
            if cached_data: return cached_data

        facts_url = self.COMPANY_FACTS_ENDPOINT.format(cik=cik)
        logging.info(f"Fetching company facts from: {facts_url}")
        try:
            company_facts_data = await self.http_client.make_request(facts_url, is_json=True)
        except SecNotFoundError:
            return None
        if not isinstance(company_facts_data, dict):
            raise TypeError(f"Company Facts must be a dict, got {type(company_facts_data).__name__}.")

        await self.cache_manager.save_data(cache_key, 'facts', company_facts_data)
        return company_facts_data

    async def get_filings_by_form(self, ticker: str, form_type: Union[str, Iterable[str]], days_back: int = 90, use_cache: bool = True) -> List[Dict]:
        """ Fetches a list of filings of a specific form type within a given timeframe."""
        result = await self.get_filings_page(
            ticker,
            form_type,
            days_back=days_back,
            use_cache=use_cache,
        )
        return result["items"]

    async def get_filings_page(
        self,
        ticker: str,
        form_type: Union[str, Iterable[str]],
        *,
        days_back: int = 90,
        use_cache: bool = True,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Dict[str, Any]:
        """Return filings plus source, warning, and pagination metadata."""

        ticker = Ticker(ticker)
        requested_forms = [form_type] if isinstance(form_type, str) else list(form_type)
        requested_forms = [form for form in requested_forms if form]
        if not requested_forms:
            raise ValueError("At least one form type is required.")
        cik = await self.get_cik_for_ticker(ticker)
        if not cik:
            return {
                "items": [],
                "metadata": {
                    "retrievedAt": datetime.now(timezone.utc).isoformat(),
                    "source": "sec-submissions",
                    "sourceFiles": [],
                    "truncated": False,
                    "nextCursor": None,
                    "warnings": ["ticker_not_found"],
                },
            }

        submissions = await self.get_company_submissions(ticker, use_cache=use_cache)
        if not submissions:
            return {
                "items": [],
                "metadata": {
                    "retrievedAt": datetime.now(timezone.utc).isoformat(),
                    "source": "sec-submissions",
                    "sourceFiles": [],
                    "truncated": False,
                    "nextCursor": None,
                    "warnings": ["submissions_not_found"],
                },
            }
        result = await self.submissions_resource.query_filings(
            submissions,
            cik=cik,
            forms=requested_forms,
            days_back=days_back,
            use_cache=use_cache,
            limit=limit,
            cursor=cursor,
        )
        return result.to_dict()

    # === CONVENIENCE FILING WRAPPERS ===
    async def fetch_insider_filings(self, ticker: str, days_back: int = 90, use_cache: bool = True) -> List[Dict]:
        """Convenience method to fetch Form 4 (insider trading) filings."""
        return await self.get_filings_by_form(ticker, ["4", "4/A"], days_back, use_cache)

    async def fetch_annual_reports(self, ticker: str, days_back: int = 365*2, use_cache: bool = True) -> List[Dict]:
        """Convenience method to fetch Form 10-K (annual report) filings."""
        return await self.get_filings_by_form(ticker, "10-K", days_back, use_cache)

    async def fetch_quarterly_reports(self, ticker: str, days_back: int = 365, use_cache: bool = True) -> List[Dict]:
        """Convenience method to fetch Form 10-Q (quarterly report) filings."""
        return await self.get_filings_by_form(ticker, "10-Q", days_back, use_cache)

    async def fetch_current_reports(self, ticker: str, days_back: int = 90, use_cache: bool = True) -> List[Dict]:
        """Convenience method to fetch Form 8-K (current report) filings."""
        return await self.get_filings_by_form(ticker, "8-K", days_back, use_cache)

    async def fetch_planned_sale_filings(self, ticker: str, days_back: int = 90, use_cache: bool = True) -> List[Dict]:
        """Convenience method to fetch Form 144 (notice of proposed sale) filings."""
        return await self.get_filings_by_form(ticker, ["144", "144/A"], days_back, use_cache)

    async def get_13f_filings(self, cik: str, days_back: int = 365*3, use_cache: bool = True) -> List[Dict]:
        """Convenience method to fetch Form 13F-HR filing metadata for an institutional manager CIK."""
        return await self.thirteenf_processor.get_13f_filings(
            cik=cik,
            days_back=days_back,
            use_cache=use_cache,
        )

    async def get_latest_13f_holdings(
        self,
        cik: str,
        days_back: int = 365*3,
        use_cache: bool = True,
        row_limit: Optional[int] = None,
    ) -> Dict:
        """Fetches and parses the latest Form 13F-HR information table for an institutional manager CIK."""
        return await self.thirteenf_processor.get_latest_13f_holdings(
            cik=cik,
            days_back=days_back,
            use_cache=use_cache,
            row_limit=row_limit,
        )

    # --- DELEGATED PROCESSOR METHODS ---

    async def get_recent_insider_transactions(self, ticker: str, days_back: int = 90,
                                             use_cache: bool = True, filing_limit: int = 10) -> List[Dict]:
        """Fetches, parses, and formats recent Form 4 transactions for UI display (Delegated)."""
        return await self.form4_processor.get_recent_insider_transactions(
            ticker=ticker,
            days_back=days_back,
            use_cache=use_cache,
            filing_limit=filing_limit
        )

    async def analyze_insider_transactions(self, ticker: str, days_back: int = 90, use_cache: bool = True) -> Dict:
        """Performs basic analysis on recent insider transactions (Delegated)."""
        return await self.form4_processor.analyze_insider_transactions(
            ticker=ticker,
            days_back=days_back,
            use_cache=use_cache
        )

    async def get_insider_signal_payload(self, ticker: str, days_back: int = 180,
                                         use_cache: bool = True, filing_limit: int = 40,
                                         anchor_type: str = 'filing_date') -> Dict:
        """Builds canonical insider events, daily aggregates, and LLM digest payload."""
        return await self.form4_processor.get_insider_signal_payload(
            ticker=ticker,
            days_back=days_back,
            use_cache=use_cache,
            filing_limit=filing_limit,
            anchor_type=anchor_type,
        )

    async def refresh_insider_signal_payload(self, ticker: str, days_back: int = 180,
                                             filing_limit: int = 40,
                                             anchor_type: str = 'filing_date') -> Dict:
        """Force a live Form 4 metadata check and merge only new accessions into the cached payload."""
        return await self.form4_processor.refresh_insider_signal_payload(
            ticker=ticker,
            days_back=days_back,
            filing_limit=filing_limit,
            anchor_type=anchor_type,
        )

    async def get_financial_summary(self, ticker: str, use_cache: bool = True) -> Optional[Dict]:
        """Generates a flattened summary of key financial metrics for the UI (Delegated)."""
        summary = await self.financial_processor.get_financial_summary(ticker=ticker, use_cache=use_cache)

        if summary:
            # Persist to SQL for faster future access and querying
            await self.sql_manager.initialize_db() # Ensure DB is ready (idempotent)
            await self.sql_manager.save_financial_history(ticker, summary)

        return summary

    async def get_financial_trend(self, ticker: str, periods: int = 8, use_cache: bool = True) -> Optional[Dict]:
        """Returns the financial summary decorated with QoQ/YoY pct changes per metric."""
        return await self.financial_processor.get_financial_trend(
            ticker=ticker, periods=periods, use_cache=use_cache
        )

    async def get_financial_series(
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
        split_events: List[Tuple[str, float]] | None = None,
    ) -> Optional[Dict]:
        """Return a persisted, point-in-time-safe canonical financial series."""
        return await self.financial_series.get_series(
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

    def list_supported_financial_metrics(self) -> List[Dict]:
        return self.financial_series.supported_metrics()

    async def get_valuation_series(
        self,
        ticker: str,
        *,
        price_observations: List[Dict[str, Any]],
        metric: str = "trailing_pe",
        split_events: List[Tuple[str, float]] | None = None,
        price_source: str = "caller",
        price_basis: str = "split_adjusted",
        refresh: bool = False,
        include_provenance: bool = True,
    ) -> Optional[Dict]:
        """Return a point-in-time trailing multiple on a caller-supplied price timeline."""
        return await self.financial_series.get_valuation_series(
            ticker,
            price_observations=price_observations,
            metric=metric,
            split_events=split_events,
            price_source=price_source,
            price_basis=price_basis,
            refresh=refresh,
            include_provenance=include_provenance,
        )

    async def get_8k_events(
        self,
        ticker: str,
        days_back: int = 180,
        use_cache: bool = True,
        filing_limit: int = 50,
        categories: Optional[List[str]] = None,
    ) -> Dict:
        """Returns parsed 8-K item-code events for a ticker."""
        return await self.form8k_processor.get_8k_events(
            ticker=ticker,
            days_back=days_back,
            use_cache=use_cache,
            filing_limit=filing_limit,
            categories=categories,
        )

    async def refresh_8k_events(
        self,
        ticker: str,
        days_back: int = 180,
        filing_limit: int = 50,
        categories: Optional[List[str]] = None,
    ) -> Dict:
        """Force a live 8-K metadata check and merge new accessions with cached events."""
        return await self.form8k_processor.refresh_8k_events(
            ticker=ticker,
            days_back=days_back,
            filing_limit=filing_limit,
            categories=categories,
        )

    async def get_planned_insider_sales(
        self,
        ticker: str,
        days_back: int = 90,
        use_cache: bool = True,
        filing_limit: int = 25,
    ) -> Dict:
        """Returns parsed Form 144 (planned insider sale) records for a ticker."""
        return await self.form144_processor.get_planned_insider_sales(
            ticker=ticker,
            days_back=days_back,
            use_cache=use_cache,
            filing_limit=filing_limit,
        )

    async def get_13f_holdings_changes(
        self,
        cik: str,
        days_back: int = 365 * 3,
        use_cache: bool = True,
        top_n: int = 25,
    ) -> Dict:
        """Returns QoQ deltas between the latest two 13F-HR filings for `cik`."""
        return await self.thirteenf_processor.get_holdings_changes(
            cik=cik, days_back=days_back, use_cache=use_cache, top_n=top_n,
        )

    async def get_supply_chain(self, ticker: str) -> List[Dict]:
        """
        Extracts supply chain relationships for a company from its latest 10-K.
        
        Orchestrates:
        1. Fetch 10-K filings list.
        2. Get primary HTML of the latest 10-K.
        3. Clean and Parse HTML.
        4. Save extracted edges to SQL.
        5. Return relationships.
        
        Args:
            ticker (str): Ticker symbol.
            
        Returns:
            List[Dict]: List of relationship dictionaries.
        """
        # 1. Get latest 10-K
        filings = await self.fetch_annual_reports(ticker, days_back=365*2)
        if not filings:
            logging.warning(f"No recent 10-K found for {ticker} to extract supply chain.")
            return []
            
        latest_10k = filings[0]
        accession_no = latest_10k['accession_no']
        doc_date = latest_10k['filing_date']
        
        logging.info(f"Extracting supply chain for {ticker} from 10-K ({doc_date})...")
        
        # 2. Fetch Raw HTML
        raw_html = await self.document_handler.fetch_primary_html(accession_no, ticker)
        if not raw_html:
            logging.error(f"Failed to fetch HTML content for 10-K {accession_no}")
            return []
            
        # 3. Parse
        clean_text = self.supply_chain_parser.clean_html(raw_html)
        sections = self.supply_chain_parser.extract_sections(clean_text)
        relationships = self.supply_chain_parser.extract_relationships(sections)
        
        logging.info(f"Extracted {len(relationships)} relationships for {ticker}.")
        
        # 4. Save to SQL
        await self.sql_manager.initialize_db()
        for rel in relationships:
            await self.sql_manager.save_relationship(
                source_ticker=ticker,
                target_entity=rel['target_entity'],
                relationship_type=rel['relationship_type'],
                weight=rel['weight'],
                context=rel['context'],
                confidence=rel['confidence_score'],
                doc_date=doc_date
            )
            
        return relationships

    # === OTHER PUBLIC METHODS ===

    async def fetch_multiple_tickers(self, tickers: List[str], form_type: str = "4", days_back: int = 90, use_cache: bool = True) -> Dict[str, List]:
        """ Fetches filings for multiple tickers concurrently."""
        logging.info(f"Fetching {form_type} filings for {len(tickers)} tickers (days back: {days_back}, cache: {use_cache})...")
        # Use asyncio.gather to run fetches in parallel
        tasks = [self.get_filings_by_form(ticker, form_type, days_back, use_cache) for ticker in tickers]
        results_list = await asyncio.gather(*tasks, return_exceptions=True) # Capture exceptions

        results = {}
        for i, ticker in enumerate(tickers):
            if isinstance(results_list[i], Exception):
                logging.error(f"Error fetching {form_type} data for {ticker}: {results_list[i]}")
                results[ticker] = [] # Return empty list on error
            else:
                results[ticker] = results_list[i]
                logging.debug(f"Found {len(results[ticker])} {form_type} filings for {ticker}")

        logging.info(f"Finished fetching filings for {len(tickers)} tickers.")
        return results

    async def close(self):
        """Closes resources like the HTTP client session."""
        if hasattr(self, 'http_client') and self.http_client:
             await self.http_client.close()
        else:
             logging.warning("HttpClient not initialized, cannot close session.")
        # Close other resources if needed
