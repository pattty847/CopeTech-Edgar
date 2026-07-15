import os
import json
import logging
import glob
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict, List, Any

class SecCacheManager:
    """
    Manages caching of SEC data to the local filesystem to reduce redundant API calls.

    This class handles the storage and retrieval of various types of SEC data
    (like company submissions, filing lists, company facts, CIK mappings) in a
    structured directory layout within a specified base cache directory.

    Key responsibilities:
    - Organizing cached data into subdirectories based on data type.
    - Generating timestamped filenames for ticker-specific data to manage versions.
    - Providing methods to save data (as JSON) to appropriate cache files.
    - Providing methods to load the most recent and relevant cached data.
    - Implementing basic freshness checks (e.g., checking if a cache file is from today).
    - Handling file I/O and JSON serialization/deserialization.

    It is instantiated by `SECDataFetcher` and used internally to check for cached
    data before making live API requests.
    """

    # Define cache subdirectories. `company_info` is stored under the `submissions`
    # subdir but still needs an entry here so _get_cache_path's allow-list passes.
    SUBDIRS = {
        "mappings": "mappings",
        "submissions": "submissions",
        "forms": "forms",
        "facts": "facts",
        "reports": "reports", # Kept for potential future use, even if deprecated
        "company_info": "submissions",
        "insider_signals": "forms",
    }

    # Raw filings are immutable: a filed document never changes (amendments are NEW
    # filings with their own accession numbers), so each is downloaded from the SEC at
    # most once, kept forever, and keyed by its globally unique accession number —
    # ticker mappings can change, accessions can't. Flat layout is fine at this scale;
    # shard by CIK/year if this ever holds hundreds of thousands of files.
    RAW_FILINGS_SUBDIR = os.path.join("raw", "filings")

    def __init__(self, cache_dir: str = "data/edgar"):
        """
        Initializes the SEC Cache Manager.

        Creates the base cache directory and all necessary subdirectories if they
        do not already exist.

        Args:
            cache_dir (str, optional): The root directory path where all SEC cache
                files will be stored. Defaults to \"data/edgar\" relative to the
                project root.
        """
        self.cache_dir = cache_dir
        self._ensure_directories()

    def _ensure_directories(self):
        """
        Internal helper to create the base cache directory and all defined subdirectories.

        This is called during initialization to ensure the cache structure is ready.
        Uses `os.makedirs` with `exist_ok=True` to avoid errors if directories already exist.
        """
        logging.debug(f"Ensuring cache directory exists: {self.cache_dir}")
        os.makedirs(self.cache_dir, exist_ok=True)
        for subdir in self.SUBDIRS.values():
            path = os.path.join(self.cache_dir, subdir)
            logging.debug(f"Ensuring cache subdirectory exists: {path}")
            os.makedirs(path, exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, self.RAW_FILINGS_SUBDIR), exist_ok=True)

    def _get_cache_path(self, data_type: str, ticker: Optional[str] = None, **kwargs) -> str:
        """
        Constructs the full path for a cache file based on data type and parameters.

        This centralizes the logic for naming and locating cache files within the
        structured directory layout.

        Args:
            data_type (str): The category of data being cached (e.g., 'submissions',
                'forms', 'facts', 'mappings', 'company_info'). Must be a key in `SUBDIRS`.
            ticker (Optional[str], optional): The stock ticker symbol. Required for most
                `data_type` values (except 'mappings'). Defaults to None.
            **kwargs: Additional keyword arguments used for specific data types:
                - map_type (str): Used when `data_type` is 'mappings' (e.g., 'ticker_cik').
                - form_type (str): Required when `data_type` is 'forms'.

        Returns:
            str: The absolute or relative path to the intended cache file.

        Raises:
            ValueError: If `data_type` is invalid, or if `ticker` or `form_type`
                are required but not provided.
        """
        subdir = self.SUBDIRS.get(data_type)
        if not subdir:
            raise ValueError(f"Invalid data_type for caching: {data_type}")

        # --- CIK Mapping --- 
        if data_type == "mappings" and kwargs.get("map_type") == "ticker_cik":
             return os.path.join(self.cache_dir, subdir, "ticker_cik_map.json")

        # --- Ticker-specific data --- 
        if not ticker:
            raise ValueError(f"Ticker is required for data_type: {data_type}")
        ticker_upper = ticker.upper()

        timestamp = datetime.now().strftime("%Y%m%d") # Daily timestamp by default

        if data_type == "submissions":
            filename = f"{ticker_upper}_submissions_{timestamp}.json"
        elif data_type == "forms":
            form_type = kwargs.get("form_type")
            if not form_type: raise ValueError("form_type is required for 'forms' data_type")
            safe_form_type = self._safe_cache_segment(str(form_type))
            days_back = kwargs.get("days_back")
            window = f"_{int(days_back)}d" if days_back is not None else ""
            filename = f"{ticker_upper}_{safe_form_type}{window}_{timestamp}.json"
        elif data_type == "facts":
            # Facts can update more often, add time to timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{ticker_upper}_facts_{timestamp}.json"
        elif data_type == "company_info": # Using submissions subdir for info for now
            # Treat company info like submissions (daily cache)
             subdir = self.SUBDIRS["submissions"]
             filename = f"{ticker_upper}_info_{timestamp}.json"
        elif data_type == "insider_signals":
            # Deliberately NOT date-stamped: validity is decided by the fingerprint
            # stored inside the payload (parser version + source accessions + config),
            # so there is exactly one file per key, replaced atomically. Date-stamped
            # payload files were an unbounded disk leak.
            days_back = int(kwargs.get("days_back") or 180)
            filing_limit = int(kwargs.get("filing_limit") or 40)
            anchor_type = self._safe_cache_segment(str(kwargs.get("anchor_type") or "filing_date"))
            filename = f"{ticker_upper}_insider_signals_{days_back}d_{filing_limit}_{anchor_type}.json"
        else:
            # Default pattern if needed, though specific types are preferred
            filename = f"{ticker_upper}_{data_type}_{timestamp}.json"

        return os.path.join(self.cache_dir, subdir, filename)

    @staticmethod
    def _safe_cache_segment(value: str) -> str:
        value = value.replace("/", "")
        segment = re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")
        segment = re.sub(r"_+", "_", segment)
        return segment or "default"

    def _matching_cache_files(self, data_type: str, ticker: str, **kwargs) -> List[str]:
        """
        All cache files matching the naming convention for (data_type, ticker, params),
        sorted by modification time, most recent first. Shared by lookup and pruning.
        """
        subdir = self.SUBDIRS.get(data_type)
        if not subdir: return []
        ticker_upper = ticker.upper()

        if data_type == "submissions":
            pattern = f"{ticker_upper}_submissions_*.json"
        elif data_type == "forms":
            form_type = kwargs.get("form_type")
            if not form_type: return []
            safe_form_type = self._safe_cache_segment(str(form_type))
            days_back = kwargs.get("days_back")
            if days_back is not None:
                pattern = f"{ticker_upper}_{safe_form_type}_{int(days_back)}d_*.json"
            else:
                pattern = f"{ticker_upper}_{safe_form_type}_*.json"
        elif data_type == "facts":
            pattern = f"{ticker_upper}_facts_*.json"
        elif data_type == "company_info": # Using submissions subdir for info
            subdir = self.SUBDIRS["submissions"]
            pattern = f"{ticker_upper}_info_*.json"
        elif data_type == "insider_signals":
            days_back = int(kwargs.get("days_back") or 180)
            filing_limit = int(kwargs.get("filing_limit") or 40)
            anchor_type = self._safe_cache_segment(str(kwargs.get("anchor_type") or "filing_date"))
            # No trailing underscore: matches both the current fixed filename and the
            # legacy date-stamped ones (so old files are found once, then pruned).
            pattern = f"{ticker_upper}_insider_signals_{days_back}d_{filing_limit}_{anchor_type}*.json"
        else:
            return [] # Pattern not defined for this type

        search_path = os.path.join(self.cache_dir, subdir, pattern)
        try:
            cache_files = sorted(glob.glob(search_path), key=os.path.getmtime, reverse=True)
            if data_type == "forms" and kwargs.get("days_back") is None:
                window_re = re.compile(rf"^{re.escape(ticker_upper)}_{re.escape(self._safe_cache_segment(str(kwargs.get('form_type'))))}_\d+d_")
                cache_files = [path for path in cache_files if not window_re.match(os.path.basename(path))]
            return cache_files
        except Exception as e:
            logging.warning(f"Error searching for cache files ({search_path}): {e}")
            return []

    def _find_latest_cache_file(self, data_type: str, ticker: str, **kwargs) -> Optional[str]:
        """The most recently modified cache file for (data_type, ticker, params), or None."""
        cache_files = self._matching_cache_files(data_type, ticker, **kwargs)
        if cache_files:
            logging.debug(f"Found latest cache file for {ticker} ({data_type}): {cache_files[0]}")
            return cache_files[0]
        logging.debug(f"No cache files found for {ticker} ({data_type})")
        return None

    def _prune_superseded_files(self, keep_path: str, data_type: str, ticker: str, **kwargs) -> None:
        """
        Deletes every cache file for this key except `keep_path`. Raw filings are never
        touched (they live outside SUBDIRS); this only reaps superseded derived
        snapshots — the date-stamped payload files that used to accumulate forever.
        """
        for path in self._matching_cache_files(data_type, ticker, **kwargs):
            if os.path.abspath(path) == os.path.abspath(keep_path):
                continue
            try:
                os.remove(path)
                logging.debug(f"Pruned superseded cache file: {path}")
            except OSError as e:
                logging.warning(f"Could not prune cache file {path}: {e}")

    def _is_cache_fresh(self, cache_file: str, data_type: str) -> bool:
        """
        Determines if a given cache file is considered fresh based on its type and timestamp.

        The current logic considers cache files for 'submissions', 'forms', and
        'company_info' fresh if their filename contains today's date (YYYYMMDD format).
        Other types (like 'facts' which include time in the timestamp, or 'mappings')
        are currently considered fresh whenever found by `_find_latest_cache_file`.
        This logic could be expanded for more sophisticated TTL strategies.

        Args:
            cache_file (str): The full path to the cache file.
            data_type (str): The category of data the file represents.

        Returns:
            bool: True if the cache file is considered fresh, False otherwise.
        """
        # Simple check: if cache is from today for submissions/forms/info
        if data_type in ["submissions", "forms", "company_info"]:
            today = datetime.now().strftime("%Y%m%d")
            is_fresh = today in os.path.basename(cache_file)
            logging.debug(f"Checking freshness for {cache_file} ({data_type}): {'Fresh' if is_fresh else 'Stale'}")
            return is_fresh
        return True # Assume other types are always fresh if found by _find_latest

    def _read_cache_file(self, file_path: str) -> Optional[Any]:
        """
        Reads and parses JSON content from a specified cache file.

        Handles file opening, reading, and JSON decoding. Logs warnings and returns
        None if the file doesn't exist, is unreadable, or contains invalid JSON.

        Args:
            file_path (str): The full path to the JSON cache file.

        Returns:
            Optional[Any]: The parsed data from the JSON file (usually dict or list),
                or None if reading or parsing fails.
        """
        if not os.path.exists(file_path):
            return None
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logging.debug(f"Successfully read cache file: {file_path}")
                return data
        except json.JSONDecodeError as e:
            logging.warning(f"JSON decode error reading cache file {file_path}: {e}")
            return None
        except Exception as e:
            logging.warning(f"Error reading cache file {file_path}: {e}")
            return None

    def _write_cache_file(self, file_path: str, data: Any) -> bool:
        """
        Writes Python data structures to a specified file path as JSON.

        Ensures the target directory exists, then opens the file and dumps the
        provided data using `json.dump` with indentation for readability.
        Logs errors if writing fails.

        Args:
            file_path (str): The full path to the target cache file.
            data (Any): The Python object (e.g., dict, list) to serialize and save.
                Must be JSON serializable.

        Returns:
            bool: True if the file was written successfully, False otherwise.
        """
        try:
            # Ensure the directory exists before writing
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # Atomic write: an interrupted dump must never leave a truncated file that
            # then reads as a valid (empty/partial) cache forever.
            tmp_path = f"{file_path}.tmp"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, file_path)
            logging.info(f"Successfully wrote cache file: {file_path}")
            return True
        except Exception as e:
            logging.error(f"Error writing cache file {file_path}: {e}")
            return False

    # --- Public Caching Methods --- 

    # CIK Mapping Specific
    async def load_cik(self, ticker: str) -> Optional[str]:
        """
        Loads the CIK for a specific ticker from the cached Ticker-CIK map.

        Reads the central `ticker_cik_map.json` file and looks up the CIK
        for the given ticker (case-insensitive).

        Args:
            ticker (str): The stock ticker symbol.

        Returns:
            Optional[str]: The 10-digit CIK string if found in the cache, otherwise None.
        """
        map_file = self._get_cache_path("mappings", map_type="ticker_cik")
        cik_map = self._read_cache_file(map_file)
        if isinstance(cik_map, dict):
            cik = cik_map.get(ticker.upper())
            if cik:
                 logging.debug(f"CIK found in cache for {ticker}: {cik}")
                 return cik
        logging.debug(f"CIK not found in cache for {ticker}")
        return None

    async def save_cik_map(self, cik_map: Dict[str, str]) -> None:
        """
        Saves the entire Ticker-CIK mapping dictionary to the cache file.

        Overwrites the existing `ticker_cik_map.json` with the provided dictionary.
        This is typically called after fetching the fresh map from the SEC.

        Args:
            cik_map (Dict[str, str]): A dictionary mapping uppercase ticker symbols
                to their 10-digit CIK strings.
        """
        map_file = self._get_cache_path("mappings", map_type="ticker_cik")
        success = self._write_cache_file(map_file, cik_map)
        if not success:
             logging.error("Failed to save Ticker-CIK map to cache.")

    # Generic Load/Save for Ticker-Specific Data
    async def load_data(self, ticker: str, data_type: str, **kwargs) -> Optional[Any]:
        """
        Loads the most recent, fresh cached data for a given ticker and data type.

        This is the primary public method for retrieving cached data. It uses internal
        helpers to find the latest relevant cache file (`_find_latest_cache_file`)
        and checks if it's considered fresh (`_is_cache_fresh`). If both conditions
        are met, it reads and returns the data (`_read_cache_file`).

        Args:
            ticker (str): The stock ticker symbol (case-insensitive).
            data_type (str): The category of data to load (e.g., 'submissions', 'forms',
                'facts', 'company_info').
            **kwargs: Additional parameters needed for specific data types, passed down
                to helper methods (e.g., `form_type` for 'forms').

        Returns:
            Optional[Any]: The cached data (typically dict or list) if found and deemed
                fresh, otherwise None.
        """
        latest_file = self._find_latest_cache_file(data_type, ticker, **kwargs)
        if latest_file and self._is_cache_fresh(latest_file, data_type):
            return self._read_cache_file(latest_file)
        if data_type == "forms" and kwargs.get("days_back") is not None:
            legacy_kwargs = dict(kwargs)
            legacy_kwargs.pop("days_back", None)
            legacy_file = self._find_latest_cache_file(data_type, ticker, **legacy_kwargs)
            if legacy_file and self._is_cache_fresh(legacy_file, data_type):
                return self._read_cache_file(legacy_file)
        else:
            if latest_file:
                 logging.debug(f"Cache file found ({latest_file}) but considered stale.")
            else:
                 logging.debug(f"No cache file found for {ticker} ({data_type} {' '.join(f'{k}={v}' for k,v in kwargs.items())})")
        return None

    # ---- immutable raw filing store ----
    # Discovery is delta'd at the accession-index layer; documents themselves are
    # download-once. Derived payloads re-parse these local files, so parser fixes
    # cost zero SEC traffic.

    def raw_filing_path(self, accession_no: str) -> Optional[str]:
        """Canonical on-disk path for a filing document. Accession numbers are globally
        unique; dashes are stripped so both '0000000000-26-000001' and its dashless form
        map to the same file."""
        accession = str(accession_no).strip().replace("-", "")
        if not accession.isdigit():
            return None
        return os.path.join(self.cache_dir, self.RAW_FILINGS_SUBDIR, f"{accession}.xml")

    def load_raw_filing(self, accession_no: str) -> Optional[str]:
        """Returns the stored document content, or None if this accession was never cached."""
        path = self.raw_filing_path(accession_no)
        if not path:
            return None
        try:
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    return f.read()
        except Exception as e:
            logging.warning(f"Error reading raw filing {accession_no}: {e}")
        return None

    def save_raw_filing(self, accession_no: str, content: str) -> bool:
        """Stores a filing document permanently. Validates that the body parses as XML
        first — an SEC throttle/error page must never be cached as a filing — and writes
        atomically so an interrupted download can't leave a truncated file that looks
        cached forever."""
        path = self.raw_filing_path(accession_no)
        if not path or not content:
            return False
        try:
            ET.fromstring(content)
        except ET.ParseError:
            logging.warning(f"Refusing to cache non-XML content for accession {accession_no}")
            return False
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp_path = f"{path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp_path, path)
            return True
        except Exception as e:
            logging.error(f"Error writing raw filing {accession_no}: {e}")
            return False

    async def save_data(self, ticker: str, data_type: str, data: Any, **kwargs) -> None:
        """
        Saves data to a new, timestamped cache file for a specific ticker and data type.

        This is the primary public method for storing fetched data. It determines the
        correct filename and path using `_get_cache_path` (which includes a timestamp)
        and writes the data using `_write_cache_file`.

        Args:
            ticker (str): The stock ticker symbol (used in the filename).
            data_type (str): The category of data being saved (e.g., 'submissions',
                'forms', 'facts', 'company_info').
            data (Any): The Python object (e.g., dict, list) to be saved as JSON.
            **kwargs: Additional parameters needed for specific data types, passed down
                to helper methods (e.g., `form_type` for 'forms'). (Note: 'cik' is ignored here).
        """
        # CIK might be passed in kwargs but isn't used for path determination here
        kwargs.pop('cik', None)

        cache_file = self._get_cache_path(data_type, ticker, **kwargs)
        success = self._write_cache_file(cache_file, data)
        if not success:
            logging.error(f"Failed to save {data_type} data for {ticker} to cache.")
            return
        # Superseded snapshots are dead weight: fingerprint-validated payloads use one
        # fixed filename per key, and day-stamped metadata/index files only ever need
        # their newest copy (freshness reads the newest file's date). Without this the
        # dated files accumulate forever — one per ticker per window per day.
        if data_type in ("insider_signals", "forms", "submissions", "company_info"):
            self._prune_superseded_files(cache_file, data_type, ticker, **kwargs)

    # Specific Load/Save methods (kept for compatibility during refactor, but delegate)
    async def _load_company_info_from_cache(self, ticker: str) -> Optional[Dict]:
        """DEPRECATED internal helper. Use `load_data` directly."""
        return await self.load_data(ticker, "company_info")

    async def _save_company_info_to_cache(self, ticker: str, cik: str, data: Dict) -> None:
        """DEPRECATED internal helper. Use `save_data` directly."""
        await self.save_data(ticker, "company_info", data, cik=cik)

    async def _load_submissions_from_cache(self, ticker: str) -> Optional[Dict]:
        """DEPRECATED internal helper. Use `load_data` directly."""
        return await self.load_data(ticker, "submissions")

    async def _save_submissions_to_cache(self, ticker: str, cik: str, data: Dict) -> None:
        """DEPRECATED internal helper. Use `save_data` directly."""
        await self.save_data(ticker, "submissions", data, cik=cik)

    async def _load_filings_from_cache(self, ticker: str, form_type: str) -> List[Dict]:
        """DEPRECATED internal helper. Use `load_data` directly."""
        result = await self.load_data(ticker, "forms", form_type=form_type)
        return result if isinstance(result, list) else [] # Ensure list return type

    async def _save_filings_to_cache(self, ticker: str, form_type: str, data: List[Dict]) -> None:
        """DEPRECATED internal helper. Use `save_data` directly."""
        await self.save_data(ticker, "forms", data, form_type=form_type)

    async def _load_company_facts_from_cache(self, ticker: str) -> Optional[Dict]:
        """DEPRECATED internal helper. Use `load_data` directly."""
        return await self.load_data(ticker, "facts")

    async def _save_company_facts_to_cache(self, ticker: str, cik: str, data: Dict) -> None:
        """DEPRECATED internal helper. Use `save_data` directly."""
        await self.save_data(ticker, "facts", data, cik=cik)
