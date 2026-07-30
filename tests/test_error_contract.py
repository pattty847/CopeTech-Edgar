import tempfile
import unittest

from copetech_sec.errors import SecNotFoundError, SecTransportError
from copetech_sec.sec_api import SECDataFetcher


class EmptyCache:
    async def load_data(self, *_args, **_kwargs):
        return None

    async def save_data(self, *_args, **_kwargs):
        return None


class RaisingHttp:
    def __init__(self, error):
        self.error = error

    async def make_request(self, *_args, **_kwargs):
        raise self.error


class ResourceErrorTranslationTests(unittest.IsolatedAsyncioTestCase):
    def _fetcher(self, error):
        fetcher = SECDataFetcher.__new__(SECDataFetcher)
        fetcher.cache_manager = EmptyCache()
        fetcher.http_client = RaisingHttp(error)

        async def get_cik(_ticker):
            return "0000320193"

        fetcher.get_cik_for_ticker = get_cik
        return fetcher

    async def test_company_facts_maps_real_404_to_absence(self):
        fetcher = self._fetcher(
            SecNotFoundError(
                "not found",
                url="https://data.sec.gov/facts",
                status_code=404,
            )
        )

        self.assertIsNone(await fetcher.get_company_facts("AAPL", use_cache=False))

    async def test_company_facts_preserves_transport_failure(self):
        fetcher = self._fetcher(
            SecTransportError(
                "timeout",
                url="https://data.sec.gov/facts",
                retryable=True,
            )
        )

        with self.assertRaises(SecTransportError):
            await fetcher.get_company_facts("AAPL", use_cache=False)
