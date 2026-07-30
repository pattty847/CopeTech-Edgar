from __future__ import annotations

import tempfile
import unittest
from unittest.mock import AsyncMock

from copetech_sec.sec_api import SECDataFetcher


class CompanyIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def test_company_info_exposes_all_share_classes_and_former_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = SECDataFetcher(cache_dir=tmpdir)
            fetcher.get_cik_for_ticker = AsyncMock(return_value="0001652044")
            fetcher.http_client.make_request = AsyncMock(
                return_value={
                    "name": "Alphabet Inc.",
                    "sic": "7370",
                    "sicDescription": "Services",
                    "addresses": {"mailing": {"city": "Mountain View"}},
                    "phone": "650-000-0000",
                    "tickers": ["GOOGL", "GOOG"],
                    "exchanges": ["Nasdaq", "Nasdaq"],
                    "formerNames": [
                        {
                            "name": "Google Inc.",
                            "from": "2004-08-18",
                            "to": "2015-10-02",
                        }
                    ],
                }
            )

            info = await fetcher.get_company_info("GOOG", use_cache=False)
            await fetcher.close()

        self.assertEqual(info["ticker"], "GOOG")
        self.assertEqual(info["tickers"], ["GOOGL", "GOOG"])
        self.assertEqual(
            info["share_classes"],
            [
                {"ticker": "GOOGL", "exchange": "Nasdaq"},
                {"ticker": "GOOG", "exchange": "Nasdaq"},
            ],
        )
        self.assertEqual(info["former_names"][0]["name"], "Google Inc.")

    async def test_ticker_map_keeps_two_symbols_for_one_cik(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = SECDataFetcher(cache_dir=tmpdir)
            fetcher.http_client.make_request = AsyncMock(
                return_value={
                    "0": {
                        "ticker": "GOOGL",
                        "cik_str": 1652044,
                        "title": "Alphabet Inc.",
                    },
                    "1": {
                        "ticker": "GOOG",
                        "cik_str": 1652044,
                        "title": "Alphabet Inc.",
                    },
                }
            )

            await fetcher._fetch_and_cache_cik_map()
            googl = await fetcher.cache_manager.load_cik("GOOGL")
            goog = await fetcher.cache_manager.load_cik("GOOG")
            await fetcher.close()

        self.assertEqual(googl, "0001652044")
        self.assertEqual(goog, "0001652044")
