from __future__ import annotations

import tempfile
import unittest

from copetech_sec.cache_manager import SecCacheManager
from copetech_sec.resources.xbrl import XbrlResource


class FakeHttp:
    def __init__(self):
        self.calls: list[str] = []

    async def make_request(self, url: str, *, is_json: bool):
        self.calls.append(url)
        return {"url": url, "facts": []}


class FakeFetcher:
    def __init__(self, cache_dir: str):
        self.http_client = FakeHttp()
        self.cache_manager = SecCacheManager(cache_dir)

    async def get_cik_for_ticker(self, ticker: str):
        return "0000320193" if ticker == "AAPL" else None


class XbrlResourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_company_concept_uses_bounded_endpoint_and_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = FakeFetcher(tmpdir)
            resource = XbrlResource(fetcher)
            first = await resource.company_concept(
                "AAPL",
                taxonomy="us-gaap",
                concept="RevenueFromContractWithCustomerExcludingAssessedTax",
            )
            second = await resource.company_concept(
                "AAPL",
                taxonomy="us-gaap",
                concept="RevenueFromContractWithCustomerExcludingAssessedTax",
            )

        self.assertEqual(first, second)
        self.assertEqual(len(fetcher.http_client.calls), 1)
        self.assertIn(
            "/companyconcept/CIK0000320193/us-gaap/"
            "RevenueFromContractWithCustomerExcludingAssessedTax.json",
            fetcher.http_client.calls[0],
        )

    async def test_frame_normalizes_period_and_supports_per_units(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = FakeFetcher(tmpdir)
            resource = XbrlResource(fetcher)
            payload = await resource.frame(
                taxonomy="us-gaap",
                concept="EarningsPerShareDiluted",
                unit="USD-per-shares",
                period="cy2025q4",
            )

        self.assertIn(
            "/frames/us-gaap/EarningsPerShareDiluted/"
            "USD-per-shares/CY2025Q4.json",
            payload["url"],
        )

    async def test_rejects_path_injection_before_request(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = FakeFetcher(tmpdir)
            resource = XbrlResource(fetcher)
            with self.assertRaises(ValueError):
                await resource.frame(
                    taxonomy="../us-gaap",
                    concept="Assets",
                    unit="USD",
                    period="CY2025Q4I",
                )

        self.assertEqual(fetcher.http_client.calls, [])

    async def test_unknown_ticker_returns_none_without_request(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = FakeFetcher(tmpdir)
            resource = XbrlResource(fetcher)
            payload = await resource.company_concept(
                "NOPE",
                taxonomy="us-gaap",
                concept="Assets",
            )

        self.assertIsNone(payload)
        self.assertEqual(fetcher.http_client.calls, [])
