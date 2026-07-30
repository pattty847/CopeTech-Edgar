from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, Mock

from copetech_sec import EdgarClient


class EdgarClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fetcher = Mock()
        self.fetcher.get_company_info = AsyncMock(return_value={"ticker": "AAPL"})
        self.fetcher.get_filings_page = AsyncMock(
            return_value={"items": [], "metadata": {}}
        )
        self.fetcher.get_insider_signal_payload = AsyncMock(
            return_value={"symbol": "AAPL"}
        )
        self.fetcher.get_latest_13f_holdings = AsyncMock(
            return_value={"manager": "Fixture"}
        )
        self.fetcher.get_financial_series = AsyncMock(
            return_value={"metric": "diluted_eps"}
        )
        self.fetcher.list_supported_financial_metrics = Mock(
            return_value=[{"id": "revenue"}]
        )
        self.fetcher.close = AsyncMock()
        self.client = EdgarClient(_fetcher=self.fetcher)

    async def test_exposes_stable_resource_namespaces(self):
        company = await self.client.companies.get("AAPL")
        filings = await self.client.filings.query("AAPL", ["10-K", "10-Q"])
        signals = await self.client.ownership.signals("AAPL")
        holdings = await self.client.institutions.latest_holdings("0000320193")
        financials = await self.client.financials.series(
            "AAPL",
            metric="diluted_eps",
        )

        self.assertEqual(company["ticker"], "AAPL")
        self.assertEqual(filings["items"], [])
        self.assertEqual(signals["symbol"], "AAPL")
        self.assertEqual(holdings["manager"], "Fixture")
        self.assertEqual(financials["metric"], "diluted_eps")
        self.assertEqual(self.client.financials.metrics(), [{"id": "revenue"}])

    async def test_async_context_manager_closes_underlying_fetcher(self):
        async with self.client as entered:
            self.assertIs(entered, self.client)

        self.fetcher.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
