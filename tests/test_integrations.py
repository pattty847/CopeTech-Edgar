from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, Mock

from copetech_sec.integrations.agents import EdgarAgentTools
from copetech_sec.financial_series_service import FinancialSeriesService
from copetech_sec.integrations.pandas import (
    financial_observations,
    institutional_holdings,
    ownership_entries,
)


class AgentIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = Mock()
        self.client.companies.get = AsyncMock(return_value={"ticker": "AAPL"})
        self.client.financials.series = AsyncMock(return_value=None)
        self.client.ownership.entries = AsyncMock(
            return_value={"symbol": "AAPL", "entries": []}
        )
        self.tools = EdgarAgentTools(self.client)

    async def test_manifest_has_stable_read_only_ids_and_schemas(self):
        manifest = self.tools.manifest()

        self.assertEqual(
            [tool["id"] for tool in manifest],
            [
                "edgar.company.get",
                "edgar.filings.query",
                "edgar.ownership.entries",
                "edgar.financials.series",
                "edgar.financials.concept",
                "edgar.financials.frame",
                "edgar.institutions.latest_holdings",
            ],
        )
        self.assertTrue(all(tool["sideEffect"] == "read_only" for tool in manifest))
        self.assertTrue(all(tool["inputSchema"]["additionalProperties"] is False for tool in manifest))

        financial_tool = next(
            tool for tool in manifest if tool["id"] == "edgar.financials.series"
        )
        metric_ids = financial_tool["inputSchema"]["properties"]["metric"]["enum"]
        expected = [entry["id"] for entry in FinancialSeriesService.supported_metrics()]
        self.assertEqual(metric_ids, expected)
        self.assertEqual(len(metric_ids), len(set(metric_ids)))
        self.assertIn("roic", metric_ids)

    async def test_invoke_distinguishes_not_found_from_failure(self):
        result = await self.tools.invoke(
            "edgar.financials.series",
            {"ticker": "NOPE", "metric": "revenue"},
        )

        self.assertEqual(result.status, "not_found")
        self.assertIsNone(result.data)

        self.client.companies.get.side_effect = RuntimeError("transport failed")
        with self.assertRaisesRegex(RuntimeError, "transport failed"):
            await self.tools.invoke("edgar.company.get", {"ticker": "AAPL"})

    async def test_unknown_tool_id_is_rejected(self):
        with self.assertRaises(KeyError):
            await self.tools.invoke("edgar.nope", {})


class PandasIntegrationTests(unittest.TestCase):
    def test_financial_frame_preserves_sources_and_metadata(self):
        frame = financial_observations(
            {
                "symbol": "AAPL",
                "metric": "diluted_eps",
                "frequency": "ttm",
                "observations": [
                    {
                        "periodEnd": "2026-06-30",
                        "value": 7.25,
                        "sources": [{"accessionNumber": "0000000000-26-000001"}],
                    }
                ],
            }
        )

        self.assertEqual(frame.loc[0, "value"], 7.25)
        self.assertEqual(
            frame.loc[0, "sources"][0]["accessionNumber"],
            "0000000000-26-000001",
        )
        self.assertEqual(frame.attrs["copetech_metadata"]["metric"], "diluted_eps")

    def test_ownership_and_holdings_frames_attach_context(self):
        entries = ownership_entries(
            {
                "symbol": "AAPL",
                "forms": ["3", "5"],
                "entries": [{"is_holding": True, "shares": 100}],
                "metadata": {"warnings": []},
            }
        )
        holdings = institutional_holdings(
            {
                "manager_cik": "0001067983",
                "manager_name": "Fixture Manager",
                "holdings": [{"cusip": "037833100", "value_usd": 1000}],
            }
        )

        self.assertTrue(entries.loc[0, "is_holding"])
        self.assertEqual(entries.attrs["copetech_metadata"]["forms"], ["3", "5"])
        self.assertEqual(holdings.loc[0, "value_usd"], 1000)
        self.assertEqual(
            holdings.attrs["copetech_metadata"]["manager_name"],
            "Fixture Manager",
        )
