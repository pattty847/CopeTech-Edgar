#!/usr/bin/env python3
"""FastAPI route tests with an injected fake SECDataFetcher and PriceCandleFetcher.

Covers auth (backend secret, demo key), rate-limit fallback, and the JSON shape
of every public route in `copetech_sec.app`.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
from fastapi.testclient import TestClient

from copetech_sec import app as app_module
from copetech_sec.app import (
    app,
    get_fetcher,
    get_price_fetcher,
)


VALID_HEADERS = {"x-backend-secret": "test-backend-secret", "x-demo-key": "test-demo-key"}


class FakeForm4Processor:
    """Stand-in for `Form4Processor` used by the /clusters route, which calls into it directly."""

    def __init__(self) -> None:
        self.cluster_calls: list[dict[str, Any]] = []

    def detect_cluster_buys(self, events, *, window_days: int = 14, min_unique_insiders: int = 3):
        self.cluster_calls.append(
            {
                "event_count": len(events),
                "window_days": window_days,
                "min_unique_insiders": min_unique_insiders,
            }
        )
        if len(events) >= min_unique_insiders:
            return [
                {
                    "window_start": "2026-03-01",
                    "window_end": "2026-03-10",
                    "unique_insiders": min_unique_insiders,
                    "event_count": len(events),
                    "total_value": 100_000.0,
                    "total_shares": 1_000.0,
                    "insiders": [{"owner_name": f"Insider{i}", "owner_role": "Officer", "gross_value": 25_000.0}
                                 for i in range(min_unique_insiders)],
                    "filing_urls": [],
                }
            ]
        return []


class FakeFetcher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.form4_processor = FakeForm4Processor()

    async def get_company_info(self, ticker: str) -> dict[str, Any] | None:
        self.calls.append(("company_info", {"ticker": ticker}))
        if ticker == "MISSING":
            return None
        return {"ticker": ticker, "cik": "0000320193", "name": "Apple Inc."}

    async def get_recent_insider_transactions(
        self, ticker: str, *, days_back: int, filing_limit: int
    ) -> list[dict[str, Any]]:
        self.calls.append(("transactions", {"ticker": ticker, "days_back": days_back, "filing_limit": filing_limit}))
        return [{"insider": "Tim Cook", "shares": 100.0, "value": 17500.0}]

    async def get_insider_signal_payload(
        self, ticker: str, *, days_back: int, filing_limit: int, anchor_type: str
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "insiders",
                {"ticker": ticker, "days_back": days_back, "filing_limit": filing_limit, "anchor_type": anchor_type},
            )
        )
        return {
            "symbol": ticker,
            "window": {"days_back": days_back, "filing_limit": filing_limit},
            "events": [{"id": 1}, {"id": 2}, {"id": 3}],
            "daily_aggregates": [],
            "llm_digest": {"summary": {}, "key_events": [], "anomalies": [], "caveats": []},
        }

    async def get_latest_13f_holdings(
        self, cik: str, *, days_back: int = 0, row_limit: int | None = None, **_: Any
    ) -> dict[str, Any]:
        self.calls.append(("13f", {"cik": cik, "days_back": days_back, "row_limit": row_limit}))
        if cik == "0000000404":
            return {"filing": None, "holdings_count": 0}
        return {
            "filing": {"accession_no": "0000000000-26-000001", "filing_date": "2026-04-30"},
            "holdings": [{"name_of_issuer": "ACME", "value_usd": 1000}],
            "holdings_count": 1,
        }

    async def get_financial_trend(
        self, ticker: str, *, periods: int = 8, use_cache: bool = True
    ) -> dict[str, Any] | None:
        self.calls.append(("trend", {"ticker": ticker, "periods": periods}))
        if ticker == "MISSING":
            return None
        return {
            "ticker": ticker,
            "entityName": "Acme",
            "cik": 1,
            "source_form": "10-Q",
            "period_end": "2025-06-30",
            "periods_requested": periods,
            "metrics": {"revenue": {"quarterly": [{"period": "Q2 2025", "value": 110, "qoq_pct": 0.1}], "annual": []}},
        }

    async def get_13f_holdings_changes(
        self, cik: str, *, days_back: int = 0, top_n: int = 25
    ) -> dict[str, Any]:
        self.calls.append(("13f_changes", {"cik": cik, "days_back": days_back, "top_n": top_n}))
        if cik == "0000000404":
            return {
                "manager_cik": cik, "manager_name": None,
                "current_filing": None, "prior_filing": None, "changes": None,
            }
        return {
            "manager_cik": cik,
            "manager_name": "Test Manager",
            "current_filing": {"accession_no": "0000000000-26-000002", "filing_date": "2026-04-30"},
            "prior_filing": {"accession_no": "0000000000-25-000099", "filing_date": "2026-01-30"},
            "changes": {
                "new_positions": [{"issuer": "NVDA", "value_change": 1_000_000}],
                "increased": [],
                "reduced": [],
                "sold_out": [],
                "unchanged_count": 5,
                "totals": {
                    "prior_value": 10_000_000, "current_value": 11_000_000,
                    "value_change": 1_000_000, "turnover_pct": 0.1,
                    "top10_concentration": 0.6,
                },
            },
        }

    async def close(self) -> None:
        pass


class FakePriceFetcher:
    async def get_daily_candles(self, symbol: str, days_back: int) -> list[dict[str, Any]]:
        return [{"time": "2026-04-30", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000}]


@pytest.fixture
def fake_fetcher() -> FakeFetcher:
    return FakeFetcher()


@pytest.fixture
def fake_prices() -> FakePriceFetcher:
    return FakePriceFetcher()


@pytest.fixture
def client(fake_fetcher: FakeFetcher, fake_prices: FakePriceFetcher) -> TestClient:
    app.dependency_overrides[get_fetcher] = lambda: fake_fetcher
    app.dependency_overrides[get_price_fetcher] = lambda: fake_prices
    app_module.aws_resources._memory_counts.clear()
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health_is_open(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["service"] == "copetech-sec-api"


def test_config_returns_public_settings(client: TestClient) -> None:
    response = client.get("/config")
    assert response.status_code == 200
    assert "aws_region" in response.json()


def test_missing_demo_key_is_401(client: TestClient) -> None:
    response = client.get(
        "/api/sec/company/AAPL", headers={"x-backend-secret": "test-backend-secret"}
    )
    assert response.status_code == 401


def test_wrong_demo_key_is_403(client: TestClient) -> None:
    response = client.get(
        "/api/sec/company/AAPL",
        headers={"x-backend-secret": "test-backend-secret", "x-demo-key": "nope"},
    )
    assert response.status_code == 403


def test_wrong_backend_secret_is_401(client: TestClient) -> None:
    response = client.get(
        "/api/sec/company/AAPL",
        headers={"x-backend-secret": "wrong", "x-demo-key": "test-demo-key"},
    )
    assert response.status_code == 401


def test_company_happy_path(client: TestClient, fake_fetcher: FakeFetcher) -> None:
    response = client.get("/api/sec/company/aapl", headers=VALID_HEADERS)
    assert response.status_code == 200
    assert response.json()["ticker"] == "AAPL"
    assert fake_fetcher.calls[0] == ("company_info", {"ticker": "AAPL"})


def test_company_404_when_fetcher_returns_none(client: TestClient) -> None:
    response = client.get("/api/sec/company/MISSING", headers=VALID_HEADERS)
    assert response.status_code == 404


def test_company_invalid_ticker_is_400(client: TestClient) -> None:
    response = client.get("/api/sec/company/!!!", headers=VALID_HEADERS)
    assert response.status_code == 400


def test_transactions_route_passes_query_params(client: TestClient, fake_fetcher: FakeFetcher) -> None:
    response = client.get(
        "/api/sec/transactions/AAPL?days_back=90&filing_limit=10",
        headers=VALID_HEADERS,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ticker"] == "AAPL"
    assert body["days_back"] == 90
    assert len(body["transactions"]) == 1
    assert fake_fetcher.calls[0][1] == {"ticker": "AAPL", "days_back": 90, "filing_limit": 10}


def test_insiders_alias_routes_share_handler(client: TestClient) -> None:
    for path in ("/sec/insiders", "/api/sec/insiders"):
        response = client.get(f"{path}?symbol=AAPL", headers=VALID_HEADERS)
        assert response.status_code == 200, path
        body = response.json()
        assert body["symbol"] == "AAPL"
        assert "events" in body
        assert "llm_digest" in body


def test_chart_route_includes_candles(client: TestClient) -> None:
    response = client.get("/api/sec/chart?symbol=AAPL", headers=VALID_HEADERS)
    assert response.status_code == 200
    body = response.json()
    assert "candles" in body
    assert body["candles"][0]["time"] == "2026-04-30"


def test_insider_signals_anchor_type_validated(client: TestClient) -> None:
    bad = client.get(
        "/api/sec/insider-signals/AAPL?anchor_type=garbage", headers=VALID_HEADERS
    )
    assert bad.status_code == 422

    ok = client.get(
        "/api/sec/insider-signals/AAPL?anchor_type=transaction_date",
        headers=VALID_HEADERS,
    )
    assert ok.status_code == 200
    assert ok.json()["symbol"] == "AAPL"


def test_thirteenf_holdings_happy(client: TestClient) -> None:
    response = client.get("/api/sec/13f/0001067983", headers=VALID_HEADERS)
    assert response.status_code == 200
    body = response.json()
    assert body["holdings_count"] == 1
    assert body["filing"]["accession_no"]


def test_thirteenf_holdings_404_when_no_filing(client: TestClient) -> None:
    response = client.get("/api/sec/13f/0000000404", headers=VALID_HEADERS)
    assert response.status_code == 404


def test_thirteenf_holdings_invalid_cik_is_400(client: TestClient) -> None:
    response = client.get("/api/sec/13f/notacik", headers=VALID_HEADERS)
    assert response.status_code == 400


def test_insider_clusters_route_returns_clusters(client: TestClient, fake_fetcher: FakeFetcher) -> None:
    response = client.get(
        "/api/sec/insider-signals/AAPL/clusters?window_days=10&min_unique_insiders=2",
        headers=VALID_HEADERS,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "AAPL"
    assert body["window_days"] == 10
    assert body["min_unique_insiders"] == 2
    assert len(body["clusters"]) == 1
    assert body["clusters"][0]["unique_insiders"] == 2
    cluster_call = fake_fetcher.form4_processor.cluster_calls[-1]
    assert cluster_call["window_days"] == 10
    assert cluster_call["min_unique_insiders"] == 2


def test_insider_clusters_validates_thresholds(client: TestClient) -> None:
    too_few = client.get(
        "/api/sec/insider-signals/AAPL/clusters?min_unique_insiders=1",
        headers=VALID_HEADERS,
    )
    assert too_few.status_code == 422


def test_financial_trend_route_happy_path(client: TestClient, fake_fetcher: FakeFetcher) -> None:
    response = client.get(
        "/api/sec/financials/AAPL/trend?periods=4", headers=VALID_HEADERS
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ticker"] == "AAPL"
    assert body["periods_requested"] == 4
    assert "revenue" in body["metrics"]
    assert fake_fetcher.calls[-1] == ("trend", {"ticker": "AAPL", "periods": 4})


def test_financial_trend_404_when_no_facts(client: TestClient) -> None:
    response = client.get("/api/sec/financials/MISSING/trend", headers=VALID_HEADERS)
    assert response.status_code == 404


def test_thirteenf_changes_route_happy_path(client: TestClient, fake_fetcher: FakeFetcher) -> None:
    response = client.get(
        "/api/sec/13f/0001067983/changes?top_n=5", headers=VALID_HEADERS
    )
    assert response.status_code == 200
    body = response.json()
    assert body["manager_cik"] == "0001067983"
    assert body["current_filing"]["accession_no"]
    assert body["changes"]["new_positions"][0]["issuer"] == "NVDA"
    assert body["changes"]["totals"]["turnover_pct"] == 0.1
    assert fake_fetcher.calls[-1][0] == "13f_changes"
    assert fake_fetcher.calls[-1][1]["top_n"] == 5


def test_thirteenf_changes_404_when_no_filing(client: TestClient) -> None:
    response = client.get("/api/sec/13f/0000000404/changes", headers=VALID_HEADERS)
    assert response.status_code == 404


def test_thirteenf_changes_invalid_cik_is_400(client: TestClient) -> None:
    response = client.get("/api/sec/13f/notacik/changes", headers=VALID_HEADERS)
    assert response.status_code == 400


def test_rate_limit_falls_back_to_memory_and_429s(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    tight = dataclasses.replace(app_module.aws_resources.settings, rate_limit_per_day=1)
    monkeypatch.setattr(app_module.aws_resources, "settings", tight)

    first = client.get("/api/sec/company/AAPL", headers=VALID_HEADERS)
    assert first.status_code == 200

    second = client.get("/api/sec/company/AAPL", headers=VALID_HEADERS)
    assert second.status_code == 429
    detail = second.json()["detail"]
    assert detail["limit"] == 1
    assert detail["count"] >= 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
