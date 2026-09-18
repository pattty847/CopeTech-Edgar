from __future__ import annotations

import asyncio

import pytest

from analysis.financial_series_audit.corpus import corpus_by_ticker
from analysis.financial_series_audit.run import (
    _audit_matrix,
    _selected_issuers,
    run_audit,
)


def _metric(state: str, *, warnings: list[str] | None = None) -> dict:
    return {
        "metric": "net_debt",
        "frequency": "annual",
        "state": state,
        "expectedUnit": "USD",
        "observations": [],
        "warnings": warnings or [],
    }


def test_runner_selects_only_pinned_corpus_issuers():
    assert [issuer.ticker for issuer in _selected_issuers(["sofi", "aapl"])] == [
        "SOFI",
        "AAPL",
    ]


def test_not_comparable_metric_requires_explicit_warning_when_available():
    issuer = corpus_by_ticker()["SOFI"]
    matrix = {"metrics": [_metric("derived")], "valuations": []}

    findings = _audit_matrix(issuer, matrix)

    assert "missing_comparability_warning" in {finding.code for finding in findings}
    assert matrix["metrics"][0]["applicability"] == "not_comparable"


def test_unavailable_not_comparable_metric_is_honest_without_warning():
    issuer = corpus_by_ticker()["SOFI"]
    matrix = {"metrics": [_metric("unavailable")], "valuations": []}

    assert _audit_matrix(issuer, matrix) == []


def test_expected_metric_blocked_by_external_split_history_is_not_called_missing():
    issuer = corpus_by_ticker()["AAPL"]
    metric = {
        "metric": "diluted_eps",
        "frequency": "ttm",
        "state": "unavailable",
        "expectedUnit": "USD/shares",
        "observations": [],
        "warnings": ["split_history_unverified"],
    }

    assert _audit_matrix(issuer, {"metrics": [metric], "valuations": []}) == []


def test_live_runner_requires_truthful_sec_user_agent(monkeypatch, tmp_path):
    monkeypatch.delenv("SEC_API_USER_AGENT", raising=False)

    with pytest.raises(RuntimeError, match="SEC_API_USER_AGENT"):
        asyncio.run(
            run_audit(
                [],
                cache_dir=tmp_path / "cache",
                output_dir=tmp_path / "output",
                offline=False,
                refresh=False,
                request_interval_seconds=0.6,
            )
        )


def test_runner_rejects_rate_limit_ceiling_pacing(tmp_path):
    with pytest.raises(ValueError, match="at least 0.5"):
        asyncio.run(
            run_audit(
                [],
                cache_dir=tmp_path / "cache",
                output_dir=tmp_path / "output",
                offline=True,
                refresh=False,
                request_interval_seconds=0.1,
            )
        )
