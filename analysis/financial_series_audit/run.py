"""Sequential, cache-first runner for the deterministic fundamentals corpus."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
from typing import Any

from copetech_sec.cache_manager import SecCacheManager
from copetech_sec.sec_api import SECDataFetcher

from .checks import AuditFinding, check_balance_equation, check_financial_series
from .corpus import CANARY_CORPUS, CorpusIssuer, applicability_for, corpus_by_ticker
from .pipeline import build_metric_matrix
from .report import write_run_report


ROOT = Path(__file__).resolve().parent
DEFAULT_CACHE_DIR = ROOT / ".cache"
DEFAULT_OUTPUT_DIR = ROOT / "output"
MINIMUM_REQUEST_INTERVAL_SECONDS = 0.5


async def run_audit(
    issuers: list[CorpusIssuer],
    *,
    cache_dir: Path,
    output_dir: Path,
    offline: bool,
    refresh: bool,
    request_interval_seconds: float,
) -> tuple[Path, list[dict[str, Any]]]:
    """Acquire each issuer once, resolve locally, and write one audit report."""

    if request_interval_seconds < MINIMUM_REQUEST_INTERVAL_SECONDS:
        raise ValueError(
            "request interval must be at least"
            f" {MINIMUM_REQUEST_INTERVAL_SECONDS:.1f} seconds"
        )
    if not offline and not os.environ.get("SEC_API_USER_AGENT"):
        raise RuntimeError(
            "SEC_API_USER_AGENT is required for live acquisition; use --offline"
            " only when every issuer is already cached"
        )

    cache_dir.mkdir(parents=True, exist_ok=True)
    issuer_reports: list[dict[str, Any]] = []
    findings: list[AuditFinding] = []
    fetcher = None if offline else SECDataFetcher(
        cache_dir=str(cache_dir), rate_limit_sleep=request_interval_seconds
    )
    cache_manager = SecCacheManager(cache_dir=str(cache_dir))
    try:
        for index, issuer in enumerate(issuers, start=1):
            print(f"[{index:02d}/{len(issuers):02d}] {issuer.ticker}", flush=True)
            payload = await _acquire_company_facts(
                fetcher,
                cache_manager,
                issuer,
                offline=offline,
                refresh=refresh,
            )
            if payload is None:
                findings.append(
                    AuditFinding(
                        code="company_facts_unavailable",
                        severity="error",
                        message="No cached or live Company Facts payload is available.",
                        symbol=issuer.ticker,
                    )
                )
                issuer_reports.append(_unavailable_issuer_report(issuer))
                continue
            actual_cik = _normalized_cik(payload.get("cik"))
            if actual_cik != issuer.cik:
                findings.append(
                    AuditFinding(
                        code="issuer_identity_mismatch",
                        severity="error",
                        message="The resolved Company Facts CIK differs from the pinned corpus CIK.",
                        symbol=issuer.ticker,
                        context={"expectedCik": issuer.cik, "actualCik": actual_cik},
                    )
                )
                issuer_reports.append(_unavailable_issuer_report(issuer))
                continue
            matrix = await build_metric_matrix(
                issuer.ticker,
                payload,
                store_path=cache_dir / "financial_series.sqlite3",
            )
            matrix.update(
                {
                    "archetype": issuer.archetype,
                    "auditNote": issuer.audit_note,
                }
            )
            findings.extend(_audit_matrix(issuer, matrix))
            issuer_reports.append(matrix)
    finally:
        if fetcher is not None:
            await fetcher.close()

    finding_records = [finding.to_dict() for finding in findings]
    run_dir = write_run_report(output_dir, issuer_reports, finding_records)
    return run_dir, finding_records


async def _acquire_company_facts(
    fetcher: SECDataFetcher | None,
    cache_manager: SecCacheManager,
    issuer: CorpusIssuer,
    *,
    offline: bool,
    refresh: bool,
) -> dict[str, Any] | None:
    if offline:
        return await cache_manager.load_data(f"CIK{issuer.cik}", "facts")
    if fetcher is None:  # Defensive contract: live mode always constructs it.
        raise RuntimeError("live acquisition client is unavailable")
    return await fetcher.get_company_facts(issuer.ticker, use_cache=not refresh)


def _audit_matrix(
    issuer: CorpusIssuer,
    matrix: dict[str, Any],
) -> list[AuditFinding]:
    findings: list[AuditFinding] = []
    by_frequency: dict[str, dict[str, dict[str, Any]]] = {}
    for metric in matrix.get("metrics") or []:
        applicability = applicability_for(issuer, metric["metric"])
        metric["applicability"] = applicability
        shaped = {"symbol": issuer.ticker, **metric}
        if metric["state"] == "error":
            findings.append(
                AuditFinding(
                    code="metric_resolution_error",
                    severity="error",
                    message="The production financial-series resolver raised an error.",
                    symbol=issuer.ticker,
                    metric=metric["metric"],
                    frequency=metric["frequency"],
                    context={"error": metric.get("error")},
                )
            )
        findings.extend(
            check_financial_series(shaped, expected_unit=metric.get("expectedUnit"))
        )
        if applicability == "expected" and metric["state"] == "unavailable":
            findings.append(
                AuditFinding(
                    code="expected_metric_unavailable",
                    severity="warning",
                    message="A metric expected for this issuer archetype is unavailable.",
                    symbol=issuer.ticker,
                    metric=metric["metric"],
                    frequency=metric["frequency"],
                )
            )
        if applicability == "unsupported_taxonomy" and metric["state"] != "unavailable":
            findings.append(
                AuditFinding(
                    code="unsupported_taxonomy_metric_available",
                    severity="warning",
                    message="A metric resolved across a taxonomy boundary not yet reviewed.",
                    symbol=issuer.ticker,
                    metric=metric["metric"],
                    frequency=metric["frequency"],
                )
            )
        if applicability == "not_comparable" and metric["state"] != "unavailable":
            warnings = {
                str(warning)
                for warning in metric.get("warnings") or []
            } | {
                str(flag)
                for row in metric.get("observations") or []
                for flag in row.get("qualityFlags") or []
            }
            if not any("not_comparable" in warning for warning in warnings):
                findings.append(
                    AuditFinding(
                        code="missing_comparability_warning",
                        severity="warning",
                        message="A non-comparable metric resolved without an explicit warning.",
                        symbol=issuer.ticker,
                        metric=metric["metric"],
                        frequency=metric["frequency"],
                    )
                )
        by_frequency.setdefault(metric["frequency"], {})[metric["metric"]] = shaped

    for valuation in matrix.get("valuations") or []:
        valuation["applicability"] = applicability_for(issuer, valuation["metric"])
    for frequency_rows in by_frequency.values():
        findings.extend(check_balance_equation(frequency_rows))
    return findings


def _unavailable_issuer_report(issuer: CorpusIssuer) -> dict[str, Any]:
    return {
        "ticker": issuer.ticker,
        "cik": issuer.cik,
        "entityName": issuer.name,
        "archetype": issuer.archetype,
        "auditNote": issuer.audit_note,
        "taxonomies": [],
        "metrics": [],
        "valuations": [],
    }


def _normalized_cik(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return f"{int(value):010d}"
    except (TypeError, ValueError):
        return str(value)


def _selected_issuers(tickers: list[str]) -> list[CorpusIssuer]:
    if not tickers:
        return list(CANARY_CORPUS)
    indexed = corpus_by_ticker()
    requested = [ticker.strip().upper() for ticker in tickers]
    unknown = [ticker for ticker in requested if ticker not in indexed]
    if unknown:
        raise ValueError(f"tickers are not in the canary corpus: {', '.join(unknown)}")
    return [indexed[ticker] for ticker in requested]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", action="append", default=[], help="Audit one corpus ticker; repeat as needed")
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--offline", action="store_true", help="Prohibit SEC network access")
    parser.add_argument("--refresh", action="store_true", help="Refresh each issuer once instead of using today's cache")
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=0.6,
        help="Shared SEC request spacing; values below 0.5 are rejected",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        issuers = _selected_issuers(args.ticker)
        run_dir, findings = asyncio.run(
            run_audit(
                issuers,
                cache_dir=args.cache_dir,
                output_dir=args.output_dir,
                offline=args.offline,
                refresh=args.refresh,
                request_interval_seconds=args.request_interval_seconds,
            )
        )
    except (RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    counts = {
        severity: sum(row["severity"] == severity for row in findings)
        for severity in ("error", "warning")
    }
    print(f"Audit report: {run_dir}")
    print(f"Findings: {counts['error']} errors, {counts['warning']} warnings")
    return 1 if counts["error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
