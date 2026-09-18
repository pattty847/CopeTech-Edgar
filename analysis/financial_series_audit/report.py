"""Write reviewable machine and human output for financial audit runs."""

from __future__ import annotations

import csv
import json
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPORT_SCHEMA_VERSION = 1
MINIMUM_REVIEW_PERIOD_COVERAGE = 5


def write_run_report(
    output_dir: str | Path,
    issuer_reports: list[dict[str, Any]],
    findings: list[dict[str, Any]],
) -> Path:
    run_dir = Path(output_dir) / datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S.%fZ"
    )
    run_dir.mkdir(parents=True, exist_ok=False)
    summary = {
        "schemaVersion": REPORT_SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "issuerCount": len(issuer_reports),
        "findingCounts": dict(Counter(row["severity"] for row in findings)),
        "issuers": _summary_projection(issuer_reports),
    }
    _write_json(run_dir / "summary.json", summary)
    _write_json(run_dir / "findings.json", findings)
    _write_coverage_csv(run_dir / "coverage.csv", issuer_reports)
    (run_dir / "manual-review.md").write_text(
        _manual_review_markdown(issuer_reports), encoding="utf-8"
    )
    return run_dir


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _summary_projection(issuers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep report provenance useful without duplicating every raw fact body."""

    projected = deepcopy(issuers)
    for issuer in projected:
        for metric in issuer.get("metrics") or []:
            for observation in metric.get("observations") or []:
                sources = observation.pop("sources", [])
                observation["sourceCount"] = len(sources)
                observation["sourceConcepts"] = sorted(
                    {
                        f"{source.get('taxonomy')}:{source.get('concept')}"
                        for source in sources
                        if isinstance(source, dict)
                        and source.get("taxonomy")
                        and source.get("concept")
                    }
                )
    return projected


def _write_coverage_csv(path: Path, issuers: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "ticker",
                "cik",
                "metric",
                "frequency",
                "applicability",
                "state",
                "observation_count",
                "warnings",
            ),
        )
        writer.writeheader()
        for issuer in issuers:
            for row in issuer.get("metrics", []) + issuer.get("valuations", []):
                writer.writerow(
                    {
                        "ticker": issuer["ticker"],
                        "cik": issuer.get("cik"),
                        "metric": row["metric"],
                        "frequency": row["frequency"],
                        "applicability": row.get("applicability", "optional"),
                        "state": row["state"],
                        "observation_count": len(row.get("observations") or []),
                        "warnings": "|".join(row.get("warnings") or []),
                    }
                )


def _manual_review_markdown(issuers: list[dict[str, Any]]) -> str:
    lines = [
        "# Financial-series manual review",
        "",
        "Verify values against the linked primary SEC filing. Do not infer zero from absence.",
        "",
    ]
    for issuer in issuers:
        lines.extend(_issuer_review(issuer))
    return "\n".join(lines) + "\n"


def _issuer_review(issuer: dict[str, Any]) -> list[str]:
    lines = [f"## {issuer['ticker']} — {issuer.get('entityName') or 'Unknown issuer'}", ""]
    for frequency in ("annual", "quarterly"):
        period_end = _recent_review_period(issuer.get("metrics") or [], frequency)
        lines.extend([f"### {frequency.title()}", ""])
        if period_end is None:
            lines.extend(["No supported observation is available.", ""])
            continue
        lines.extend(
            [
                f"Selected economic period: `{period_end}`",
                "",
                "| Check | Metric | Value | Source concept | Filing | Warnings |",
                "|---|---|---:|---|---|---|",
            ]
        )
        for metric in issuer.get("metrics") or []:
            if metric.get("frequency") != frequency:
                continue
            observation = next(
                (
                    row
                    for row in reversed(metric.get("observations") or [])
                    if row.get("periodEnd") == period_end
                ),
                None,
            )
            if observation is None:
                continue
            source = observation.get("availabilitySource") or observation.get("selectedSource") or {}
            filing = _filing_link(issuer.get("cik"), source.get("accessionNumber"))
            value = f"{observation.get('value')} {observation.get('unit') or ''}".strip()
            warnings = ", ".join(observation.get("qualityFlags") or []) or "—"
            lines.append(
                "| [ ] | {metric} | {value} | `{taxonomy}:{concept}` | {filing} | {warnings} |".format(
                    metric=metric["metric"],
                    value=value,
                    taxonomy=source.get("taxonomy") or "?",
                    concept=source.get("concept") or "?",
                    filing=filing,
                    warnings=warnings,
                )
            )
        lines.extend(["", "Reviewer: __________  Date: __________", ""])
    return lines


def _recent_review_period(metrics: list[dict[str, Any]], frequency: str) -> str | None:
    counts = Counter(
        str(observation["periodEnd"])
        for metric in metrics
        if metric.get("frequency") == frequency
        for observation in metric.get("observations") or []
        if observation.get("periodEnd")
    )
    if not counts:
        return None
    sufficiently_broad = [
        period
        for period, count in counts.items()
        if count >= MINIMUM_REVIEW_PERIOD_COVERAGE
    ]
    if sufficiently_broad:
        return max(sufficiently_broad)
    highest_coverage = max(counts.values())
    return max(period for period, count in counts.items() if count == highest_coverage)


def _filing_link(cik: Any, accession: Any) -> str:
    if not cik or not accession:
        return "—"
    accession = str(accession)
    compact = accession.replace("-", "")
    cik_number = str(int(str(cik)))
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_number}/{compact}/"
        f"{accession}-index.html"
    )
    return f"[SEC]({url})"
