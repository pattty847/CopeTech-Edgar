"""Fail-closed guards for unresolved observations entering financial arithmetic."""

from __future__ import annotations

from copy import deepcopy
import json
from typing import Any


AMBIGUOUS_INPUT_WARNING = "ambiguous_derivation_inputs"


def canonical_ambiguities(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Give repeated dependency paths one stable, evidence-preserving record."""
    unique = {json.dumps(record, sort_keys=True): record for record in records}
    return [unique[key] for key in sorted(unique)]


def dependent_ambiguity(
    *, stage: str, frequency: str, period_end: str, candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    """Identify an affected output window without inventing an arithmetic value."""
    unique = {json.dumps(row, sort_keys=True): row for row in candidates}
    return {
        "reason": "ambiguous_upstream_inputs", "stage": stage,
        "frequency": frequency, "periodEnd": period_end,
        "candidates": [deepcopy(unique[key]) for key in sorted(unique)],
    }


def unambiguous_observations(
    rows: list[dict[str, Any]],
    *,
    stage: str,
    ambiguities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep unique period ends without deciding equivalence or choosing a value.

    Quarterly and cumulative YTD facts can legitimately share an end, so cadence
    remains part of this guard's identity. Different starts or units within one
    cadence are unresolved even when values match. The caller keeps reported
    candidates; only arithmetic inputs are removed, with their evidence retained.
    """
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row.get("frequency") or ""), row["periodEnd"])
        grouped.setdefault(key, []).append(row)
    blocked = {key for key, candidates in grouped.items() if len(candidates) > 1}
    for frequency, period_end in sorted(blocked):
        ambiguities.append(
            {
                "reason": "duplicate_period_end",
                "stage": stage,
                "frequency": frequency,
                "periodEnd": period_end,
                "candidates": sorted(
                    deepcopy(grouped[(frequency, period_end)]),
                    key=lambda row: json.dumps(row, sort_keys=True),
                ),
            }
        )
    return [
        row for row in rows
        if (str(row.get("frequency") or ""), row["periodEnd"]) not in blocked
    ]
