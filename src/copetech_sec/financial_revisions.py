"""Reconstruct as-filed observation revisions for historical valuations.

The ordinary financial-series response intentionally presents one current
canonical value per economic window. A historical valuation needs a different
shape: if a later amendment changes an old quarter, prices before the amendment
must keep using the value investors knew then. This module samples the resolver
at each SEC filing date and retains only actual state changes per window.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any


SnapshotResolver = Callable[[str], Iterable[dict[str, Any]]]


def filing_dates(*row_groups: Iterable[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            str(row["filed"])
            for rows in row_groups
            for row in rows
            if row.get("filed")
        }
    )


def collect_observation_revisions(
    cutoffs: Iterable[str],
    resolve_at: SnapshotResolver,
) -> list[dict[str, Any]]:
    """Return the first observation plus each later value revision per window."""

    last_state: dict[tuple[str, str, str], tuple[float, tuple[str, ...]]] = {}
    revisions: list[dict[str, Any]] = []
    for cutoff in sorted(set(cutoffs)):
        for raw in resolve_at(cutoff):
            row = dict(raw)
            window = (
                str(row["periodStart"]),
                str(row["periodEnd"]),
                str(row["unit"]),
            )
            state = (
                float(row["value"]),
                tuple(sorted(row.get("qualityFlags") or [])),
            )
            if last_state.get(window) == state:
                continue
            if window in last_state:
                # A value that returns to a previously reported number is still
                # a new revision. The resolver's earliest-same-value semantics
                # would otherwise backdate that return to the original filing.
                row["availableAt"] = cutoff
                if row.get("alignedAt") != row.get("periodEnd"):
                    row["alignedAt"] = cutoff
                source = _source_filed_at(row, cutoff)
                if source is not None:
                    row["availabilitySource"] = source
            last_state[window] = state
            revisions.append(row)
    revisions.sort(key=lambda row: (row["availableAt"], row["periodEnd"]))
    return revisions


def resolve_financial_revisions(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    metric: str,
    frequency: str,
) -> list[dict[str, Any]]:
    from .financial_series import resolve_financial_series

    return collect_observation_revisions(
        filing_dates(rows),
        lambda cutoff: resolve_financial_series(
            rows,
            symbol=symbol,
            metric=metric,
            frequency=frequency,
            alignment="availability",
            as_of=cutoff,
        )["observations"],
    )


def resolve_derived_revisions(
    rows_by_component: dict[str, list[dict[str, Any]]],
    *,
    symbol: str,
    metric: str,
    frequency: str,
) -> list[dict[str, Any]]:
    from .derived_series import resolve_derived_series
    from .financial_series import resolve_financial_series

    return collect_observation_revisions(
        filing_dates(*rows_by_component.values()),
        lambda cutoff: resolve_derived_series(
            {
                component: resolve_financial_series(
                    rows,
                    symbol=symbol,
                    metric=component,
                    frequency=frequency,
                    alignment="availability",
                    as_of=cutoff,
                )
                for component, rows in rows_by_component.items()
                if rows
            },
            symbol=symbol,
            metric=metric,
            frequency=frequency,
            basis="canonical",
            alignment="availability",
            as_of=cutoff,
        )["observations"],
    )


def resolve_share_revisions(
    share_rows: list[dict[str, Any]],
    net_income_rows: list[dict[str, Any]],
    eps_rows: list[dict[str, Any]],
    instant_share_rows: list[dict[str, Any]],
    *,
    symbol: str,
) -> list[dict[str, Any]]:
    from .valuation_series import build_share_windows

    groups = (share_rows, net_income_rows, eps_rows, instant_share_rows)

    def snapshot(cutoff: str) -> list[dict[str, Any]]:
        filtered = [
            [row for row in rows if str(row["filed"]) <= cutoff]
            for rows in groups
        ]
        return build_share_windows(
            filtered[0],
            filtered[1],
            filtered[2],
            symbol=symbol,
            instant_share_rows=filtered[3],
        )

    return collect_observation_revisions(filing_dates(*groups), snapshot)


def _source_filed_at(row: dict[str, Any], cutoff: str) -> dict[str, Any] | None:
    return next(
        (
            source
            for source in reversed(row.get("sources") or [])
            if source.get("filed") == cutoff
        ),
        row.get("selectedSource"),
    )
