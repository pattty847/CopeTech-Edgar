"""Lazy pandas adapters for normalized CopeTech-Edgar payloads."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def _pandas():
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - exercised by minimal-install CI
        raise ImportError(
            "pandas adapters require the optional dependency: "
            "pip install 'copetech-edgar[dataframes]'"
        ) from exc
    return pd


def to_dataframe(
    records: Iterable[Mapping[str, Any]],
    *,
    metadata: Mapping[str, Any] | None = None,
):
    """Create a DataFrame without mutating or discarding nested provenance fields."""
    frame = _pandas().DataFrame.from_records(list(records))
    if metadata is not None:
        frame.attrs["copetech_metadata"] = dict(metadata)
    return frame


def financial_observations(payload: Mapping[str, Any]):
    """Convert a canonical financial-series payload into one row per observation."""
    return to_dataframe(
        payload.get("observations") or [],
        metadata={
            "symbol": payload.get("symbol"),
            "metric": payload.get("metric"),
            "frequency": payload.get("frequency"),
            "as_of": payload.get("asOf"),
            "warnings": payload.get("warnings") or [],
        },
    )


def ownership_entries(payload: Mapping[str, Any]):
    """Convert a Forms 3/4/5 payload into transaction and holding rows."""
    return to_dataframe(
        payload.get("entries") or [],
        metadata={
            "symbol": payload.get("symbol"),
            "forms": payload.get("forms") or [],
            **dict(payload.get("metadata") or {}),
        },
    )


def institutional_holdings(payload: Mapping[str, Any]):
    """Convert a latest-13F payload into reported holding rows."""
    return to_dataframe(
        payload.get("holdings") or [],
        metadata={
            "manager_cik": payload.get("manager_cik"),
            "manager_name": payload.get("manager_name"),
            "filing": payload.get("filing"),
        },
    )


def filing_rows(payload: Mapping[str, Any]):
    """Convert a paged filing-search payload into rows with page metadata attached."""
    return to_dataframe(
        payload.get("items") or [],
        metadata=payload.get("metadata") or {},
    )
