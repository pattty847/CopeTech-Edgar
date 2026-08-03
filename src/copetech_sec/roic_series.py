"""Return on invested capital: TTM NOPAT over average invested capital.

ROIC is the first cross-cadence composite: the numerator is a trailing flow
(operating income after an effective tax rate) while the denominator is a
balance-sheet quantity, conventionally averaged over the beginning and ending
balances of the trailing window. Definitional choices, stated once:

- NOPAT = TTM operating income × (1 − TTM tax expense ÷ TTM pre-tax income).
  Windows with non-positive pre-tax income are skipped — an effective tax rate
  has no meaning there — and the payload says how many were skipped.
- Invested capital = stockholders' equity + debt − cash − short-term
  investments, exactly the `invested_capital` composite. Operating leases and
  goodwill are left untouched; changing that is a definitional decision, not a
  data fix.
- The denominator averages the balance closest to (and not after) each end of
  the TTM window. When no beginning balance exists the ending balance stands
  alone, flagged `single_period_invested_capital`.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from .financial_series import NORMALIZATION_VERSION

#: How far back from a window edge a balance date may sit and still represent it.
BALANCE_LOOKBACK_DAYS = 135

ROIC_METRIC_INFO: dict[str, Any] = {
    "id": "roic",
    "label": "Return on invested capital",
    "factType": "derived",
    "validUnits": ["ratio"],
    "aggregation": "composite",
    "derived": True,
    "components": [
        "operating_income",
        "tax_expense",
        "pretax_income",
        "invested_capital",
    ],
}


def resolve_roic_series(
    operating_income_ttm: dict[str, Any],
    tax_expense_ttm: dict[str, Any],
    pretax_income_ttm: dict[str, Any],
    invested_capital: dict[str, Any],
    *,
    symbol: str,
    frequency: str = "ttm",
    alignment: str = "availability",
    as_of: str | None = None,
) -> dict[str, Any]:
    warnings: set[str] = {
        warning
        for payload in (
            operating_income_ttm,
            tax_expense_ttm,
            pretax_income_ttm,
            invested_capital,
        )
        for warning in payload.get("warnings") or []
    }
    observations: list[dict[str, Any]] = []
    skipped_non_positive_pretax = 0

    if frequency != "ttm":
        warnings.add("roic_available_only_as_ttm")
    else:
        tax_by_window = _by_window(tax_expense_ttm)
        pretax_by_window = _by_window(pretax_income_ttm)
        balances = sorted(
            invested_capital.get("observations") or [],
            key=lambda row: row["periodEnd"],
        )
        for window in operating_income_ttm.get("observations") or []:
            key = (window["periodStart"], window["periodEnd"])
            tax = tax_by_window.get(key)
            pretax = pretax_by_window.get(key)
            if tax is None or pretax is None:
                continue
            pretax_value = float(pretax["value"])
            if pretax_value <= 0:
                skipped_non_positive_pretax += 1
                continue
            flags = {
                flag
                for row in (window, tax, pretax)
                for flag in row.get("qualityFlags") or []
            }
            tax_rate = float(tax["value"]) / pretax_value
            if tax_rate < 0 or tax_rate > 1:
                tax_rate = min(1.0, max(0.0, tax_rate))
                flags.add("effective_tax_rate_clamped")
            nopat = float(window["value"]) * (1.0 - tax_rate)

            ending = _balance_at(balances, window["periodEnd"])
            if ending is None:
                continue
            beginning = _balance_at(balances, window["periodStart"])
            used_balances = [ending]
            if beginning is None or beginning["periodEnd"] == ending["periodEnd"]:
                flags.add("single_period_invested_capital")
                average_capital = float(ending["value"])
            else:
                used_balances.append(beginning)
                average_capital = (
                    float(ending["value"]) + float(beginning["value"])
                ) / 2.0
            if average_capital <= 0:
                skipped = "non_positive_invested_capital"
                warnings.add(skipped)
                continue
            for balance in used_balances:
                flags |= set(balance.get("qualityFlags") or [])

            used = [window, tax, pretax] + used_balances
            available_at = max(str(row["availableAt"]) for row in used)
            observations.append(
                {
                    "periodStart": window["periodStart"],
                    "periodEnd": window["periodEnd"],
                    "availableAt": available_at,
                    "alignedAt": (
                        available_at
                        if alignment == "availability"
                        else window["periodEnd"]
                    ),
                    "value": nopat / average_capital,
                    "unit": "ratio",
                    "frequency": "ttm",
                    "fiscalYear": window.get("fiscalYear"),
                    "fiscalPeriod": "TTM",
                    "reported": False,
                    "derived": True,
                    "derivation": (
                        "TTM operating income after the effective tax rate, over"
                        " invested capital averaged across the window's beginning"
                        " and ending balance dates"
                    ),
                    "confidence": min(float(row["confidence"]) for row in used),
                    "qualityFlags": sorted(flags),
                    "availabilitySource": max(
                        used, key=lambda row: str(row["availableAt"])
                    )["availabilitySource"],
                    "selectedSource": window["selectedSource"],
                    "sources": [
                        source
                        for row in used
                        for source in row.get("sources") or []
                    ],
                }
            )

    if skipped_non_positive_pretax:
        warnings.add("roic_windows_skipped_non_positive_pretax")
    observations.sort(key=lambda row: (row["periodEnd"], row["availableAt"]))
    warnings |= {flag for row in observations for flag in row["qualityFlags"] if flag}
    return {
        "symbol": symbol.upper(),
        "cik": operating_income_ttm.get("cik"),
        "entityName": operating_income_ttm.get("entityName"),
        "metric": "roic",
        "label": ROIC_METRIC_INFO["label"],
        "frequency": frequency,
        "basis": "canonical",
        "alignment": alignment,
        "asOf": as_of,
        "normalizationVersion": NORMALIZATION_VERSION,
        "derived": True,
        "components": list(ROIC_METRIC_INFO["components"]),
        "observations": observations,
        "warnings": sorted(warnings),
    }


def _by_window(payload: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (row["periodStart"], row["periodEnd"]): row
        for row in payload.get("observations") or []
    }


def _balance_at(
    balances: list[dict[str, Any]],
    edge: str,
) -> dict[str, Any] | None:
    """Latest balance at or before `edge`, no older than the lookback bound."""
    floor = (date.fromisoformat(edge) - timedelta(days=BALANCE_LOOKBACK_DAYS)).isoformat()
    eligible = [
        row for row in balances if floor <= row["periodEnd"] <= edge
    ]
    return eligible[-1] if eligible else None
