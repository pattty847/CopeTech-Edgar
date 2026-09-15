"""Composite financial series derived from canonical base metrics.

A derived metric never touches raw facts. Each component is resolved through
``resolve_financial_series`` first, so dedup, amendment timing, and derived-Q4
behavior are identical to base metrics, and composites are joined only on
exactly matching ``(periodStart, periodEnd)`` windows. Point-in-time honesty is
preserved by construction: an observation's ``availableAt`` is the latest
``availableAt`` among the components its value actually used, its confidence is
their minimum, and its quality flags are their union.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .debt_series import DEBT_COMPONENTS, invested_capital, net_debt
from .financial_metrics import get_metric_definition, supported_frequencies

# compute receives the component values present for one aligned window and
# returns (value, extra_quality_flags, component_ids_actually_used), or None to
# skip the window. Returning the used ids keeps availableAt honest when an
# optional component is present but not needed.
ComputeResult = tuple[float, tuple[str, ...], tuple[str, ...]] | None
Compute = Callable[[dict[str, float]], ComputeResult]


@dataclass(frozen=True)
class DerivedMetricDefinition:
    id: str
    label: str
    unit: str
    required: tuple[str, ...]
    optional: tuple[str, ...]
    compute: Compute
    derivation: str


def _fcf(values: dict[str, float]) -> ComputeResult:
    return (
        values["operating_cash_flow"] - values["capex"],
        (),
        ("operating_cash_flow", "capex"),
    )


def _gross_margin(values: dict[str, float]) -> ComputeResult:
    revenue = values["revenue"]
    if revenue == 0:
        return None
    if "gross_profit" in values:
        return values["gross_profit"] / revenue, (), ("gross_profit", "revenue")
    if "cost_of_revenue" in values:
        return (
            (revenue - values["cost_of_revenue"]) / revenue,
            ("gross_profit_derived_from_cost_of_revenue",),
            ("cost_of_revenue", "revenue"),
        )
    return None


def _operating_margin(values: dict[str, float]) -> ComputeResult:
    revenue = values["revenue"]
    if revenue == 0:
        return None
    return values["operating_income"] / revenue, (), ("operating_income", "revenue")


def _rnd_intensity(values: dict[str, float]) -> ComputeResult:
    revenue = values["revenue"]
    if revenue == 0:
        return None
    return values["rnd_expense"] / revenue, (), ("rnd_expense", "revenue")


def _fcf_margin(values: dict[str, float]) -> ComputeResult:
    revenue = values["revenue"]
    if revenue == 0:
        return None
    fcf = values["operating_cash_flow"] - values["capex"]
    return fcf / revenue, (), ("operating_cash_flow", "capex", "revenue")


def _sbc_burden(values: dict[str, float]) -> ComputeResult:
    revenue = values["revenue"]
    if revenue == 0:
        return None
    return values["sbc"] / revenue, (), ("sbc", "revenue")


def _capex_intensity(values: dict[str, float]) -> ComputeResult:
    revenue = values["revenue"]
    if revenue == 0:
        return None
    return values["capex"] / revenue, (), ("capex", "revenue")


def _gross_profit_dollars(values: dict[str, float]) -> ComputeResult:
    if "gross_profit" in values:
        return values["gross_profit"], (), ("gross_profit",)
    if "cost_of_revenue" in values:
        return (
            values["revenue"] - values["cost_of_revenue"],
            ("gross_profit_derived_from_cost_of_revenue",),
            ("cost_of_revenue", "revenue"),
        )
    return None


def _ebitda(values: dict[str, float]) -> ComputeResult:
    return (
        values["operating_income"] + values["dep_amort"],
        (),
        ("operating_income", "dep_amort"),
    )


def _interest_coverage(values: dict[str, float]) -> ComputeResult:
    interest = values["interest_expense"]
    if interest <= 0:
        return None
    return (
        values["operating_income"] / interest,
        (),
        ("operating_income", "interest_expense"),
    )


def _working_capital(values: dict[str, float]) -> ComputeResult:
    return (
        values["current_assets"] - values["current_liabilities"],
        (),
        ("current_assets", "current_liabilities"),
    )


def _revenue_per_share(values: dict[str, float]) -> ComputeResult:
    shares = values["diluted_shares"]
    if shares == 0:
        return None
    return values["revenue"] / shares, (), ("revenue", "diluted_shares")


DERIVED_METRIC_REGISTRY: dict[str, DerivedMetricDefinition] = {
    "fcf": DerivedMetricDefinition(
        id="fcf",
        label="Free cash flow",
        unit="USD",
        required=("operating_cash_flow", "capex"),
        optional=(),
        compute=_fcf,
        derivation="operating cash flow minus capital expenditures",
    ),
    "fcf_margin": DerivedMetricDefinition(
        id="fcf_margin",
        label="FCF margin",
        unit="ratio",
        required=("operating_cash_flow", "capex", "revenue"),
        optional=(),
        compute=_fcf_margin,
        derivation="free cash flow divided by revenue over the same window",
    ),
    "gross_margin": DerivedMetricDefinition(
        id="gross_margin",
        label="Gross margin",
        unit="ratio",
        required=("revenue",),
        optional=("gross_profit", "cost_of_revenue"),
        compute=_gross_margin,
        derivation=(
            "gross profit divided by revenue; falls back to revenue minus cost of"
            " revenue when the issuer does not tag GrossProfit"
        ),
    ),
    "operating_margin": DerivedMetricDefinition(
        id="operating_margin",
        label="Operating margin",
        unit="ratio",
        required=("revenue", "operating_income"),
        optional=(),
        compute=_operating_margin,
        derivation="operating income divided by revenue over the same window",
    ),
    "rnd_intensity": DerivedMetricDefinition(
        id="rnd_intensity",
        label="R&D intensity",
        unit="ratio",
        required=("revenue", "rnd_expense"),
        optional=(),
        compute=_rnd_intensity,
        derivation="research and development expense divided by revenue",
    ),
    "sbc_burden": DerivedMetricDefinition(
        id="sbc_burden",
        label="SBC burden",
        unit="ratio",
        required=("revenue", "sbc"),
        optional=(),
        compute=_sbc_burden,
        derivation="stock-based compensation divided by revenue",
    ),
    "capex_intensity": DerivedMetricDefinition(
        id="capex_intensity",
        label="Capex intensity",
        unit="ratio",
        required=("revenue", "capex"),
        optional=(),
        compute=_capex_intensity,
        derivation="capital expenditures divided by revenue",
    ),
    # Shadows the base gross_profit metric: issuers that never tag GrossProfit
    # (Alphabet tags CostOfRevenue instead) get the derived dollar series with
    # the same fallback gross_margin uses. The service routes derived ids first,
    # and the metric listing dedupes by id with the derived entry winning.
    "gross_profit": DerivedMetricDefinition(
        id="gross_profit",
        label="Gross profit",
        unit="USD",
        required=("revenue",),
        optional=("gross_profit", "cost_of_revenue"),
        compute=_gross_profit_dollars,
        derivation=(
            "reported gross profit; falls back to revenue minus cost of revenue"
            " when the issuer does not tag GrossProfit"
        ),
    ),
    "ebitda": DerivedMetricDefinition(
        id="ebitda",
        label="EBITDA",
        unit="USD",
        required=("operating_income", "dep_amort"),
        optional=(),
        compute=_ebitda,
        derivation="operating income plus depreciation and amortization",
    ),
    "interest_coverage": DerivedMetricDefinition(
        id="interest_coverage",
        label="Interest coverage",
        # "x" renders as a multiple (29.1×); "ratio" would render as a percent.
        unit="x",
        required=("operating_income", "interest_expense"),
        optional=(),
        compute=_interest_coverage,
        derivation="operating income divided by interest expense over the same window",
    ),
    "invested_capital": DerivedMetricDefinition(
        id="invested_capital",
        label="Invested capital",
        unit="USD",
        required=("stockholders_equity", "cash_equivalents"),
        optional=("short_term_investments",) + DEBT_COMPONENTS,
        compute=invested_capital,
        derivation=(
            "stockholders' equity plus reported all-debt aggregate, or long-term"
            " debt plus separate short-term borrowings, minus cash and short-term"
            " investments at the same balance date"
        ),
    ),
    "net_debt": DerivedMetricDefinition(
        id="net_debt",
        label="Net debt",
        unit="USD",
        required=("cash_equivalents",),
        optional=("short_term_investments",) + DEBT_COMPONENTS,
        compute=net_debt,
        derivation=(
            "reported all-debt aggregate, or long-term debt plus separate short-term"
            " borrowings, or current plus noncurrent debt, minus cash and short-term"
            " investments at the same balance date"
        ),
    ),
    "working_capital": DerivedMetricDefinition(
        id="working_capital",
        label="Working capital",
        unit="USD",
        required=("current_assets", "current_liabilities"),
        optional=(),
        compute=_working_capital,
        derivation="current assets minus current liabilities at the same balance date",
    ),
    "revenue_per_share": DerivedMetricDefinition(
        id="revenue_per_share",
        label="Revenue per diluted share",
        unit="USD/shares",
        required=("revenue", "diluted_shares"),
        optional=(),
        compute=_revenue_per_share,
        derivation="revenue divided by diluted weighted-average shares",
    ),
}


def is_derived_metric(metric: str) -> bool:
    return metric in DERIVED_METRIC_REGISTRY


def get_derived_definition(metric: str) -> DerivedMetricDefinition:
    try:
        return DERIVED_METRIC_REGISTRY[metric]
    except KeyError as exc:
        raise ValueError(
            f"unsupported derived metric {metric!r}; supported:"
            f" {', '.join(DERIVED_METRIC_REGISTRY)}"
        ) from exc


def list_derived_metrics() -> list[dict[str, Any]]:
    return [
        {
            "id": definition.id,
            "label": definition.label,
            "factType": "derived",
            "validUnits": [definition.unit],
            "aggregation": "composite",
            "derived": True,
            "frequencies": _supported_frequencies(definition),
            "components": sorted(definition.required + definition.optional),
        }
        for definition in DERIVED_METRIC_REGISTRY.values()
    ]


def _supported_frequencies(definition: DerivedMetricDefinition) -> list[str]:
    component_frequencies = [
        set(supported_frequencies(get_metric_definition(component)))
        for component in definition.required
    ]
    if not component_frequencies:
        return ["quarterly", "ttm", "annual"]
    supported = set.intersection(*component_frequencies)
    return [
        frequency
        for frequency in ("quarterly", "ttm", "annual")
        if frequency in supported
    ]


def resolve_derived_series(
    component_payloads: dict[str, dict[str, Any]],
    *,
    symbol: str,
    metric: str,
    frequency: str,
    basis: str,
    alignment: str,
    as_of: str | None = None,
) -> dict[str, Any]:
    definition = get_derived_definition(metric)
    indexed: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
    for component, payload in component_payloads.items():
        indexed[component] = {
            (row["periodStart"], row["periodEnd"]): row
            for row in payload.get("observations", [])
        }
    warnings: set[str] = {
        warning
        for payload in component_payloads.values()
        for warning in payload.get("warnings") or []
    }
    if metric in {"invested_capital", "net_debt"}:
        if not any(component in component_payloads for component in DEBT_COMPONENTS):
            warnings.add("debt_concepts_missing")
    if metric == "net_debt":
        warnings.add("net_debt_not_comparable_for_financial_companies")
    if frequency == "ttm":
        for component in definition.required:
            if get_metric_definition(component).aggregation == "weighted_average":
                warnings.add("ttm_unavailable_for_weighted_average_component")
    required_windows = [
        indexed.get(component, {}).keys() for component in definition.required
    ]
    common = set.intersection(*(set(keys) for keys in required_windows)) if required_windows else set()
    observations: list[dict[str, Any]] = []
    for window in sorted(common):
        component_rows = {
            component: indexed[component][window]
            for component in definition.required
        }
        for component in definition.optional:
            row = indexed.get(component, {}).get(window)
            if row is not None:
                component_rows[component] = row
        if metric in {"invested_capital", "net_debt"}:
            component_rows = _deduplicate_debt_hierarchy(component_rows)
        computed = definition.compute(
            {component: float(row["value"]) for component, row in component_rows.items()}
        )
        if computed is None:
            continue
        value, extra_flags, used = computed
        used_rows = [component_rows[component] for component in used]
        available_at = max(row["availableAt"] for row in used_rows)
        primary = component_rows[definition.required[0]]
        flags = sorted(
            {flag for row in used_rows for flag in row.get("qualityFlags") or []}
            | set(extra_flags)
        )
        observations.append(
            {
                "periodStart": window[0],
                "periodEnd": window[1],
                "availableAt": available_at,
                "alignedAt": available_at if alignment == "availability" else window[1],
                "value": value,
                "unit": definition.unit,
                "frequency": frequency,
                "fiscalYear": primary.get("fiscalYear"),
                "fiscalPeriod": primary.get("fiscalPeriod"),
                "reported": False,
                "derived": True,
                "derivation": definition.derivation,
                "inputMetrics": list(used),
                "confidence": min(float(row["confidence"]) for row in used_rows),
                "qualityFlags": flags,
                "availabilitySource": max(
                    used_rows, key=lambda row: row["availableAt"]
                )["availabilitySource"],
                # The first `used` component is the formula's numerator or
                # primary value. Pointing this at the first *required* component
                # mislabeled reported gross profit as revenue because revenue is
                # required only to support the fallback calculation.
                "selectedSource": used_rows[0]["selectedSource"],
                "sources": [
                    source for row in used_rows for source in row.get("sources") or []
                ],
            }
        )
    observations.sort(key=lambda row: (row["periodEnd"], row["availableAt"]))
    warnings |= {flag for row in observations for flag in row["qualityFlags"] if flag}
    any_payload = next(iter(component_payloads.values()), {})
    return {
        "symbol": symbol.upper(),
        "cik": any_payload.get("cik"),
        "entityName": any_payload.get("entityName"),
        "metric": metric,
        "label": definition.label,
        "frequency": frequency,
        "basis": basis,
        "alignment": alignment,
        "asOf": as_of,
        "normalizationVersion": any_payload.get("normalizationVersion"),
        "derived": True,
        "components": sorted(definition.required + definition.optional),
        "observations": observations,
        "warnings": sorted(warnings),
    }


def _deduplicate_debt_hierarchy(
    component_rows: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Keep debt siblings, but remove children already included by a parent."""

    rows = dict(component_rows)
    total = rows.get("total_debt")
    if total is not None:
        rows.pop("long_term_debt", None)
        rows.pop("debt_current", None)
        rows.pop("debt_noncurrent", None)
        rows.pop("short_term_borrowings", None)
        return rows

    if "long_term_debt" in rows:
        rows.pop("debt_current", None)
        rows.pop("debt_noncurrent", None)
        return rows

    current = rows.get("debt_current")
    current_concept = str(
        ((current or {}).get("selectedSource") or {}).get("concept") or ""
    )
    if current_concept == "DebtCurrent":
        rows.pop("short_term_borrowings", None)
    return rows
