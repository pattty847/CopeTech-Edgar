"""Debt-aware composite calculations with explicit parent/component precedence."""

from __future__ import annotations

from typing import TypeAlias


ComputeResult: TypeAlias = tuple[float, tuple[str, ...], tuple[str, ...]] | None
DEBT_COMPONENTS = ("total_debt", "debt_current", "debt_noncurrent")


def selected_debt_components(values: dict[str, float]) -> tuple[str, ...]:
    """Prefer a reported parent total; otherwise use its current/noncurrent children."""

    if "total_debt" in values:
        return ("total_debt",)
    return tuple(component for component in DEBT_COMPONENTS[1:] if component in values)


def invested_capital(values: dict[str, float]) -> ComputeResult:
    debt_components = selected_debt_components(values)
    cash_components = tuple(
        component
        for component in ("cash_equivalents", "short_term_investments")
        if component in values
    )
    debt = sum(values[component] for component in debt_components)
    cash = sum(values[component] for component in cash_components)
    flags = () if debt_components else ("debt_concepts_missing_assumed_zero",)
    return (
        values["stockholders_equity"] + debt - cash,
        flags,
        ("stockholders_equity",) + debt_components + cash_components,
    )


def net_debt(values: dict[str, float]) -> ComputeResult:
    debt_components = selected_debt_components(values)
    if not debt_components:
        # Missing facts are unknown, not proof that the issuer has no debt.
        return None
    cash_components = tuple(
        component
        for component in ("cash_equivalents", "short_term_investments")
        if component in values
    )
    debt = sum(values[component] for component in debt_components)
    cash = sum(values[component] for component in cash_components)
    return (
        debt - cash,
        ("net_debt_not_comparable_for_financial_companies",),
        debt_components + cash_components,
    )
