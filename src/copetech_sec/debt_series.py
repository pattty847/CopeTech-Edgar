"""Debt-aware composite calculations with explicit parent/component precedence."""

from __future__ import annotations

from typing import TypeAlias


ComputeResult: TypeAlias = tuple[float, tuple[str, ...], tuple[str, ...]] | None
DEBT_COMPONENTS = (
    "total_debt",
    "long_term_debt",
    "debt_noncurrent",
    "debt_current_total",
    "long_term_debt_current",
    "reported_short_term_borrowings",
    "commercial_paper",
    "other_short_term_borrowings",
)


def selected_debt_components(values: dict[str, float]) -> tuple[str, ...]:
    """Return a debt hierarchy only when the available branches form a complete total."""

    if "total_debt" in values:
        long_term_children = _long_term_children(values)
        short_term = _short_term_components(values)
        if (
            long_term_children
            and short_term
            and _values_match(
                values["total_debt"],
                sum(values[component] for component in long_term_children),
            )
            and sum(values[component] for component in short_term) != 0
        ):
            # Some issuers apply the combined-debt tag only to current and
            # noncurrent maturities of long-term debt. The exact child equation
            # proves that separately tagged short-term borrowings are outside it.
            return ("total_debt",) + short_term
        return ("total_debt",)
    short_term = _short_term_components(values)
    if "long_term_debt" in values and short_term:
        return ("long_term_debt",) + short_term
    if "debt_current_total" in values and "debt_noncurrent" in values:
        return ("debt_current_total", "debt_noncurrent")
    long_term_children = _long_term_children(values)
    if long_term_children and short_term:
        return long_term_children + short_term
    return ()


def _long_term_children(values: dict[str, float]) -> tuple[str, ...]:
    required = ("long_term_debt_current", "debt_noncurrent")
    return required if all(component in values for component in required) else ()


def debt_decision_evidence(values: dict[str, float]) -> tuple[str, ...]:
    """Non-arithmetic facts needed to justify adding debt outside an aggregate."""
    selected = selected_debt_components(values)
    if "total_debt" in selected and len(selected) > 1:
        return _long_term_children(values)
    return ()


def _short_term_components(values: dict[str, float]) -> tuple[str, ...]:
    if "reported_short_term_borrowings" in values:
        return ("reported_short_term_borrowings",)
    return tuple(
        component
        for component in ("commercial_paper", "other_short_term_borrowings")
        if component in values
    )


def _values_match(left: float, right: float) -> bool:
    return abs(left - right) <= max(1.0, abs(left), abs(right)) * 1e-9


def invested_capital(values: dict[str, float]) -> ComputeResult:
    debt_components = selected_debt_components(values)
    if not debt_components:
        # A missing debt concept is unknown. It is not evidence of a zero balance.
        return None
    cash_components = tuple(
        component
        for component in ("cash_equivalents", "short_term_investments")
        if component in values
    )
    debt = sum(values[component] for component in debt_components)
    cash = sum(values[component] for component in cash_components)
    return (
        values["stockholders_equity"] + debt - cash,
        (),
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
        ("generic_net_debt_not_comparable_for_financial_companies",),
        debt_components + cash_components,
    )
