"""Canonical financial metric registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MetricDefinition:
    id: str
    label: str
    fact_type: str
    concepts: tuple[tuple[str, str], ...]
    valid_units: tuple[str, ...]
    aggregation: str


METRIC_REGISTRY: dict[str, MetricDefinition] = {
    "revenue": MetricDefinition(
        id="revenue",
        label="Revenue",
        fact_type="duration",
        concepts=(
            ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
            ("us-gaap", "RevenueFromContractWithCustomerIncludingAssessedTax"),
            ("us-gaap", "Revenues"),
            ("us-gaap", "SalesRevenueGoodsNet"),
            ("us-gaap", "SalesRevenueNet"),
            ("ifrs-full", "Revenue"),
            ("ifrs-full", "RevenueFromContractsWithCustomers"),
        ),
        valid_units=("USD",),
        aggregation="sum",
    ),
}


def list_supported_metrics() -> list[dict[str, Any]]:
    return [
        {
            "id": definition.id,
            "label": definition.label,
            "factType": definition.fact_type,
            "validUnits": list(definition.valid_units),
            "aggregation": definition.aggregation,
            "concepts": [
                {"taxonomy": taxonomy, "concept": concept}
                for taxonomy, concept in definition.concepts
            ],
        }
        for definition in METRIC_REGISTRY.values()
    ]


def get_metric_definition(metric: str) -> MetricDefinition:
    try:
        return METRIC_REGISTRY[metric]
    except KeyError as exc:
        raise ValueError(
            f"unsupported metric {metric!r}; supported: {', '.join(METRIC_REGISTRY)}"
        ) from exc
