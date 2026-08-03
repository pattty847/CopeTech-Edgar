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
    # Cash-flow-statement items are reported cumulatively in Q2/Q3 10-Qs, so
    # standalone quarters must be derived by differencing year-to-date windows.
    ytd_cadence: bool = False


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
    "diluted_eps": MetricDefinition(
        id="diluted_eps",
        label="Diluted earnings per share",
        fact_type="duration",
        concepts=(
            ("us-gaap", "EarningsPerShareDiluted"),
        ),
        valid_units=("USD/shares",),
        aggregation="weighted_average",
    ),
    "basic_eps": MetricDefinition(
        id="basic_eps",
        label="Basic earnings per share",
        fact_type="duration",
        concepts=(
            ("us-gaap", "EarningsPerShareBasic"),
        ),
        valid_units=("USD/shares",),
        aggregation="weighted_average",
    ),
    "net_income": MetricDefinition(
        id="net_income",
        label="Net income",
        fact_type="duration",
        concepts=(
            ("us-gaap", "NetIncomeLoss"),
            ("us-gaap", "ProfitLoss"),
        ),
        valid_units=("USD",),
        aggregation="sum",
    ),
    "diluted_shares": MetricDefinition(
        id="diluted_shares",
        label="Diluted weighted-average shares",
        fact_type="duration",
        concepts=(
            ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"),
            ("us-gaap", "WeightedAverageNumberOfShareOutstandingBasicAndDiluted"),
        ),
        valid_units=("shares",),
        aggregation="weighted_average",
    ),
    "gross_profit": MetricDefinition(
        id="gross_profit",
        label="Gross profit",
        fact_type="duration",
        concepts=(
            ("us-gaap", "GrossProfit"),
        ),
        valid_units=("USD",),
        aggregation="sum",
    ),
    "cost_of_revenue": MetricDefinition(
        id="cost_of_revenue",
        label="Cost of revenue",
        fact_type="duration",
        concepts=(
            ("us-gaap", "CostOfGoodsAndServicesSold"),
            ("us-gaap", "CostOfRevenue"),
            ("us-gaap", "CostOfGoodsSold"),
            ("us-gaap", "CostOfServices"),
        ),
        valid_units=("USD",),
        aggregation="sum",
    ),
    "operating_income": MetricDefinition(
        id="operating_income",
        label="Operating income",
        fact_type="duration",
        concepts=(
            ("us-gaap", "OperatingIncomeLoss"),
        ),
        valid_units=("USD",),
        aggregation="sum",
    ),
    "rnd_expense": MetricDefinition(
        id="rnd_expense",
        label="Research and development expense",
        fact_type="duration",
        concepts=(
            ("us-gaap", "ResearchAndDevelopmentExpense"),
            ("us-gaap", "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"),
        ),
        valid_units=("USD",),
        aggregation="sum",
    ),
    # Cash-flow-statement metrics: 10-Qs after Q1 report these only as
    # year-to-date windows, so ytd_cadence derives standalone Q2/Q3 by
    # differencing successive cumulative windows.
    "operating_cash_flow": MetricDefinition(
        id="operating_cash_flow",
        label="Operating cash flow",
        fact_type="duration",
        concepts=(
            ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
            ("us-gaap", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
        ),
        valid_units=("USD",),
        aggregation="sum",
        ytd_cadence=True,
    ),
    "capex": MetricDefinition(
        id="capex",
        label="Capital expenditures",
        fact_type="duration",
        concepts=(
            ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"),
            ("us-gaap", "PaymentsToAcquireProductiveAssets"),
            ("us-gaap", "PaymentsForCapitalImprovements"),
        ),
        valid_units=("USD",),
        aggregation="sum",
        ytd_cadence=True,
    ),
    "sbc": MetricDefinition(
        id="sbc",
        label="Stock-based compensation",
        fact_type="duration",
        concepts=(
            ("us-gaap", "ShareBasedCompensation"),
            ("us-gaap", "AllocatedShareBasedCompensationExpense"),
        ),
        valid_units=("USD",),
        aggregation="sum",
        ytd_cadence=True,
    ),
    "dep_amort": MetricDefinition(
        id="dep_amort",
        label="Depreciation and amortization",
        fact_type="duration",
        concepts=(
            ("us-gaap", "DepreciationDepletionAndAmortization"),
            ("us-gaap", "DepreciationAmortizationAndAccretionNet"),
            ("us-gaap", "DepreciationAndAmortization"),
        ),
        valid_units=("USD",),
        aggregation="sum",
        ytd_cadence=True,
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
            "ytdCadence": definition.ytd_cadence,
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
