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
    # Some fallback concepts are deliberately broader or narrower than the
    # metric label. Preserve coverage, but make that semantic compromise travel
    # with every selected fact instead of hiding it in registry order.
    concept_quality_flags: tuple[tuple[str, tuple[str, ...]], ...] = ()


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
        concept_quality_flags=(("CostOfServices", ("cost_of_services_may_not_equal_total_cost_of_revenue",)),),
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
            # Alphabet tags depreciation alone; intangible amortization is
            # reported separately when it exists at all. Understates D&A for
            # issuers with heavy intangible amortization — provenance shows
            # which concept was used.
            ("us-gaap", "Depreciation"),
        ),
        valid_units=("USD",),
        aggregation="sum",
        ytd_cadence=True,
        concept_quality_flags=(("Depreciation", ("depreciation_only_may_understate_dep_amort",)),),
    ),
    "interest_expense": MetricDefinition(
        id="interest_expense",
        label="Interest expense",
        fact_type="duration",
        concepts=(
            ("us-gaap", "InterestExpense"),
            ("us-gaap", "InterestExpenseNonoperating"),
            ("us-gaap", "InterestExpenseDebt"),
            ("us-gaap", "InterestIncomeExpenseNet"),
        ),
        valid_units=("USD",),
        aggregation="sum",
        concept_quality_flags=(("InterestIncomeExpenseNet", ("net_interest_used_for_interest_expense",)),),
    ),
    "tax_expense": MetricDefinition(
        id="tax_expense",
        label="Income tax expense",
        fact_type="duration",
        concepts=(("us-gaap", "IncomeTaxExpenseBenefit"),),
        valid_units=("USD",),
        aggregation="sum",
    ),
    "pretax_income": MetricDefinition(
        id="pretax_income",
        label="Pre-tax income",
        fact_type="duration",
        concepts=(
            ("us-gaap", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"),
            ("us-gaap", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"),
        ),
        valid_units=("USD",),
        aggregation="sum",
    ),
    # Balance-sheet (instant) metrics: one value per reported balance date.
    "shares_outstanding": MetricDefinition(
        id="shares_outstanding",
        label="Shares outstanding",
        fact_type="instant",
        concepts=(
            ("dei", "EntityCommonStockSharesOutstanding"),
            ("us-gaap", "CommonStockSharesOutstanding"),
        ),
        valid_units=("shares",),
        aggregation="point_in_time",
    ),
    "stockholders_equity": MetricDefinition(
        id="stockholders_equity",
        label="Stockholders' equity",
        fact_type="instant",
        concepts=(
            ("us-gaap", "StockholdersEquity"),
            ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
        ),
        valid_units=("USD",),
        aggregation="point_in_time",
        concept_quality_flags=(("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", ("equity_includes_noncontrolling_interest",)),),
    ),
    "cash_equivalents": MetricDefinition(
        id="cash_equivalents",
        label="Cash and cash equivalents",
        fact_type="instant",
        concepts=(
            ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
            ("us-gaap", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"),
        ),
        valid_units=("USD",),
        aggregation="point_in_time",
        concept_quality_flags=(("CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", ("cash_includes_restricted_cash",)),),
    ),
    "short_term_investments": MetricDefinition(
        id="short_term_investments",
        label="Short-term investments",
        fact_type="instant",
        concepts=(
            ("us-gaap", "ShortTermInvestments"),
            ("us-gaap", "MarketableSecuritiesCurrent"),
            ("us-gaap", "AvailableForSaleSecuritiesDebtSecuritiesCurrent"),
        ),
        valid_units=("USD",),
        aggregation="point_in_time",
    ),
    "debt_current": MetricDefinition(
        id="debt_current",
        label="Debt due within one year",
        fact_type="instant",
        concepts=(
            ("us-gaap", "LongTermDebtCurrent"),
            ("us-gaap", "DebtCurrent"),
            ("us-gaap", "LongTermDebtAndCapitalLeaseObligationsCurrent"),
        ),
        valid_units=("USD",),
        aggregation="point_in_time",
    ),
    "debt_noncurrent": MetricDefinition(
        id="debt_noncurrent",
        label="Long-term debt",
        fact_type="instant",
        concepts=(
            ("us-gaap", "LongTermDebtNoncurrent"),
            ("us-gaap", "LongTermDebtAndCapitalLeaseObligations"),
        ),
        valid_units=("USD",),
        aggregation="point_in_time",
    ),
    "total_debt": MetricDefinition(
        id="total_debt",
        label="Total debt",
        fact_type="instant",
        concepts=(
            ("us-gaap", "DebtLongtermAndShorttermCombinedAmount"),
            ("us-gaap", "LongTermDebt"),
            ("us-gaap", "LongTermDebtAndCapitalLeaseObligations"),
        ),
        valid_units=("USD",),
        aggregation="point_in_time",
        concept_quality_flags=(
            ("LongTermDebt", ("total_debt_may_exclude_short_term_borrowings",)),
            (
                "LongTermDebtAndCapitalLeaseObligations",
                ("total_debt_may_exclude_short_term_borrowings",),
            ),
        ),
    ),
    "current_assets": MetricDefinition(
        id="current_assets",
        label="Current assets",
        fact_type="instant",
        concepts=(("us-gaap", "AssetsCurrent"),),
        valid_units=("USD",),
        aggregation="point_in_time",
    ),
    "current_liabilities": MetricDefinition(
        id="current_liabilities",
        label="Current liabilities",
        fact_type="instant",
        concepts=(("us-gaap", "LiabilitiesCurrent"),),
        valid_units=("USD",),
        aggregation="point_in_time",
    ),
    "receivables": MetricDefinition(
        id="receivables",
        label="Accounts receivable",
        fact_type="instant",
        concepts=(
            ("us-gaap", "AccountsReceivableNetCurrent"),
            ("us-gaap", "ReceivablesNetCurrent"),
        ),
        valid_units=("USD",),
        aggregation="point_in_time",
    ),
    "inventory": MetricDefinition(
        id="inventory",
        label="Inventory",
        fact_type="instant",
        concepts=(("us-gaap", "InventoryNet"),),
        valid_units=("USD",),
        aggregation="point_in_time",
    ),
    "payables": MetricDefinition(
        id="payables",
        label="Accounts payable",
        fact_type="instant",
        concepts=(
            ("us-gaap", "AccountsPayableCurrent"),
            ("us-gaap", "AccountsPayableAndAccruedLiabilitiesCurrent"),
        ),
        valid_units=("USD",),
        aggregation="point_in_time",
        concept_quality_flags=(("AccountsPayableAndAccruedLiabilitiesCurrent", ("payables_include_accrued_liabilities",)),),
    ),
    "total_assets": MetricDefinition(
        id="total_assets",
        label="Total assets",
        fact_type="instant",
        concepts=(("us-gaap", "Assets"),),
        valid_units=("USD",),
        aggregation="point_in_time",
    ),
    "total_liabilities": MetricDefinition(
        id="total_liabilities",
        label="Total liabilities",
        fact_type="instant",
        concepts=(("us-gaap", "Liabilities"),),
        valid_units=("USD",),
        aggregation="point_in_time",
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
            "frequencies": supported_frequencies(definition),
            "ytdCadence": definition.ytd_cadence,
            "concepts": [
                {"taxonomy": taxonomy, "concept": concept}
                for taxonomy, concept in definition.concepts
            ],
        }
        for definition in METRIC_REGISTRY.values()
    ]


def supported_frequencies(definition: MetricDefinition) -> list[str]:
    """Cadences the resolver can produce without returning a known-empty series."""

    if definition.fact_type == "instant":
        return ["quarterly", "annual"]
    if definition.aggregation == "weighted_average" and definition.id != "diluted_eps":
        return ["quarterly", "annual"]
    return ["quarterly", "ttm", "annual"]


def concept_quality_flags(
    definition: MetricDefinition,
    concept: str,
) -> tuple[str, ...]:
    return next(
        (flags for candidate, flags in definition.concept_quality_flags if candidate == concept),
        (),
    )


def get_metric_definition(metric: str) -> MetricDefinition:
    try:
        return METRIC_REGISTRY[metric]
    except KeyError as exc:
        raise ValueError(
            f"unsupported metric {metric!r}; supported: {', '.join(METRIC_REGISTRY)}"
        ) from exc
