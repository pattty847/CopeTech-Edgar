"""Deterministic issuer corpus for the fundamentals audit.

The corpus selects accounting shapes, not a statistically representative sample.
CIKs pin issuer identity because tickers can change or be reused. Applicability is
deliberately conservative: a metric that a profile does not name is optional, so
the audit never turns an unreviewed absence into a correctness failure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping


MetricApplicability = Literal[
    "expected",
    "optional",
    "not_comparable",
    "not_applicable",
    "unsupported_taxonomy",
]


@dataclass(frozen=True)
class ApplicabilityProfile:
    id: str
    expected: frozenset[str] = frozenset()
    not_comparable: frozenset[str] = frozenset()
    not_applicable: frozenset[str] = frozenset()
    optional: frozenset[str] = frozenset()
    default: MetricApplicability = "optional"

    def for_metric(self, metric: str) -> MetricApplicability:
        if metric in self.expected:
            return "expected"
        if metric in self.not_comparable:
            return "not_comparable"
        if metric in self.not_applicable:
            return "not_applicable"
        if metric in self.optional:
            return "optional"
        return self.default


@dataclass(frozen=True)
class CorpusIssuer:
    ticker: str
    cik: str
    name: str
    archetype: str
    applicability_profile: str
    audit_note: str


_COMMON_REPORTED = frozenset(
    {
        "revenue",
        "diluted_eps",
        "basic_eps",
        "net_income",
        "diluted_shares",
        "operating_income",
        "operating_cash_flow",
        "tax_expense",
        "pretax_income",
        "shares_outstanding",
        "stockholders_equity",
        "cash_equivalents",
        "total_assets",
        "total_liabilities",
    }
)

_OPERATING_DERIVED = frozenset(
    {
        "operating_margin",
        "revenue_per_share",
    }
)

_FINANCIAL_NONCOMPARABLE = frozenset(
    {
        "fcf",
        "fcf_margin",
        "gross_margin",
        "ebitda",
        "interest_coverage",
        "invested_capital",
        "net_debt",
        "working_capital",
    }
)


APPLICABILITY_PROFILES: Mapping[str, ApplicabilityProfile] = {
    "operating_company": ApplicabilityProfile(
        id="operating_company",
        expected=_COMMON_REPORTED | _OPERATING_DERIVED,
    ),
    "inventory_company": ApplicabilityProfile(
        id="inventory_company",
        expected=_COMMON_REPORTED
        | _OPERATING_DERIVED
        | {"inventory", "current_assets", "current_liabilities"},
    ),
    "financial_company": ApplicabilityProfile(
        id="financial_company",
        expected=frozenset(
            {
                "revenue",
                "diluted_eps",
                "basic_eps",
                "net_income",
                "diluted_shares",
                "shares_outstanding",
                "stockholders_equity",
                "cash_equivalents",
                "total_assets",
                "total_liabilities",
            }
        ),
        not_comparable=_FINANCIAL_NONCOMPARABLE,
    ),
    "reit": ApplicabilityProfile(
        id="reit",
        expected=frozenset(
            {
                "revenue",
                "net_income",
                "diluted_eps",
                "diluted_shares",
                "operating_cash_flow",
                "stockholders_equity",
                "cash_equivalents",
                "total_assets",
                "total_liabilities",
            }
        ),
        not_comparable=frozenset({"gross_margin", "ebitda"}),
    ),
    # Only revenue currently has IFRS concepts, monetary facts are USD-only,
    # and 6-K interim reports are outside the financial-series form contract.
    # DEI identity facts are taxonomy-neutral and remain optional.
    "foreign_ifrs_boundary": ApplicabilityProfile(
        id="foreign_ifrs_boundary",
        optional=frozenset({"revenue", "shares_outstanding"}),
        default="unsupported_taxonomy",
    ),
    "foreign_to_domestic_us_gaap": ApplicabilityProfile(
        id="foreign_to_domestic_us_gaap",
        expected=_COMMON_REPORTED | _OPERATING_DERIVED,
    ),
}


CANARY_CORPUS: tuple[CorpusIssuer, ...] = (
    CorpusIssuer("AAPL", "0000320193", "Apple Inc.", "technology", "operating_company", "US-GAAP baseline with cash, investments, commercial paper, and debt."),
    CorpusIssuer("MSFT", "0000789019", "Microsoft Corporation", "technology", "operating_company", "Deferred revenue, acquisitions, and intangible amortization."),
    CorpusIssuer("GOOGL", "0001652044", "Alphabet Inc.", "multi_class_technology", "operating_company", "GOOG shares this CIK; tests issuer-keyed storage and gross-profit fallback."),
    CorpusIssuer("META", "0001326801", "Meta Platforms, Inc.", "net_cash_technology", "operating_company", "Negative net debt is valid only when reported debt evidence exists."),
    CorpusIssuer("AMZN", "0001018724", "Amazon.com, Inc.", "retail_and_cloud", "inventory_company", "Inventory, leases, retail, and cloud economics in one issuer."),
    CorpusIssuer("WMT", "0000104169", "Walmart Inc.", "non_calendar_retail", "inventory_company", "January fiscal year-end tests economic-period labels."),
    CorpusIssuer("COST", "0000909832", "Costco Wholesale Corporation", "fifty_three_week_retail", "inventory_company", "52/53-week durations test cadence tolerances."),
    CorpusIssuer("CALM", "0000016160", "Cal-Maine Foods, Inc.", "small_cap_agriculture", "inventory_company", "Smaller issuer with agricultural inventory and an unusual fiscal calendar."),
    CorpusIssuer("MCD", "0000063908", "McDonald's Corporation", "franchise", "operating_company", "Negative stockholders' equity must remain valid."),
    CorpusIssuer("GM", "0001467858", "General Motors Company", "manufacturer_with_captive_finance", "inventory_company", "Consolidated debt includes captive-finance operations."),
    CorpusIssuer("CVX", "0000093410", "Chevron Corporation", "integrated_energy", "inventory_company", "Depletion, commodity inventory, and capital intensity."),
    CorpusIssuer("NEE", "0000753308", "NextEra Energy, Inc.", "regulated_utility", "operating_company", "Capital-intensive utility with sector-specific leverage."),
    CorpusIssuer("DAL", "0000027904", "Delta Air Lines, Inc.", "lease_heavy_airline", "operating_company", "Debt extraction may omit operating lease liabilities."),
    CorpusIssuer("O", "0000726728", "Realty Income Corporation", "reit", "reit", "REIT earnings need FFO/AFFO context, which is outside the current registry."),
    CorpusIssuer("JPM", "0000019617", "JPMorgan Chase & Co.", "bank", "financial_company", "Bank balance-sheet model and non-comparable generic leverage metrics."),
    CorpusIssuer("SOFI", "0001818874", "SoFi Technologies, Inc.", "non_bank_lender", "financial_company", "Warehouse and securitization funding test aggregate debt selection."),
    CorpusIssuer("PGR", "0000080661", "The Progressive Corporation", "insurer", "financial_company", "Insurer accounting makes conventional working-capital and EV metrics unsuitable."),
    CorpusIssuer("BRK-B", "0001067983", "Berkshire Hathaway Inc.", "multi_class_insurance_conglomerate", "financial_company", "SEC-native hyphenated ticker; insurance and operating subsidiaries coexist."),
    CorpusIssuer("TSM", "0001046179", "Taiwan Semiconductor Manufacturing Company Limited", "foreign_20f_ifrs_adr", "foreign_ifrs_boundary", "20-F, IFRS, TWD monetary facts, and ADR share-basis boundary."),
    CorpusIssuer("SHOP", "0001594805", "Shopify Inc.", "foreign_to_domestic_us_gaap_transition", "foreign_to_domestic_us_gaap", "Historical 40-F issuer now reporting on 10-K/10-Q with US GAAP; old 6-K interim facts remain excluded."),
)


def corpus_by_ticker() -> dict[str, CorpusIssuer]:
    """Return a new ticker index so callers cannot mutate the corpus."""

    return {issuer.ticker: issuer for issuer in CANARY_CORPUS}


def applicability_for(issuer: CorpusIssuer, metric: str) -> MetricApplicability:
    """Return the reviewed applicability, defaulting unspecified metrics to optional."""

    return APPLICABILITY_PROFILES[issuer.applicability_profile].for_metric(metric)


def validate_corpus() -> None:
    """Raise ``ValueError`` when the checked-in corpus contract is inconsistent."""

    tickers = [issuer.ticker for issuer in CANARY_CORPUS]
    ciks = [issuer.cik for issuer in CANARY_CORPUS]
    if len(tickers) != len(set(tickers)):
        raise ValueError("canary corpus tickers must be unique")
    if len(ciks) != len(set(ciks)):
        raise ValueError("canary corpus CIKs must be unique")
    for issuer in CANARY_CORPUS:
        if len(issuer.cik) != 10 or not issuer.cik.isdigit():
            raise ValueError(f"invalid pinned CIK for {issuer.ticker}: {issuer.cik}")
        if issuer.applicability_profile not in APPLICABILITY_PROFILES:
            raise ValueError(
                f"unknown applicability profile for {issuer.ticker}: "
                f"{issuer.applicability_profile}"
            )


validate_corpus()
