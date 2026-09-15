from analysis.financial_series_audit.corpus import (
    APPLICABILITY_PROFILES,
    CANARY_CORPUS,
    applicability_for,
    corpus_by_ticker,
    validate_corpus,
)


def test_canary_corpus_is_deterministic_and_pins_unique_issuer_identity() -> None:
    validate_corpus()

    assert len(CANARY_CORPUS) == 20
    assert len({issuer.ticker for issuer in CANARY_CORPUS}) == 20
    assert len({issuer.cik for issuer in CANARY_CORPUS}) == 20
    assert CANARY_CORPUS[0].ticker == "AAPL"
    assert CANARY_CORPUS[-1].ticker == "SHOP"


def test_canary_corpus_contains_the_reviewed_accounting_shapes() -> None:
    by_ticker = corpus_by_ticker()

    assert by_ticker["GOOGL"].cik == "0001652044"
    assert by_ticker["BRK-B"].archetype == "multi_class_insurance_conglomerate"
    assert by_ticker["SOFI"].archetype == "non_bank_lender"
    assert by_ticker["JPM"].applicability_profile == "financial_company"
    assert by_ticker["O"].applicability_profile == "reit"
    assert by_ticker["TSM"].archetype == "foreign_20f_ifrs_adr"
    assert by_ticker["SHOP"].archetype == "foreign_40f_ifrs"
    assert by_ticker["COST"].archetype == "fifty_three_week_retail"
    assert by_ticker["CALM"].archetype == "small_cap_agriculture"


def test_unspecified_metric_applicability_is_optional() -> None:
    by_ticker = corpus_by_ticker()

    assert applicability_for(by_ticker["AAPL"], "inventory") == "optional"
    assert applicability_for(by_ticker["TSM"], "revenue") == "optional"
    assert (
        applicability_for(by_ticker["TSM"], "stockholders_equity")
        == "unsupported_taxonomy"
    )
    assert (
        applicability_for(by_ticker["SHOP"], "stockholders_equity")
        == "unsupported_taxonomy"
    )


def test_profiles_distinguish_expected_from_not_comparable_metrics() -> None:
    by_ticker = corpus_by_ticker()

    assert applicability_for(by_ticker["AAPL"], "revenue") == "expected"
    assert applicability_for(by_ticker["WMT"], "inventory") == "expected"
    assert applicability_for(by_ticker["JPM"], "total_assets") == "expected"
    assert applicability_for(by_ticker["SOFI"], "net_debt") == "not_comparable"
    assert applicability_for(by_ticker["PGR"], "working_capital") == "not_comparable"
    assert applicability_for(by_ticker["O"], "ebitda") == "not_comparable"


def test_every_issuer_references_a_known_profile() -> None:
    assert all(
        issuer.applicability_profile in APPLICABILITY_PROFILES
        for issuer in CANARY_CORPUS
    )
