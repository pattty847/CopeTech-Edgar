#!/usr/bin/env python3
"""Regression tests for `Form4Processor.parse_form4_xml`.

The XML parser was the highest-risk code in the package and had no direct test coverage:
`test_form4_signals.py` exercises normalization, dedupe, aggregation and caching, all of
which run on already-parsed dicts. Every defect covered here was reproduced against real
SEC documents before being fixed.

Fixtures use the real ownership schema (root `<ownershipDocument>`, no XML namespace,
`<x><value>…</value></x>` wrappers), verified against
https://www.sec.gov/Archives/edgar/data/320193/000114036126025622/form4.xml
"""

import unittest

from copetech_sec.form4_processor import Form4Processor


def _processor() -> Form4Processor:
    return Form4Processor(document_handler=None, fetch_filings_func=None)


# Shape of the real Apple Form 4 above: an RSU settlement reported as three rows — the
# non-derivative shares acquired (M/A), the shares withheld for tax (F/D), and the
# derivative RSUs given up (M/D).
RSU_SETTLEMENT = """<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0609</schemaVersion>
  <documentType>4</documentType>
  <periodOfReport>2026-06-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001780525</rptOwnerCik>
      <rptOwnerName>Newstead Jennifer</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>SVP, GC and Secretary</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-06-15</value></transactionDate>
      <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>M</transactionCode><equitySwapInvolved>0</equitySwapInvolved></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>30104</value></transactionShares>
        <transactionPricePerShare><value>0</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>100000</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-06-15</value></transactionDate>
      <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>F</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>16238</value></transactionShares>
        <transactionPricePerShare><value>296.42</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>83762</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
      <footnoteId id="F2"/>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Restricted Stock Units</value></securityTitle>
      <transactionDate><value>2026-06-15</value></transactionDate>
      <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>M</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>30104</value></transactionShares>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <underlyingSecurity>
        <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
        <underlyingSecurityShares><value>30104</value></underlyingSecurityShares>
      </underlyingSecurity>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>0</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
      <footnoteId id="F1"/>
    </derivativeTransaction>
  </derivativeTable>
  <footnotes>
    <footnote id="F1">Each restricted stock unit represents the right to receive one share of common stock.</footnote>
    <footnote id="F2">Shares withheld by Apple to satisfy tax withholding requirements on vesting of RSUs. No shares were sold.</footnote>
  </footnotes>
</ownershipDocument>
"""


JOINT_FILING = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Example Corp</issuerName>
    <issuerTradingSymbol>EXMP</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0000001111</rptOwnerCik><rptOwnerName>SMITH JANE</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
  </reportingOwner>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0000002222</rptOwnerCik><rptOwnerName>SMITH FAMILY TRUST</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-01-15</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>150.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


def _minimal_form4(*, shares: str = "1000", price: str = "150.00", code: str = "P", acq_disp: str = "A") -> str:
    return f"""<ownershipDocument>
  <documentType>4</documentType>
  <issuer><issuerCik>0000320193</issuerCik><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0000001111</rptOwnerCik><rptOwnerName>OWNER ONE</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable><nonDerivativeTransaction>
    <securityTitle><value>Common Stock</value></securityTitle>
    <transactionDate><value>2026-01-15</value></transactionDate>
    <transactionCoding><transactionCode>{code}</transactionCode></transactionCoding>
    <transactionAmounts>
      <transactionShares><value>{shares}</value></transactionShares>
      <transactionPricePerShare><value>{price}</value></transactionPricePerShare>
      <transactionAcquiredDisposedCode><value>{acq_disp}</value></transactionAcquiredDisposedCode>
    </transactionAmounts>
  </nonDerivativeTransaction></nonDerivativeTable>
</ownershipDocument>"""


class MultipleReportingOwnerTests(unittest.TestCase):
    """A Form 4 may name up to 10 reporting owners (Ownership XML Technical Specification,
    `reportingOwner` maxOccurs="10") — joint filings, co-filing 10% owners, trusts,
    estates. The parser read only the first via
    `findtext('.//reportingOwner/reportingOwnerId/rptOwnerCik')` and dropped the others.

    The schema carries no per-row owner attribution, so the fix must expose all owners
    *without* duplicating the transaction rows per owner — that would multiply reported
    shares and value by the owner count."""

    def test_every_reporting_owner_is_exposed(self):
        rows = _processor().parse_form4_xml(JOINT_FILING)

        owners = rows[0]["reporting_owners"]
        self.assertEqual(
            [owner["owner_cik"] for owner in owners], ["0000001111", "0000002222"]
        )
        self.assertEqual(rows[0]["owner_count"], 2)
        self.assertTrue(rows[0]["is_joint_filing"])

    def test_each_owner_keeps_its_own_relationship(self):
        owners = _processor().parse_form4_xml(JOINT_FILING)[0]["reporting_owners"]
        by_cik = {owner["owner_cik"]: owner for owner in owners}

        self.assertEqual(by_cik["0000001111"]["owner_position"], "Director")
        self.assertEqual(by_cik["0000002222"]["owner_position"], "10% Owner")
        self.assertTrue(by_cik["0000002222"]["owner_is_ten_percent_owner"])
        self.assertFalse(by_cik["0000002222"]["owner_is_director"])

    def test_transactions_are_not_duplicated_per_owner(self):
        # The joint fixture reports ONE transaction of 1,000 shares at $150. A row per
        # owner-pair would report 2,000 shares / $300,000 of activity that never happened.
        rows = _processor().parse_form4_xml(JOINT_FILING)

        self.assertEqual(len(rows), 1, "one table row must yield one record")
        self.assertEqual(rows[0]["shares"], 1000.0)
        self.assertEqual(sum(row["value"] for row in rows), 150_000.0)

    def test_primary_owner_fields_stay_populated_for_backward_compatibility(self):
        row = _processor().parse_form4_xml(JOINT_FILING)[0]
        self.assertEqual(row["owner_cik"], "0000001111")
        self.assertEqual(row["owner_name"], "SMITH JANE")
        self.assertEqual(row["owner_position"], "Director")

    def test_single_owner_filing_is_unchanged(self):
        rows = _processor().parse_form4_xml(_minimal_form4())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["owner_cik"], "0000001111")
        self.assertEqual(rows[0]["owner_count"], 1)
        self.assertFalse(rows[0]["is_joint_filing"])


class TransactionDirectionTests(unittest.TestCase):
    """Direction must come from the filing's own `transactionAcquiredDisposedCode`, not from
    a code lookup table. Deriving it from the transaction code put both legs of an option
    exercise (M) on the acquisition side, so the same shares were counted twice as buys and
    net insider value was materially overstated."""

    def test_derivative_leg_of_an_exercise_is_a_disposition(self):
        rows = _processor().parse_form4_xml(RSU_SETTLEMENT)
        derivative = next(row for row in rows if row["is_derivative"])

        self.assertEqual(derivative["transaction_code"], "M")
        self.assertEqual(derivative["acq_disp_code"], "D")
        self.assertFalse(derivative["is_acquisition"])
        self.assertTrue(derivative["is_disposition"])

    def test_non_derivative_leg_of_the_same_exercise_is_an_acquisition(self):
        rows = _processor().parse_form4_xml(RSU_SETTLEMENT)
        acquired = next(
            row for row in rows if not row["is_derivative"] and row["transaction_code"] == "M"
        )
        self.assertTrue(acquired["is_acquisition"])
        self.assertFalse(acquired["is_disposition"])

    def test_direction_neutral_codes_follow_the_filing(self):
        # G (gift), J (other), W (will/descent), C (conversion) all run either way.
        for code in ("G", "J", "W", "C", "Z", "I", "K"):
            with self.subTest(code=code):
                disposed = _processor().parse_form4_xml(
                    _minimal_form4(code=code, acq_disp="D")
                )[0]
                self.assertTrue(disposed["is_disposition"], f"{code} with A/D=D must be a disposition")
                self.assertFalse(disposed["is_acquisition"])

                acquired = _processor().parse_form4_xml(
                    _minimal_form4(code=code, acq_disp="A")
                )[0]
                self.assertTrue(acquired["is_acquisition"], f"{code} with A/D=A must be an acquisition")
                self.assertFalse(acquired["is_disposition"])

    def test_direction_is_never_both(self):
        for code in ("P", "S", "M", "F", "A", "D", "G", "X"):
            for acq_disp in ("A", "D"):
                with self.subTest(code=code, acq_disp=acq_disp):
                    row = _processor().parse_form4_xml(
                        _minimal_form4(code=code, acq_disp=acq_disp)
                    )[0]
                    self.assertNotEqual(row["is_acquisition"], row["is_disposition"])

    def test_falls_back_to_code_hint_when_direction_is_absent(self):
        xml = _minimal_form4(code="P").replace(
            "<transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>", ""
        )
        row = _processor().parse_form4_xml(xml)[0]
        self.assertTrue(row["is_acquisition"])

    def test_w_code_label_is_the_sec_definition(self):
        # 'W' is acquisition/disposition by will or the laws of descent and distribution,
        # not "Warrant Exercise".
        self.assertIn("will", Form4Processor.TRANSACTION_CODE_MAP["W"].lower())

    def test_code_table_covers_every_sec_form4_code(self):
        expected = set("PSVADFIMCEHOXGJKLUWZ")
        self.assertEqual(expected - set(Form4Processor.TRANSACTION_CODE_MAP), set())


class NumericParsingTests(unittest.TestCase):
    """`raw.replace('.', '', 1).isdigit()` silently produced 0.0 for anything that was not
    a bare unsigned decimal — whitespace-padded values, negatives, thousands separators and
    scientific notation all became zero shares at zero dollars."""

    def test_whitespace_padded_values_parse(self):
        xml = _minimal_form4().replace(
            "<value>1000</value>", "<value>\n        1000\n      </value>"
        ).replace("<value>150.00</value>", "<value>\n   150.00\n  </value>")
        row = _processor().parse_form4_xml(xml)[0]

        self.assertEqual(row["shares"], 1000.0)
        self.assertEqual(row["price_per_share"], 150.0)
        self.assertEqual(row["value"], 150000.0)

    def test_thousands_separators_parse(self):
        row = _processor().parse_form4_xml(_minimal_form4(shares="1,000"))[0]
        self.assertEqual(row["shares"], 1000.0)

    def test_negative_values_are_preserved_not_zeroed(self):
        row = _processor().parse_form4_xml(_minimal_form4(shares="-500"))[0]
        self.assertEqual(row["shares"], -500.0)

    def test_unparseable_value_is_none_not_zero(self):
        # A parse failure must be distinguishable from a genuine zero.
        row = _processor().parse_form4_xml(_minimal_form4(shares="not-a-number"))[0]
        self.assertIsNone(row["shares"])

    def test_missing_price_yields_none_value_not_zero_dollars(self):
        xml = _minimal_form4(code="A").replace(
            "<transactionPricePerShare><value>150.00</value></transactionPricePerShare>", ""
        )
        row = _processor().parse_form4_xml(xml)[0]

        self.assertIsNone(row["price_per_share"])
        self.assertIsNone(row["value"], "an unpriced award is unknown value, not $0")

    def test_genuine_zero_price_is_kept_as_zero(self):
        row = _processor().parse_form4_xml(_minimal_form4(price="0"))[0]
        self.assertEqual(row["price_per_share"], 0.0)
        self.assertEqual(row["value"], 0.0)


class HoldingRowTests(unittest.TestCase):
    """Only `*Transaction` elements were parsed, so `nonDerivativeHolding` and
    `derivativeHolding` rows were dropped. Form 3 consists almost entirely of holdings."""

    HOLDINGS_ONLY = """<ownershipDocument>
      <documentType>3</documentType>
      <issuer><issuerCik>0000320193</issuerCik><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
      <reportingOwner>
        <reportingOwnerId><rptOwnerCik>0000001111</rptOwnerCik><rptOwnerName>NEW OFFICER</rptOwnerName></reportingOwnerId>
        <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>CFO</officerTitle></reportingOwnerRelationship>
      </reportingOwner>
      <nonDerivativeTable><nonDerivativeHolding>
        <securityTitle><value>Common Stock</value></securityTitle>
        <postTransactionAmounts><sharesOwnedFollowingTransaction><value>50000</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
        <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
      </nonDerivativeHolding></nonDerivativeTable>
      <derivativeTable><derivativeHolding>
        <securityTitle><value>Stock Option</value></securityTitle>
        <conversionOrExercisePrice><value>120.50</value></conversionOrExercisePrice>
        <underlyingSecurity><underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle><underlyingSecurityShares><value>10000</value></underlyingSecurityShares></underlyingSecurity>
      </derivativeHolding></derivativeTable>
    </ownershipDocument>"""

    def test_holdings_are_parsed(self):
        rows = _processor().parse_form4_xml(self.HOLDINGS_ONLY)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["is_holding"] for row in rows))

    def test_holding_carries_position_and_terms(self):
        rows = _processor().parse_form4_xml(self.HOLDINGS_ONLY)
        common = next(row for row in rows if not row["is_derivative"])
        option = next(row for row in rows if row["is_derivative"])

        self.assertEqual(common["shares_owned_after"], 50000.0)
        self.assertEqual(option["conversion_exercise_price"], 120.50)
        self.assertEqual(option["underlying_shares"], 10000.0)

    def test_transactions_are_not_flagged_as_holdings(self):
        rows = _processor().parse_form4_xml(RSU_SETTLEMENT)
        self.assertFalse(any(row["is_holding"] for row in rows))

    def test_holdings_are_excluded_from_the_signal_event_stream(self):
        # Position statements must not inflate transaction counts in the signal payload.
        processor = _processor()
        rows = processor.parse_form4_xml(self.HOLDINGS_ONLY)
        self.assertTrue(rows)
        self.assertEqual(processor._classify_signal(rows[0]), "holding")


class UniformRowSchemaTests(unittest.TestCase):
    """Derivative rows lacked `price_per_share`/`value` while non-derivative rows lacked
    `conversion_exercise_price`, so consumers indexing a column raised KeyError on half the
    rows and `pandas.DataFrame` produced ragged, NaN-filled columns."""

    def test_all_rows_share_one_key_set(self):
        rows = _processor().parse_form4_xml(RSU_SETTLEMENT)
        self.assertGreater(len(rows), 1)

        key_sets = [frozenset(row) for row in rows]
        self.assertEqual(len(set(key_sets)), 1, "row schemas diverge between row types")

    def test_derivative_rows_expose_price_columns(self):
        derivative = next(row for row in _processor().parse_form4_xml(RSU_SETTLEMENT) if row["is_derivative"])
        for column in ("price_per_share", "value", "conversion_exercise_price"):
            self.assertIn(column, derivative)


class ProvenanceTests(unittest.TestCase):
    """Footnotes, the Rule 10b5-1 flag, form type and reporting period were all parsed away."""

    def test_footnotes_are_attached_to_their_rows(self):
        rows = _processor().parse_form4_xml(RSU_SETTLEMENT)
        withheld = next(row for row in rows if row["transaction_code"] == "F")

        self.assertEqual(withheld["footnote_ids"], ["F2"])
        self.assertIn("No shares were sold", withheld["footnotes"][0])

    def test_document_metadata_is_preserved(self):
        row = _processor().parse_form4_xml(RSU_SETTLEMENT)[0]
        self.assertEqual(row["document_type"], "4")
        self.assertEqual(row["period_of_report"], "2026-06-15")
        self.assertEqual(row["transaction_form_type"], "4")

    def test_rule_10b5_1_flag_is_read_from_the_document_level_element(self):
        # Per the EDGAR Ownership XML Technical Specification v5.5, `aff10b5One` is a
        # required document-level element on Forms 4/5 — a sibling of <issuer> and
        # <reportingOwner>. Classification previously searched `security_title` for
        # "10b5-1", which holds values like "Common Stock", so the branch was unreachable.
        xml = _minimal_form4(code="S", acq_disp="D").replace(
            "<nonDerivativeTable>", "<aff10b5One>1</aff10b5One><nonDerivativeTable>"
        )
        row = _processor().parse_form4_xml(xml)[0]

        self.assertTrue(row["rule_10b5_1_plan"])
        self.assertEqual(_processor()._classify_signal(row), "planned_sale_10b5_1")

    def test_rule_10b5_1_accepts_the_boolean_spelling_filers_use(self):
        # The schema types it xs:boolean; real filings emit "false"/"true" while SEC's own
        # sample documents emit "1"/"0".
        for raw, expected in (("true", True), ("1", True), ("false", False), ("0", False)):
            with self.subTest(raw=raw):
                xml = _minimal_form4(code="S", acq_disp="D").replace(
                    "<nonDerivativeTable>", f"<aff10b5One>{raw}</aff10b5One><nonDerivativeTable>"
                )
                self.assertIs(_processor().parse_form4_xml(xml)[0]["rule_10b5_1_plan"], expected)

    def test_document_level_remarks_are_preserved(self):
        xml = _minimal_form4().replace(
            "</ownershipDocument>", "<remarks>Filed to correct a prior report.</remarks></ownershipDocument>"
        )
        self.assertEqual(
            _processor().parse_form4_xml(xml)[0]["remarks"], "Filed to correct a prior report."
        )

    def test_plan_sale_detected_from_footnote_text(self):
        xml = _minimal_form4(code="S", acq_disp="D").replace(
            "</nonDerivativeTransaction>",
            '<footnoteId id="F1"/></nonDerivativeTransaction>',
        ).replace(
            "</ownershipDocument>",
            '<footnotes><footnote id="F1">Sale under a Rule 10b5-1 trading plan adopted on 2025-11-03.</footnote></footnotes></ownershipDocument>',
        )
        row = _processor().parse_form4_xml(xml)[0]
        self.assertEqual(_processor()._classify_signal(row), "planned_sale_10b5_1")

    def test_ordinary_sale_is_not_misclassified_as_a_planned_sale(self):
        row = _processor().parse_form4_xml(_minimal_form4(code="S", acq_disp="D"))[0]
        self.assertEqual(_processor()._classify_signal(row), "open_market_sell")


class RoleWeightTests(unittest.TestCase):
    def test_ten_percent_owner_weight_is_reachable(self):
        # The branch compared '10% owner' against the original-case role string, but the
        # parser emits '10% Owner', so it never matched.
        self.assertEqual(_processor()._role_weight("10% Owner"), 0.65)

    def test_officer_title_from_a_joint_filing_is_weighted(self):
        self.assertEqual(_processor()._role_weight("Officer (Chief Executive Officer)"), 1.0)


class MalformedInputTests(unittest.TestCase):
    def test_malformed_xml_returns_empty_list(self):
        self.assertEqual(_processor().parse_form4_xml("<ownershipDocument><unclosed>"), [])

    def test_empty_document_returns_empty_list(self):
        self.assertEqual(_processor().parse_form4_xml("<ownershipDocument/>"), [])

    def test_missing_reporting_owner_does_not_crash(self):
        xml = """<ownershipDocument>
          <issuer><issuerCik>1</issuerCik><issuerTradingSymbol>X</issuerTradingSymbol></issuer>
          <nonDerivativeTable><nonDerivativeTransaction>
            <securityTitle><value>Common Stock</value></securityTitle>
            <transactionDate><value>2026-01-15</value></transactionDate>
            <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
            <transactionAmounts>
              <transactionShares><value>10</value></transactionShares>
              <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
          </nonDerivativeTransaction></nonDerivativeTable>
        </ownershipDocument>"""
        rows = _processor().parse_form4_xml(xml)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["owner_cik"], "N/A")


if __name__ == "__main__":
    unittest.main()
