"""Recovering the weighted-average diluted share count when an issuer never tags it.

Alphabet is the reference case. Through mid-2024 it reported weighted-average shares
broken out by share class using XBRL dimensions, and the SEC's Company Facts API returns
only non-dimensional facts — so the divisor was simply absent while diluted EPS was fully
tagged the whole time. That stranded every interim TTM reconstruction and left one usable
EPS point per year, which surfaced as roughly six months of trailing P/E followed by six
months of gap, every year.

The count is recovered from the identity that defines it: net income / diluted EPS.
"""

from __future__ import annotations

import unittest

from copetech_sec.eps_series import TTM_EPS_DISCONTINUITY_FLAG as FLAG
from copetech_sec.eps_series import resolve_diluted_eps_ttm
from copetech_sec.financial_series import extract_financial_facts


DERIVED_FLAG = "diluted_shares_derived_from_net_income"


def _fact(value: float, start: str, end: str, filed: str, accession: str, *, form: str, fp: str) -> dict:
    return {
        "val": value,
        "start": start,
        "end": end,
        "filed": filed,
        "accn": accession,
        "form": form,
        "fy": int(end[:4]),
        "fp": fp,
    }


def _extract(concepts: dict, metric: str) -> list[dict]:
    return extract_financial_facts(
        {"cik": 1234, "entityName": "Share Fixture", "facts": {"us-gaap": concepts}},
        symbol="TEST",
        metric=metric,
        retrieved_at="2026-07-01T00:00:00+00:00",
    )


def _bundle(eps: list[dict], shares: list[dict] | None, income: list[dict] | None):
    concepts: dict = {"EarningsPerShareDiluted": {"units": {"USD/shares": eps}}}
    if shares is not None:
        concepts["WeightedAverageNumberOfDilutedSharesOutstanding"] = {"units": {"shares": shares}}
    if income is not None:
        concepts["NetIncomeLoss"] = {"units": {"USD": income}}
    return (
        _extract(concepts, "diluted_eps"),
        _extract(concepts, "diluted_shares") if shares is not None else [],
        _extract(concepts, "net_income") if income is not None else [],
    )


def _quarters():
    """A prior annual plus the two YTD windows an interim TTM bridge needs."""
    return [
        ("2024-01-01", "2024-12-31", "2025-02-05", "fy2024", "10-K", "FY"),
        ("2025-01-01", "2025-03-31", "2025-04-25", "q1-25", "10-Q", "Q1"),
        ("2024-01-01", "2024-03-31", "2025-04-25", "q1-24", "10-Q", "Q1"),
    ]


class DerivedDilutedSharesTests(unittest.TestCase):
    def test_interim_ttm_is_reconstructed_with_no_tagged_share_counts(self):
        eps, income = [], []
        for start, end, filed, accn, form, fp in _quarters():
            per_share = 8.0 if form == "10-K" else 2.0
            eps.append(_fact(per_share, start, end, filed, accn, form=form, fp=fp))
            income.append(_fact(per_share * 10_000_000_000, start, end, filed, accn, form=form, fp=fp))

        eps_rows, share_rows, income_rows = _bundle(eps, None, income)
        payload = resolve_diluted_eps_ttm(
            eps_rows,
            share_rows,
            symbol="TEST",
            split_events=[],
            net_income_rows=income_rows,
        )

        interim = [row for row in payload["observations"] if row["derived"]]
        self.assertTrue(interim, "an interim TTM must be reconstructible without tagged shares")
        self.assertIn(DERIVED_FLAG, payload["warnings"])
        # 8.0 annual + 2.0 current YTD - 2.0 prior YTD, all on one share basis.
        self.assertAlmostEqual(interim[-1]["value"], 8.0, places=6)

    def test_tagged_share_counts_win_when_present(self):
        eps, shares, income = [], [], []
        for start, end, filed, accn, form, fp in _quarters():
            per_share = 8.0 if form == "10-K" else 2.0
            eps.append(_fact(per_share, start, end, filed, accn, form=form, fp=fp))
            shares.append(_fact(10_000_000_000, start, end, filed, accn, form=form, fp=fp))
            # Deliberately inconsistent income: if it were used, the answer would move.
            income.append(_fact(per_share * 99_000_000_000, start, end, filed, accn, form=form, fp=fp))

        eps_rows, share_rows, income_rows = _bundle(eps, shares, income)
        payload = resolve_diluted_eps_ttm(
            eps_rows,
            share_rows,
            symbol="TEST",
            split_events=[],
            net_income_rows=income_rows,
        )

        self.assertNotIn(DERIVED_FLAG, payload["warnings"])
        self.assertAlmostEqual(payload["observations"][-1]["value"], 8.0, places=6)

    def test_derivation_is_skipped_when_eps_is_zero(self):
        eps, income = [], []
        for start, end, filed, accn, form, fp in _quarters():
            eps.append(_fact(0.0, start, end, filed, accn, form=form, fp=fp))
            income.append(_fact(1_000_000.0, start, end, filed, accn, form=form, fp=fp))

        eps_rows, share_rows, income_rows = _bundle(eps, None, income)
        payload = resolve_diluted_eps_ttm(
            eps_rows,
            share_rows,
            symbol="TEST",
            split_events=[],
            net_income_rows=income_rows,
        )

        # A zero EPS says nothing about the divisor, so no interim may be invented from it.
        self.assertEqual([row for row in payload["observations"] if row["derived"]], [])

    def test_a_loss_making_issuer_still_derives_a_positive_share_count(self):
        eps, income = [], []
        for start, end, filed, accn, form, fp in _quarters():
            per_share = -8.0 if form == "10-K" else -2.0
            eps.append(_fact(per_share, start, end, filed, accn, form=form, fp=fp))
            income.append(_fact(per_share * 10_000_000_000, start, end, filed, accn, form=form, fp=fp))

        eps_rows, share_rows, income_rows = _bundle(eps, None, income)
        payload = resolve_diluted_eps_ttm(
            eps_rows,
            share_rows,
            symbol="TEST",
            split_events=[],
            net_income_rows=income_rows,
        )

        interim = [row for row in payload["observations"] if row["derived"]]
        self.assertTrue(interim, "negative EPS over negative income is a valid share count")
        self.assertAlmostEqual(interim[-1]["value"], -8.0, places=6)

    def test_missing_net_income_leaves_the_gap_rather_than_guessing(self):
        eps = [
            _fact(8.0 if form == "10-K" else 2.0, start, end, filed, accn, form=form, fp=fp)
            for start, end, filed, accn, form, fp in _quarters()
        ]

        eps_rows, share_rows, income_rows = _bundle(eps, None, None)
        payload = resolve_diluted_eps_ttm(
            eps_rows,
            share_rows,
            symbol="TEST",
            split_events=[],
            net_income_rows=income_rows,
        )

        self.assertEqual([row for row in payload["observations"] if row["derived"]], [])



class TtmDiscontinuityTests(unittest.TestCase):
    """Marking trailing EPS that moved abnormally against the prior quarter.

    A P/E can fall because the price dropped or because earnings jumped, and the chart
    cannot tell you which. GOOG posted $9.11 diluted for one quarter against $2.31 a year
    earlier and its trailing multiple halved on the denominator alone.
    """

    @staticmethod
    def _series(values: list[float]) -> dict:
        eps, income = [], []
        for index, per_share in enumerate(values):
            year = 2020 + index
            eps.append(
                _fact(per_share, f"{year}-01-01", f"{year}-12-31", f"{year + 1}-02-05",
                      f"fy{year}", form="10-K", fp="FY")
            )
            income.append(
                _fact(per_share * 1_000_000_000, f"{year}-01-01", f"{year}-12-31",
                      f"{year + 1}-02-05", f"fy{year}", form="10-K", fp="FY")
            )
        eps_rows, share_rows, income_rows = _bundle(eps, None, income)
        return resolve_diluted_eps_ttm(
            eps_rows, share_rows, symbol="TEST", split_events=[], net_income_rows=income_rows
        )

    def test_a_doubling_is_flagged(self):
        payload = self._series([2.00, 2.10, 4.60])

        flags = [row["qualityFlags"] for row in payload["observations"]]
        self.assertNotIn(FLAG, flags[1], "a 5% move is ordinary")
        self.assertIn(FLAG, flags[2])
        self.assertIn(FLAG, payload["warnings"])

    def test_ordinary_growth_is_not_flagged(self):
        payload = self._series([2.00, 2.20, 2.45, 2.70])

        for row in payload["observations"]:
            self.assertNotIn(FLAG, row["qualityFlags"])
        self.assertNotIn(FLAG, payload["warnings"])

    def test_a_collapse_is_flagged_as_well_as_a_spike(self):
        payload = self._series([4.00, 1.00])

        self.assertIn(FLAG, payload["observations"][1]["qualityFlags"])

    def test_crossing_from_a_loss_into_a_profit_is_flagged(self):
        # A relative change across zero is meaningless, so the sign flip is the signal.
        payload = self._series([-0.50, 0.55])

        self.assertIn(FLAG, payload["observations"][1]["qualityFlags"])

    def test_the_first_observation_is_never_flagged(self):
        payload = self._series([9.99])

        self.assertNotIn(FLAG, payload["observations"][0]["qualityFlags"])

if __name__ == "__main__":
    unittest.main()
