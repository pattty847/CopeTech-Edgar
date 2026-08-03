from __future__ import annotations

import unittest

from copetech_sec.financial_series import (
    extract_financial_facts,
    resolve_financial_series,
)


def fact(
    value: float,
    start: str,
    end: str,
    filed: str,
    accession: str,
    *,
    form: str = "10-Q",
    fy: int = 2025,
    fp: str = "Q1",
) -> dict:
    return {
        "val": value,
        "start": start,
        "end": end,
        "filed": filed,
        "accn": accession,
        "form": form,
        "fy": fy,
        "fp": fp,
        "frame": None,
    }


def ocf_facts(entries: list[dict]) -> dict:
    return {
        "cik": 320193,
        "entityName": "Fixture Corp",
        "facts": {
            "us-gaap": {
                "NetCashProvidedByUsedInOperatingActivities": {
                    "units": {"USD": entries}
                }
            }
        },
    }


# A fiscal year reported the way real cash-flow statements are: Q1 standalone,
# then six- and nine-month cumulatives, then the 10-K annual window.
FY = [
    fact(10.0, "2025-01-01", "2025-03-31", "2025-04-25", "q1", fp="Q1"),
    fact(25.0, "2025-01-01", "2025-06-30", "2025-07-25", "h1", fp="Q2"),
    fact(45.0, "2025-01-01", "2025-09-30", "2025-10-25", "nm", fp="Q3"),
    fact(70.0, "2025-01-01", "2025-12-31", "2026-02-01", "fy", form="10-K", fp="FY"),
]


class YtdCadenceTests(unittest.TestCase):
    def _series(self, frequency: str, basis: str = "canonical") -> dict:
        rows = extract_financial_facts(
            ocf_facts(FY), symbol="TEST", metric="operating_cash_flow"
        )
        return resolve_financial_series(
            rows,
            symbol="TEST",
            metric="operating_cash_flow",
            frequency=frequency,
            basis=basis,
        )

    def test_q2_and_q3_are_derived_by_differencing_cumulative_windows(self):
        series = self._series("quarterly")
        by_window = {
            (row["periodStart"], row["periodEnd"]): row
            for row in series["observations"]
        }
        q2 = by_window[("2025-04-01", "2025-06-30")]
        q3 = by_window[("2025-07-01", "2025-09-30")]
        self.assertEqual(q2["value"], 15.0)
        self.assertEqual(q3["value"], 20.0)
        for row in (q2, q3):
            self.assertTrue(row["derived"])
            self.assertIn("derived_from_ytd", row["qualityFlags"])
        # Q3 is unknowable before BOTH cumulative windows were filed.
        self.assertEqual(q3["availableAt"], "2025-10-25")

    def test_q4_from_annual_now_composes_with_ytd_derived_quarters(self):
        series = self._series("quarterly")
        by_window = {
            (row["periodStart"], row["periodEnd"]): row
            for row in series["observations"]
        }
        q4 = by_window[("2025-10-01", "2025-12-31")]
        self.assertEqual(q4["value"], 25.0)
        self.assertIn("derived_q4", q4["qualityFlags"])

    def test_ttm_sums_the_full_derived_quarter_chain(self):
        series = self._series("ttm")
        (ttm,) = series["observations"]
        self.assertEqual(ttm["value"], 70.0)
        self.assertEqual(ttm["periodStart"], "2025-01-01")
        self.assertEqual(ttm["periodEnd"], "2025-12-31")

    def test_reported_basis_excludes_every_derived_window(self):
        series = self._series("quarterly", basis="reported")
        self.assertEqual(
            [(row["periodStart"], row["periodEnd"]) for row in series["observations"]],
            [("2025-01-01", "2025-03-31")],
        )

    def test_negative_residual_is_flagged_implausible(self):
        entries = [
            fact(10.0, "2025-01-01", "2025-03-31", "2025-04-25", "q1", fp="Q1"),
            fact(8.0, "2025-01-01", "2025-06-30", "2025-07-25", "h1", fp="Q2"),
        ]
        rows = extract_financial_facts(
            ocf_facts(entries), symbol="TEST", metric="operating_cash_flow"
        )
        series = resolve_financial_series(
            rows, symbol="TEST", metric="operating_cash_flow", frequency="quarterly"
        )
        q2 = next(
            row for row in series["observations"] if row["periodStart"] == "2025-04-01"
        )
        self.assertEqual(q2["value"], -2.0)
        self.assertIn("implausible_ytd_residual", q2["qualityFlags"])
        self.assertEqual(q2["confidence"], 0.55)

    def test_metrics_without_ytd_cadence_are_untouched(self):
        payload = {
            "cik": 320193,
            "entityName": "Fixture Corp",
            "facts": {
                "us-gaap": {
                    "Revenues": {"units": {"USD": FY}},
                }
            },
        }
        rows = extract_financial_facts(payload, symbol="TEST", metric="revenue")
        series = resolve_financial_series(
            rows, symbol="TEST", metric="revenue", frequency="quarterly"
        )
        self.assertFalse(
            any("derived_from_ytd" in row["qualityFlags"] for row in series["observations"])
        )


if __name__ == "__main__":
    unittest.main()
