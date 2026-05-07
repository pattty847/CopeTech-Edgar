from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from analysis.phase0_thesis.cusip_map import CusipMapper, sec_name_match
from analysis.phase0_thesis.db import connect, init_schema


class FakeFigiClient:
    def __init__(self, mapping: dict[str, str | None]):
        self.mapping = mapping
        self.calls: list[list[str]] = []
        self.api_key = None

    def map_cusips(self, cusips: list[str]) -> dict[str, str | None]:
        self.calls.append(cusips)
        return {cusip: self.mapping.get(cusip) for cusip in cusips}


class Phase0CusipMapTests(unittest.TestCase):
    def test_cache_hit_avoids_requery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            con = connect(Path(temp_dir) / "phase0.duckdb")
            init_schema(con)
            fake_client = FakeFigiClient({"000000002": "FAKE"})
            mapper = CusipMapper(con, fake_client)
            holdings = [{"cusip": "000000002", "issuer_name": "FAKE INC"}]

            mapper.map_holdings(holdings, company_rows=[])
            mapper.map_holdings(holdings, company_rows=[])

            self.assertEqual(fake_client.calls, [["000000002"]])
            con.close()

    def test_sec_name_fallback_assigns_confidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            con = connect(Path(temp_dir) / "phase0.duckdb")
            init_schema(con)
            mapper = CusipMapper(con, FakeFigiClient({"000000001": None}))

            result = mapper.map_holdings(
                [{"cusip": "000000001", "issuer_name": "APPLE INC"}],
                company_rows=[{"title": "Apple Inc.", "ticker": "AAPL"}],
            )

            confidence = con.execute(
                "SELECT mapping_confidence FROM cusip_ticker_map WHERE cusip = '000000001'"
            ).fetchone()[0]
            self.assertEqual(result["000000001"], "AAPL")
            self.assertEqual(confidence, "sec_name_match")
            con.close()

    def test_unmapped_confidence_is_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            con = connect(Path(temp_dir) / "phase0.duckdb")
            init_schema(con)
            mapper = CusipMapper(con, FakeFigiClient({"999999999": None}))

            mapper.map_holdings([{"cusip": "999999999", "issuer_name": "NO MATCH"}], company_rows=[])

            ticker, confidence = con.execute(
                "SELECT ticker, mapping_confidence FROM cusip_ticker_map WHERE cusip = '999999999'"
            ).fetchone()
            self.assertIsNone(ticker)
            self.assertEqual(confidence, "unmapped")
            con.close()

    def test_manual_override_wins_over_cached_vendor_ticker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            con = connect(Path(temp_dir) / "phase0.duckdb")
            init_schema(con)
            con.execute("""
                INSERT INTO cusip_ticker_map VALUES
                ('46090E103', 'NDQ', 'INVESCO QQQ TR', 'openfigi', 'old')
            """)
            mapper = CusipMapper(con, FakeFigiClient({}))

            result = mapper.map_holdings([{"cusip": "46090E103", "issuer_name": "INVESCO QQQ TR"}], company_rows=[])

            ticker, confidence = con.execute(
                "SELECT ticker, mapping_confidence FROM cusip_ticker_map WHERE cusip = '46090E103'"
            ).fetchone()
            self.assertEqual(result["46090E103"], "QQQ")
            self.assertEqual(ticker, "QQQ")
            self.assertEqual(confidence, "manual_override")
            con.close()

    def test_sec_name_match_scores_synthetic_known_issuer(self) -> None:
        self.assertEqual(
            sec_name_match("TESLA INC", [{"title": "Tesla, Inc.", "ticker": "TSLA"}]),
            "TSLA",
        )

    def test_anonymous_openfigi_batches_are_limited_to_ten_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            con = connect(Path(temp_dir) / "phase0.duckdb")
            init_schema(con)
            fake_client = FakeFigiClient({})
            mapper = CusipMapper(con, fake_client)
            holdings = [{"cusip": f"{index:09d}", "issuer_name": f"ISSUER {index}"} for index in range(23)]

            mapper.map_holdings(holdings, company_rows=[])

            self.assertEqual([len(call) for call in fake_client.calls], [10, 10, 3])
            con.close()


if __name__ == "__main__":
    unittest.main()
