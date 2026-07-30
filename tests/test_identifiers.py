import unittest

from copetech_sec.identifiers import Accession, Cik, Ticker


class CikTests(unittest.TestCase):
    def test_normalizes_to_ten_digits(self):
        cik = Cik("320193")

        self.assertEqual(cik, "0000320193")
        self.assertEqual(cik.archive_path, "320193")

    def test_rejects_non_digits_and_zero(self):
        for value in ("", "ABC", "0", "0000000000", "12345678901"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                Cik(value)


class AccessionTests(unittest.TestCase):
    def test_accepts_dashed_and_compact_forms(self):
        dashed = Accession("0000320193-26-000001")
        compact = Accession("000032019326000001")

        self.assertEqual(dashed, compact)
        self.assertEqual(dashed.compact, "000032019326000001")
        self.assertEqual(dashed.submitter_cik, "0000320193")

    def test_rejects_path_and_partial_values(self):
        for value in ("../0000320193-26-000001", "0000320193-26-1", "abc"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                Accession(value)


class TickerTests(unittest.TestCase):
    def test_normalizes_common_sec_ticker_shapes(self):
        self.assertEqual(Ticker(" brk-b "), "BRK-B")
        self.assertEqual(Ticker("bf.b"), "BF.B")

    def test_rejects_path_like_values(self):
        for value in ("", "../AAPL", "AAPL/USD", "AAPL US"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                Ticker(value)
