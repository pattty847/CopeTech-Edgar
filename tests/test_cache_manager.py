#!/usr/bin/env python3
"""Regression test for the company_info cache path.

Bug history: `_get_cache_path` validates `data_type` against `SUBDIRS` before
the per-type branch runs. `company_info` had a dedicated branch lower in the
function but was missing from `SUBDIRS`, so the validator rejected it and the
branch was dead code. Callers swallowed the ValueError and silently returned
None — every `company_info` cache read/write became a no-op.
"""

from __future__ import annotations

import tempfile
import unittest

from copetech_sec.cache_manager import SecCacheManager


class CompanyInfoCachePathTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.manager = SecCacheManager(cache_dir=self.tmpdir)

    def test_company_info_is_in_subdirs_allowlist(self):
        self.assertIn("company_info", SecCacheManager.SUBDIRS)

    def test_get_cache_path_for_company_info_returns_path_under_submissions(self):
        path = self.manager._get_cache_path("company_info", ticker="AAPL")
        self.assertIn("submissions", path)
        self.assertIn("AAPL_info_", path)
        self.assertTrue(path.endswith(".json"))

    def test_save_and_load_round_trip(self):
        import asyncio

        async def run():
            payload = {"ticker": "AAPL", "cik": "0000320193", "name": "Apple Inc."}
            await self.manager.save_data("AAPL", "company_info", payload, cik="0000320193")
            loaded = await self.manager.load_data("AAPL", "company_info")
            return loaded

        loaded = asyncio.run(run())
        self.assertEqual(loaded, {"ticker": "AAPL", "cik": "0000320193", "name": "Apple Inc."})

    def test_invalid_data_type_still_rejected(self):
        with self.assertRaises(ValueError):
            self.manager._get_cache_path("not_a_real_type", ticker="AAPL")

    def test_forms_cache_path_includes_days_back_when_provided(self):
        path_1d = self.manager._get_cache_path("forms", ticker="AAPL", form_type="4,4/A", days_back=1)
        path_180d = self.manager._get_cache_path("forms", ticker="AAPL", form_type="4,4/A", days_back=180)
        self.assertIn("AAPL_4_4A_1d_", path_1d)
        self.assertIn("AAPL_4_4A_180d_", path_180d)
        self.assertNotEqual(path_1d, path_180d)

    def test_forms_load_prefers_window_cache_but_falls_back_to_legacy_cache(self):
        import asyncio
        from pathlib import Path

        async def run():
            legacy_path = Path(self.manager._get_cache_path("forms", ticker="AAPL", form_type="4,4/A"))
            self.manager._write_cache_file(str(legacy_path), [{"accession_no": "legacy"}])
            legacy_loaded = await self.manager.load_data("AAPL", "forms", form_type="4,4/A", days_back=180)

            await self.manager.save_data(
                "AAPL",
                "forms",
                [{"accession_no": "window"}],
                form_type="4,4/A",
                days_back=180,
            )
            window_loaded = await self.manager.load_data("AAPL", "forms", form_type="4,4/A", days_back=180)
            one_day_loaded = await self.manager.load_data("AAPL", "forms", form_type="4,4/A", days_back=1)
            return legacy_loaded, window_loaded, one_day_loaded

        legacy_loaded, window_loaded, one_day_loaded = asyncio.run(run())
        self.assertEqual(legacy_loaded, [{"accession_no": "legacy"}])
        self.assertEqual(window_loaded, [{"accession_no": "window"}])
        self.assertEqual(one_day_loaded, [{"accession_no": "legacy"}])


if __name__ == "__main__":
    unittest.main()


class RawFilingStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.manager = SecCacheManager(cache_dir=self.tmpdir)

    def test_round_trip_and_dash_normalization(self):
        xml = "<ownershipDocument><issuer/></ownershipDocument>"
        self.assertTrue(self.manager.save_raw_filing("0000320193-26-000042", xml))
        # Dashed and dashless forms resolve to the same immutable file.
        self.assertEqual(self.manager.load_raw_filing("000032019326000042"), xml)

    def test_rejects_non_xml_content(self):
        # An SEC throttle/error page must never be cached as a filing.
        html = "<html><body>Request Rate Threshold Exceeded"
        self.assertFalse(self.manager.save_raw_filing("0000320193-26-000043", html))
        self.assertIsNone(self.manager.load_raw_filing("0000320193-26-000043"))

    def test_rejects_bogus_accession(self):
        self.assertIsNone(self.manager.raw_filing_path("../../etc/passwd"))


class InsiderSignalsPayloadFileTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.manager = SecCacheManager(cache_dir=self.tmpdir)

    def test_fixed_filename_and_legacy_snapshot_pruning(self):
        import asyncio
        import os

        async def run():
            forms_dir = os.path.join(self.tmpdir, "forms")
            # Legacy date-stamped snapshots from the old scheme (the disk leak).
            for day in ("20260708", "20260709"):
                with open(os.path.join(forms_dir, f"ACME_insider_signals_180d_40_filing_date_{day}.json"), "w") as f:
                    f.write("{}")
            await self.manager.save_data(
                "ACME", "insider_signals", {"fingerprint": "abc"},
                days_back=180, filing_limit=40, anchor_type="filing_date",
            )
            files = sorted(os.listdir(forms_dir))
            loaded = await self.manager.load_data(
                "ACME", "insider_signals", days_back=180, filing_limit=40, anchor_type="filing_date",
            )
            return files, loaded

        files, loaded = asyncio.get_event_loop().run_until_complete(run()) if False else asyncio.run(run())
        # One fixed file per key; the dated leftovers are gone.
        self.assertEqual(files, ["ACME_insider_signals_180d_40_filing_date.json"])
        self.assertEqual(loaded, {"fingerprint": "abc"})
