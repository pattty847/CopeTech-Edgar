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
