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


if __name__ == "__main__":
    unittest.main()
