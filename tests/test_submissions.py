from __future__ import annotations

from datetime import date, timedelta
import tempfile
import unittest

from copetech_sec.cache_manager import SecCacheManager
from copetech_sec.submissions import SubmissionsResource


def filing_block(accessions, dates, forms):
    return {
        "accessionNumber": accessions,
        "filingDate": dates,
        "reportDate": dates,
        "form": forms,
        "primaryDocument": ["primary.htm"] * len(accessions),
        "primaryDocDescription": ["Primary"] * len(accessions),
        "items": [""] * len(accessions),
    }


class FakeHttp:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    async def make_request(self, url, is_json=True):
        self.calls.append(url)
        return self.payloads[url.rsplit("/", 1)[-1]]


class SubmissionsResourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_traverses_historical_files_and_returns_pagination_metadata(self):
        today = date.today()
        recent_date = today.isoformat()
        historical_date = (today - timedelta(days=500)).isoformat()
        history_name = "CIK0000320193-submissions-001.json"
        primary = {
            "filings": {
                "recent": filing_block(
                    ["0000320193-26-000001"],
                    [recent_date],
                    ["10-K"],
                ),
                "files": [{
                    "name": history_name,
                    "filingFrom": historical_date,
                    "filingTo": historical_date,
                    "filingCount": 1,
                }],
            }
        }
        history = filing_block(
            ["0000320193-25-000001"],
            [historical_date],
            ["10-K"],
        )
        http = FakeHttp({history_name: history})

        with tempfile.TemporaryDirectory() as tmpdir:
            resource = SubmissionsResource(http, SecCacheManager(tmpdir))
            first = await resource.query_filings(
                primary,
                cik="0000320193",
                forms=["10-K"],
                days_back=600,
                use_cache=True,
                limit=1,
            )
            second = await resource.query_filings(
                primary,
                cik="0000320193",
                forms=["10-K"],
                days_back=600,
                use_cache=True,
                limit=1,
                cursor=first.next_cursor,
            )

        self.assertEqual(first.items[0]["accession_no"], "0000320193-26-000001")
        self.assertTrue(first.truncated)
        self.assertEqual(first.next_cursor, "1")
        self.assertEqual(second.items[0]["accession_no"], "0000320193-25-000001")
        self.assertFalse(second.truncated)
        self.assertEqual(
            first.source_files,
            ("CIK0000320193.json", history_name),
        )
        self.assertEqual(len(http.calls), 1, "second page should reuse cached history")

    async def test_skips_history_files_older_than_requested_window(self):
        today = date.today()
        old_date = (today - timedelta(days=1000)).isoformat()
        history_name = "CIK0000320193-submissions-001.json"
        primary = {
            "filings": {
                "recent": filing_block([], [], []),
                "files": [{
                    "name": history_name,
                    "filingFrom": old_date,
                    "filingTo": old_date,
                    "filingCount": 1,
                }],
            }
        }
        http = FakeHttp({})

        with tempfile.TemporaryDirectory() as tmpdir:
            resource = SubmissionsResource(http, SecCacheManager(tmpdir))
            result = await resource.query_filings(
                primary,
                cik="0000320193",
                forms=["10-K"],
                days_back=90,
                use_cache=True,
            )

        self.assertEqual(result.items, [])
        self.assertEqual(http.calls, [])
        self.assertFalse(result.truncated)

    async def test_invalid_history_name_is_reported_without_fetching(self):
        primary = {
            "filings": {
                "recent": filing_block([], [], []),
                "files": [{"name": "../secret.json", "filingTo": date.today().isoformat()}],
            }
        }
        http = FakeHttp({})

        with tempfile.TemporaryDirectory() as tmpdir:
            resource = SubmissionsResource(http, SecCacheManager(tmpdir))
            result = await resource.query_filings(
                primary,
                cik="0000320193",
                forms=["10-K"],
                days_back=90,
                use_cache=True,
            )

        self.assertIn("invalid_historical_file_name:../secret.json", result.warnings)
        self.assertEqual(http.calls, [])
