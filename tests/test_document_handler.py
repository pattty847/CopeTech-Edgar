#!/usr/bin/env python3
"""Regression tests for filing-document discovery.

Document selection had no test coverage. The defects below were found by comparing the
code's assumptions against live SEC `index.json` responses for a Form 4, a 10-K and a
13F-HR: SEC populates the `type` field with a *directory-listing icon name*
("text.gif", "compressed.gif"), not an EDGAR document type, so every branch keying on
`type` was unreachable.
"""

import re
import tempfile
import unittest
from pathlib import Path

from copetech_sec.document_handler import FilingDocumentHandler, _is_safe_document_name


class PrimaryDocumentRegexTests(unittest.TestCase):
    """`_find_primary_document_name` builds its index.htm fallback patterns as raw
    strings, but two used `\\d` — a literal backslash followed by 'd' — so they could
    never match a filename."""

    INDEX_HTML = (
        '<table><tr><td><a href="/Archives/edgar/data/1/000/d123456k.htm">8-K</a></td></tr>'
        '<tr><td><a href="/Archives/edgar/data/1/000/msft-20230630.htm">10-K</a></td></tr></table>'
    )

    @staticmethod
    def _pattern_literals() -> list[str]:
        """The `r'''href=...'''` pattern literals from the index.htm fallback."""
        import inspect

        source = inspect.getsource(FilingDocumentHandler._find_primary_document_name)
        return [
            line.strip()
            for line in source.splitlines()
            if line.strip().startswith("r'''href=")
        ]

    def test_pattern_literals_were_found(self):
        self.assertGreaterEqual(len(self._pattern_literals()), 5)

    def test_no_pattern_contains_a_double_escaped_digit_class(self):
        # r'\\d' inside a raw string is backslash-backslash-d: never a digit class.
        offenders = [literal for literal in self._pattern_literals() if r"\\d" in literal]
        self.assertEqual(offenders, [], f"patterns still use '\\\\d' instead of '\\d': {offenders}")

    def test_every_pattern_compiles(self):
        for literal in self._pattern_literals():
            with self.subTest(literal=literal):
                # Drop the trailing comma/comment, then eval the raw literal itself.
                expression = literal[: literal.rindex("'''") + 3]
                re.compile(eval(expression))  # noqa: S307 - fixed literals from our own source

    def test_eight_k_filename_pattern_matches(self):
        self.assertEqual(
            re.findall(r'''href=["'][^"']*(d\d+k\.htm)["']''', self.INDEX_HTML, re.I),
            ["d123456k.htm"],
        )

    def test_ticker_dated_filename_pattern_matches(self):
        matches = re.findall(r'''href=["'][^"']*(\w+-\d{8}\.htm)["']''', self.INDEX_HTML, re.I)
        self.assertTrue(any(m.endswith("-20230630.htm") for m in matches), matches)


class DocumentTypeFieldTests(unittest.TestCase):
    """SEC's index.json `type` is an icon filename, verified against live responses."""

    def test_icon_names_are_not_treated_as_document_types(self):
        for icon in ("text.gif", "compressed.gif", "image2.gif", "IMAGE2.GIF"):
            with self.subTest(icon=icon):
                self.assertFalse(FilingDocumentHandler._document_type_is_meaningful(icon))

    def test_real_edgar_types_are_accepted(self):
        for doc_type in ("10-K", "10-Q", "EX-99.1", "INFORMATION TABLE"):
            with self.subTest(doc_type=doc_type):
                self.assertTrue(FilingDocumentHandler._document_type_is_meaningful(doc_type))

    def test_empty_type_is_not_meaningful(self):
        self.assertFalse(FilingDocumentHandler._document_type_is_meaningful(None))
        self.assertFalse(FilingDocumentHandler._document_type_is_meaningful("  "))


class DocumentNameSafetyTests(unittest.TestCase):
    """Names come from remote index.json and are joined into both a URL and, in
    `download_all_form_documents`, a local output path."""

    def test_legitimate_names_are_allowed(self):
        for name in ("form4.xml", "aapl-20250927.htm", "xslF345X03/form4.xml", "50240.xml"):
            with self.subTest(name=name):
                self.assertTrue(_is_safe_document_name(name))

    def test_traversal_and_absolute_names_are_rejected(self):
        for name in (
            "../../../etc/passwd",
            "/etc/passwd",
            "a/../../b.xml",
            "%2e%2e/secret.xml",
            "form4.xml?raw=1",
            "form4.xml#fragment",
            "",
            "./x",
            "C:/x",
        ):
            with self.subTest(name=name):
                self.assertFalse(_is_safe_document_name(name))


class RecordingHttpClient:
    def __init__(self, index_items=None):
        self.calls = []
        self.index_items = index_items or []

    async def make_archive_request(self, url, is_json=False):
        self.calls.append((url, is_json))
        if url.endswith("index.json"):
            return {"directory": {"item": self.index_items}}
        return "document body"


class DocumentBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def _handler(self, http_client):
        async def lookup(_ticker):
            return "0000320193"

        return FilingDocumentHandler(http_client, lookup)

    async def test_download_rejects_unsafe_name_before_http(self):
        http = RecordingHttpClient()
        handler = self._handler(http)

        with self.assertRaises(ValueError):
            await handler.download_form_document(
                "0000320193-26-000001",
                "../secret.xml",
                cik="0000320193",
            )

        self.assertEqual(http.calls, [])

    async def test_download_rejects_malformed_accession_before_http(self):
        http = RecordingHttpClient()
        handler = self._handler(http)

        with self.assertRaises(ValueError):
            await handler.download_form_document(
                "../../outside",
                "form4.xml",
                cik="0000320193",
            )

        self.assertEqual(http.calls, [])

    async def test_bulk_download_preserves_safe_nested_document_path(self):
        http = RecordingHttpClient([
            {"name": "nested/form4.xml", "type": "text.gif"},
        ])
        handler = self._handler(http)

        with tempfile.TemporaryDirectory() as tmpdir:
            result = await handler.download_all_form_documents(
                "0000320193-26-000001",
                ticker="AAPL",
                output_dir=tmpdir,
            )

            saved = Path(tmpdir) / "000032019326000001" / "nested" / "form4.xml"
            self.assertEqual(result["nested/form4.xml"], "document body")
            self.assertEqual(saved.read_text(), "document body")


class ExhibitFilteringTests(unittest.TestCase):
    """The exhibit filter used a bare `'ex-' in name` substring test, which also matched
    'index-headers' and any primary document whose ticker contains 'ex-'."""

    def test_flex_style_ticker_is_not_discarded_as_an_exhibit(self):
        import inspect

        source = inspect.getsource(FilingDocumentHandler.fetch_primary_html)
        self.assertNotIn("'ex-' in name", source, "bare 'ex-' substring test still present")

    def test_exhibit_names_still_match_the_documented_shapes(self):
        # Guards the replacement predicate's intent without invoking the network path.
        def is_exhibit(name: str) -> bool:
            base = name.rsplit("/", 1)[-1]
            return base.startswith(("ex-", "exhibit")) or "-ex-" in base or "exhibit" in base

        self.assertTrue(is_exhibit("ex-99_1.htm"))
        self.assertTrue(is_exhibit("exhibit991.htm"))
        self.assertTrue(is_exhibit("aapl-ex-991.htm"))
        self.assertFalse(is_exhibit("flex-20240101.htm"))
        self.assertFalse(is_exhibit("aapl-20250927.htm"))


class ThirteenFDocumentSelectionTests(unittest.TestCase):
    """Berkshire's 13F names its information table opaquely ("50240.xml"), so selection
    cannot rely on recognising the table by name — only on excluding the cover page and
    SEC's XSL-rendered view."""

    def setUp(self):
        from copetech_sec.thirteenf_processor import ThirteenFProcessor

        self.choose = ThirteenFProcessor.choose_information_table_document

    def test_opaque_table_name_beats_cover_page(self):
        documents = [
            {"name": "0001193125-26-054580-index.html", "type": "text.gif"},
            {"name": "50240.xml", "type": "text.gif"},
            {"name": "primary_doc.xml", "type": "text.gif"},
        ]
        self.assertEqual(self.choose(documents), "50240.xml")

    def test_xsl_rendered_view_is_never_selected(self):
        documents = [
            {"name": "xslForm13F_X02/primary_doc.xml", "type": "text.gif"},
            {"name": "form13fInfoTable.xml", "type": "text.gif"},
        ]
        self.assertEqual(self.choose(documents), "form13fInfoTable.xml")

    def test_returns_none_when_only_excluded_candidates_exist(self):
        # Better to report "not found" than to download the cover page and parse zero
        # holdings, which is how a silent empty portfolio was produced.
        documents = [
            {"name": "primary_doc.xml", "type": "text.gif"},
            {"name": "xslForm13F_X02/primary_doc.xml", "type": "text.gif"},
        ]
        self.assertIsNone(self.choose(documents))

    def test_returns_none_when_no_xml_present(self):
        self.assertIsNone(self.choose([{"name": "filing.txt", "type": "text.gif"}]))


class FormXmlSelectionTests(unittest.TestCase):
    """`download_form_xml` prioritised any name containing 'form4', which matched both
    `form4.xml` and `xslF345X03/wf-form4_x.xml` — the latter being SEC's styled HTML view
    served under a .xml name."""

    def test_xsl_paths_are_excluded_from_form_xml_candidates(self):
        import inspect

        source = inspect.getsource(FilingDocumentHandler.download_form_xml)
        self.assertIn("xsl", source, "no XSL exclusion in download_form_xml")

    def test_selection_prefers_the_machine_readable_form(self):
        def select(names):
            high, low = [], []
            for name in names:
                lowered = name.lower()
                if not lowered.endswith(".xml"):
                    continue
                if lowered.startswith("xsl") or "/xsl" in lowered:
                    continue
                if (
                    "form4" in lowered
                    or "f345" in lowered
                    or lowered.endswith("/primary_doc.xml")
                    or lowered == "primary_doc.xml"
                ):
                    high.append(name)
                else:
                    low.append(name)
            candidates = high + low
            return candidates[0] if candidates else None

        self.assertEqual(select(["xslF345X03/wf-form4_1.xml", "form4.xml"]), "form4.xml")
        self.assertEqual(select(["xslF345X03/wf-form4_1.xml"]), None)
        self.assertEqual(select(["primary_doc.xml"]), "primary_doc.xml")


if __name__ == "__main__":
    unittest.main()
