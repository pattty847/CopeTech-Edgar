from pathlib import Path
import unittest

from copetech_sec.form4_processor import Form4Processor
from copetech_sec.ownership import OwnershipXmlParser


FIXTURES = Path(__file__).parent / "fixtures" / "sec"


class RecordedOwnershipFixtureTests(unittest.TestCase):
    def test_apple_rsu_settlement_contract(self):
        xml = (
            FIXTURES
            / "ownership"
            / "apple-2026-rsu-settlement.xml"
        ).read_text()

        rows = Form4Processor(None, None).parse_form4_xml(xml)

        self.assertEqual(len(rows), 3)
        self.assertEqual(
            [(row["transaction_code"], row["acq_disp_code"]) for row in rows],
            [("M", "A"), ("F", "D"), ("M", "D")],
        )
        tax_row = next(row for row in rows if row["transaction_code"] == "F")
        self.assertIn("No shares were sold", tax_row["footnotes"][0])

    def test_compatibility_facade_uses_the_namespaced_parser(self):
        xml = (
            FIXTURES
            / "ownership"
            / "apple-2026-rsu-settlement.xml"
        ).read_text()

        direct = OwnershipXmlParser().parse(xml)
        facade = Form4Processor(None, None).parse_form4_xml(xml)

        self.assertEqual(facade, direct)
