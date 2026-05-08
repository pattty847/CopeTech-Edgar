#!/usr/bin/env python3
import unittest

from copetech_sec.supply_chain_parser import SupplyChainParser


def _build_sample_html(business_body: str, risk_body: str) -> str:
    return (
        "<html><head><style>.hdr { color: red; }</style></head><body>"
        "<p>Item&nbsp;1.&nbsp;Business</p>"
        f"<p>{business_body}</p>"
        "<p>Item&nbsp;1A.&nbsp;Risk Factors</p>"
        f"<p>{risk_body}</p>"
        "<p>Item 2. Properties</p>"
        "<p>Some properties text.</p>"
        "</body></html>"
    )

BUSINESS_BODY = (
    "We are a global technology company. "
    "Walmart accounted for 18% of our net sales in fiscal 2025. "
    "We rely heavily on Foxconn for assembly of our flagship devices. "
    "Key suppliers include TSMC, Samsung and Pegatron. "
    "Our primary competitors include Google, Microsoft, and Meta. "
    + ("Filler text. " * 200)
)

RISK_BODY = (
    "We depend substantially on a limited number of contract manufacturers. "
    + ("Risk filler text. " * 200)
)


class SupplyChainParserTests(unittest.TestCase):
    def setUp(self):
        self.parser = SupplyChainParser()
        self.html = _build_sample_html(BUSINESS_BODY, RISK_BODY)

    def test_clean_html_strips_tags_and_entities(self):
        cleaned = self.parser.clean_html("<p>Hello&nbsp;<b>world</b></p>")
        self.assertNotIn("<", cleaned)
        self.assertNotIn(">", cleaned)
        self.assertIn("Hello", cleaned)
        self.assertIn("world", cleaned)

    def test_clean_html_drops_script_and_style(self):
        cleaned = self.parser.clean_html(
            "<style>.x{color:red}</style><script>alert(1)</script><p>visible</p>"
        )
        self.assertNotIn("alert", cleaned)
        self.assertNotIn("color:red", cleaned)
        self.assertIn("visible", cleaned)

    def test_extract_sections_finds_business_and_risk(self):
        cleaned = self.parser.clean_html(self.html)
        sections = self.parser.extract_sections(cleaned)
        self.assertIn("Walmart", sections["business"])
        self.assertIn("contract manufacturers", sections["risk_factors"])
        self.assertNotIn("Risk filler", sections["business"])

    def test_extract_relationships_classifies_customer_supplier_competitor(self):
        cleaned = self.parser.clean_html(self.html)
        sections = self.parser.extract_sections(cleaned)
        rels = self.parser.extract_relationships(sections)

        by_type: dict[str, list[dict]] = {}
        for rel in rels:
            by_type.setdefault(rel["relationship_type"], []).append(rel)

        customer_targets = {r["target_entity"] for r in by_type.get("customer", [])}
        self.assertTrue(any("Walmart" in t for t in customer_targets))
        walmart = next(r for r in by_type["customer"] if "Walmart" in r["target_entity"])
        self.assertAlmostEqual(walmart["weight"], 0.18, places=2)

        supplier_targets = {r["target_entity"] for r in by_type.get("supplier", [])}
        self.assertTrue(supplier_targets, "expected at least one supplier extraction")

        competitor_targets = {r["target_entity"] for r in by_type.get("competitor", [])}
        self.assertIn("Google", competitor_targets)
        self.assertIn("Microsoft", competitor_targets)

    def test_extract_relationships_skips_short_sections(self):
        rels = self.parser.extract_relationships({"business": "tiny", "risk_factors": ""})
        self.assertEqual(rels, [])


if __name__ == "__main__":
    unittest.main()
