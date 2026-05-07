import unittest

from copetech_sec.thirteenf_processor import ThirteenFProcessor, normalize_cik


SAMPLE_INFORMATION_TABLE = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>1450000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>8123456</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority>
      <Sole>8123456</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>TESLA INC</nameOfIssuer>
    <titleOfClass>CALL</titleOfClass>
    <cusip>88160R101</cusip>
    <value>198700</value>
    <shrsOrPrnAmt>
      <sshPrnamt>250000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <putCall>Call</putCall>
    <investmentDiscretion>DFND</investmentDiscretion>
    <votingAuthority>
      <Sole>0</Sole>
      <Shared>250000</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
</informationTable>
"""


class ThirteenFProcessorTests(unittest.TestCase):
    def test_normalize_cik_accepts_digits_and_pads(self):
        self.assertEqual(normalize_cik("1614314"), "0001614314")

    def test_normalize_cik_rejects_invalid_values(self):
        with self.assertRaises(ValueError):
            normalize_cik("SIG")

    def test_parse_information_table_xml_extracts_holdings(self):
        holdings = ThirteenFProcessor.parse_information_table_xml(SAMPLE_INFORMATION_TABLE)

        self.assertEqual(len(holdings), 2)
        self.assertEqual(holdings[0]["issuer"], "APPLE INC")
        self.assertEqual(holdings[0]["cusip"], "037833100")
        self.assertEqual(holdings[0]["value_thousands"], 1450000)
        self.assertEqual(holdings[0]["value"], 1450000000)
        self.assertEqual(holdings[0]["shares"], 8123456)
        self.assertEqual(holdings[0]["share_type"], "SH")
        self.assertEqual(holdings[0]["voting_authority"]["sole"], 8123456)
        self.assertEqual(holdings[1]["put_call"], "Call")

    def test_choose_information_table_document_prefers_info_xml(self):
        document = ThirteenFProcessor.choose_information_table_document(
            [
                {"name": "primary_doc.xml", "type": "13F-HR"},
                {"name": "form13fInfoTable.xml", "type": "INFORMATION TABLE"},
                {"name": "xslForm13F_X01.xml", "type": "XML"},
            ]
        )

        self.assertEqual(document, "form13fInfoTable.xml")


if __name__ == "__main__":
    unittest.main()
