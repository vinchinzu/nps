from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from i990.parse.xml_header import _oid_from_name, extract
from i990.sources.irs_xml import normalize_batch_id, _parse_sub_year


class ParsingHelpersTest(unittest.TestCase):
    def test_normalize_batch_id_uppercases_teos_suffix(self) -> None:
        self.assertEqual(normalize_batch_id("2024_TEOS_XML_04a"), "2024_TEOS_XML_04A")
        self.assertEqual(normalize_batch_id("download990xml_2019_1"), "download990xml_2019_1")

    def test_parse_sub_year_handles_year_and_timestamp(self) -> None:
        self.assertEqual(_parse_sub_year("2024"), 2024)
        self.assertEqual(_parse_sub_year("1/3/2017 8:55:06 AM"), 2017)
        self.assertIsNone(_parse_sub_year(""))

    def test_oid_from_name_strips_public_suffix(self) -> None:
        self.assertEqual(_oid_from_name("202410229349201231_public.xml"), "202410229349201231")
        self.assertEqual(_oid_from_name("abc.xml"), "abc")

    def test_extract_reads_common_header_fields(self) -> None:
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<Return xmlns="http://www.irs.gov/efile">
  <ReturnHeader>
    <ReturnTypeCd>990</ReturnTypeCd>
    <TaxYr>2024</TaxYr>
    <TaxPeriodBeginDt>2024-01-01</TaxPeriodBeginDt>
    <TaxPeriodEndDt>2024-12-31</TaxPeriodEndDt>
    <Filer>
      <EIN>123456789</EIN>
      <BusinessName>
        <BusinessNameLine1Txt>Example Org</BusinessNameLine1Txt>
      </BusinessName>
      <USAddress>
        <AddressLine1Txt>123 Main St</AddressLine1Txt>
        <CityNm>Austin</CityNm>
        <StateAbbreviationCd>TX</StateAbbreviationCd>
        <ZIPCd>78701</ZIPCd>
      </USAddress>
    </Filer>
  </ReturnHeader>
  <ReturnData>
    <IRS990>
      <MissionDesc>Testing things</MissionDesc>
      <WebsiteAddressTxt>https://example.org</WebsiteAddressTxt>
      <CYTotalRevenueAmt>123456</CYTotalRevenueAmt>
      <CYTotalExpensesAmt>100000</CYTotalExpensesAmt>
      <TotalAssetsEOYAmt>500000</TotalAssetsEOYAmt>
      <TotalLiabilitiesEOYAmt>20000</TotalLiabilitiesEOYAmt>
      <NetAssetsOrFundBalancesEOYAmt>480000</NetAssetsOrFundBalancesEOYAmt>
      <Form990PartVIISectionAGrp>
        <PersonNm>Jane Doe</PersonNm>
        <TitleTxt>CEO</TitleTxt>
        <ReportableCompFromOrgAmt>60000</ReportableCompFromOrgAmt>
        <OtherCompensationAmt>5000</OtherCompensationAmt>
        <AverageHoursPerWeekRt>40</AverageHoursPerWeekRt>
      </Form990PartVIISectionAGrp>
    </IRS990>
  </ReturnData>
</Return>
"""
        data = extract(xml)
        self.assertIsNotNone(data)
        assert data is not None
        self.assertEqual(data["ein"], "123456789")
        self.assertEqual(data["return_type"], "990")
        self.assertEqual(data["tax_year"], 2024)
        self.assertEqual(data["org_name"], "Example Org")
        self.assertEqual(data["city"], "Austin")
        self.assertEqual(data["total_revenue"], 123456)
        self.assertEqual(len(data["officers"]), 1)


if __name__ == "__main__":
    unittest.main()
