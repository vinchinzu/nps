"""Stream-parse 990 XML files from downloaded ZIPs.

The IRS 990 XML schema is namespaced (urn:irs.gov:efile). There are several
top-level return types (Form 990, 990-EZ, 990-PF, 990-T) and the schema
evolves yearly. This parser extracts a stable subset of "header" fields
that are present across variants:

  - ReturnHeader: EIN, tax year/period, filer name, address, return type
  - ReturnData: mission, website, total revenue/expenses/assets/liabilities,
    and the top-compensated officers list.

We use iterparse and ignore unrecognized elements rather than binding
to a specific schema version.
"""
from __future__ import annotations

import json
import logging
import re
import shutil
import sqlite3
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable, Iterator
from xml.etree import ElementTree as ET

from ..config import XML_DIR
from ..db import record_run_end, record_run_start, session

log = logging.getLogger(__name__)

# The IRS uses this default namespace on 990 XML. We strip namespaces
# before matching tags to keep XPath-ish code readable.
NS_STRIP_PREFIX = "{http://www.irs.gov/efile}"


def _localname(tag: str) -> str:
    if tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag


def _text(el: ET.Element | None) -> str | None:
    if el is None:
        return None
    t = (el.text or "").strip()
    return t or None


def _find(root: ET.Element, *paths: str) -> ET.Element | None:
    """Find first match among several candidate tag paths. Namespace-agnostic."""
    for path in paths:
        parts = path.split("/")
        cur: ET.Element | None = root
        ok = True
        for part in parts:
            if cur is None:
                ok = False
                break
            nxt = None
            for child in cur:
                if _localname(child.tag) == part:
                    nxt = child
                    break
            if nxt is None:
                ok = False
                break
            cur = nxt
        if ok and cur is not None:
            return cur
    return None


def _int(el: ET.Element | None) -> int | None:
    v = _text(el)
    if not v:
        return None
    try:
        return int(v.replace(",", "").split(".")[0])
    except ValueError:
        return None


def _norm_name(name: str | None) -> str | None:
    """Normalise a person name for cross-EIN matching."""
    if not name:
        return None
    n = name.upper().strip()
    n = re.sub(r"[.,]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    n = re.sub(r"\s+(JR|SR|II|III|IV|V|VI)$", "", n)
    n = re.sub(r"\s+", " ", n)
    return n.strip() or None


def _ind_int(val: str | None) -> int | None:
    if val is None:
        return None
    return 1 if val.lower() in ("x", "true", "1", "yes") else 0


def _hours_float(val: str | None) -> float | None:
    if not val:
        return None
    try:
        return float(val.replace(",", ""))
    except ValueError:
        return None


def _extract_persons(root: ET.Element, ein: str, tax_year: int | None) -> list[dict]:
    """Extract all person records from a filing for the filing_persons table.

    Covers:
      - Form990 Part VII Section A: officers, directors, trustees, key employees
      - Form990 Part VII Section B: top independent contractors
      - Form990PF: OfficerDirTrstKeyEmplInfoGrp
      - ReturnHeader: signing officer and preparer
    """
    persons: list[dict] = []

    def _row(role: str, **kw) -> dict:
        return {"ein": ein, "tax_year": tax_year, "person_role": role, **kw}

    # --- Part VII Section A (990/990EZ officers, directors, key employees) ---
    for el in root.iter():
        tag = _localname(el.tag)
        if tag != "Form990PartVIISectionAGrp":
            continue
        name = _text(_find(el, "PersonNm")) or _text(_find(el, "BusinessName/BusinessNameLine1Txt"))
        title = _text(_find(el, "TitleTxt"))
        comp = _int(_find(el, "ReportableCompFromOrgAmt"))
        other = _int(_find(el, "OtherCompensationAmt"))
        related_comp = _int(_find(el, "ReportableCompFromRltdOrgAmt"))
        is_key = _ind_int(_text(_find(el, "KeyEmployeeInd")))
        is_hce = _ind_int(_text(_find(el, "HighlyCompensatedEmployeeInd")))
        is_former = _ind_int(_text(_find(el, "FormerOfcrDirectorTrusteeInd")))
        persons.append(_row(
            "officer_director",
            name=name,
            name_norm=_norm_name(name),
            title=title,
            reportable_comp=comp,
            other_comp=other,
            related_org_comp=related_comp,
            hours_per_week=_hours_float(_text(_find(el, "AverageHoursPerWeekRt"))),
            hours_related=_hours_float(_text(_find(el, "AverageHoursPerWeekRltdOrgRt"))),
            is_officer=_ind_int(_text(_find(el, "OfficerInd"))),
            is_director=_ind_int(_text(_find(el, "IndividualTrusteeOrDirectorInd"))),
            is_key_employee=is_key,
            is_hce=is_hce,
            is_former=is_former,
        ))

    # --- Part VII Section B (independent contractors) ---
    for el in root.iter():
        if _localname(el.tag) != "Form990PartVIISectionBGrp":
            continue
        name = (_text(_find(el, "PersonNm"))
                or _text(_find(el, "BusinessName/BusinessNameLine1Txt")))
        comp = _int(_find(el, "CompensationAmt"))
        services = _text(_find(el, "ServicesDesc"))
        persons.append(_row(
            "contractor",
            name=name,
            name_norm=_norm_name(name),
            title=None,
            reportable_comp=comp,
            services_desc=services,
        ))

    # --- 990PF officer/director/trustee/key-employee group ---
    for el in root.iter():
        if _localname(el.tag) not in (
            "OfficerDirTrstKeyEmplInfoGrp",
            "OfficerDirectorTrusteeEmplGrp",
        ):
            continue
        name = (_text(_find(el, "PersonNm"))
                or _text(_find(el, "BusinessName/BusinessNameLine1Txt")))
        title = _text(_find(el, "TitleTxt"))
        comp = _int(_find(el, "CompensationAmt"))
        persons.append(_row(
            "officer_director",
            name=name,
            name_norm=_norm_name(name),
            title=title,
            reportable_comp=comp,
        ))

    # --- Related-org officer table (Schedule R / Part VII) ---
    for el in root.iter():
        if _localname(el.tag) != "RltdOrgOfficerTrstKeyEmplGrp":
            continue
        name = _text(_find(el, "PersonNm"))
        title = _text(_find(el, "TitleTxt"))
        comp = _int(_find(el, "ReportableCompFromOrgAmt"))
        persons.append(_row(
            "related_org_officer",
            name=name,
            name_norm=_norm_name(name),
            title=title,
            reportable_comp=comp,
            hours_per_week=_hours_float(_text(_find(el, "AverageHrsPerWkDevotedToPosRt"))),
        ))

    # --- ReturnHeader: signing officer ---
    hdr = _find(root, "ReturnHeader")
    if hdr is not None:
        bog = _find(hdr, "BusinessOfficerGrp")
        if bog is not None:
            name = _text(_find(bog, "PersonNm"))
            title = _text(_find(bog, "PersonTitleTxt"))
            persons.append(_row(
                "signing_officer",
                name=name,
                name_norm=_norm_name(name),
                title=title,
            ))
        prep = _find(hdr, "PreparerPersonGrp")
        if prep is not None:
            name = _text(_find(prep, "PreparerPersonNm"))
            persons.append(_row(
                "preparer",
                name=name,
                name_norm=_norm_name(name),
                title="Preparer",
            ))

    return [p for p in persons if p.get("name_norm")]


def _officers(root: ET.Element, limit: int = 10) -> list[dict]:
    """Extract Form 990 Part VII Section A officer rows for officers_json."""
    out: list[dict] = []
    for el in root.iter():
        if _localname(el.tag) != "Form990PartVIISectionAGrp":
            continue
        name = _text(_find(el, "PersonNm")) or _text(_find(el, "BusinessName/BusinessNameLine1Txt"))
        title = _text(_find(el, "TitleTxt"))
        comp = _int(_find(el, "ReportableCompFromOrgAmt"))
        other = _int(_find(el, "OtherCompensationAmt"))
        related_comp = _int(_find(el, "ReportableCompFromRltdOrgAmt"))
        hours = _text(_find(el, "AverageHoursPerWeekRt"))
        hours_related = _text(_find(el, "AverageHoursPerWeekRltdOrgRt"))
        is_officer = _text(_find(el, "OfficerInd"))
        is_director = _text(_find(el, "IndividualTrusteeOrDirectorInd"))
        out.append({
            "name": name,
            "title": title,
            "reportable_comp": comp,
            "other_comp": other,
            "related_org_comp": related_comp,
            "hours_per_week": hours,
            "hours_per_week_related": hours_related,
            "is_officer": is_officer,
            "is_director": is_director,
        })
        if len(out) >= limit:
            break
    return out


# Boolean indicator fields from Parts IV, V, VI of Form 990.
# These are the self-reported yes/no checklist items.
_FLAG_TAGS = (
    # Part IV: Required Schedules
    "PoliticalCampaignActyInd",
    "LobbyingActivitiesInd",
    "SubjectToProxyTaxInd",
    "DonorAdvisedFundInd",
    "ConservationEasementsInd",
    "CollectionsOfArtInd",
    "CreditCounselingInd",
    "DonorRstrOrQuasiEndowmentsInd",
    "ReportLandBuildingEquipmentInd",
    "ReportInvestmentsOtherSecInd",
    "ReportProgramRelatedInvstInd",
    "ReportOtherAssetsInd",
    "ReportOtherLiabilitiesInd",
    "IncludeFIN48FootnoteInd",
    "IndependentAuditFinclStmtInd",
    "ConsolidatedAuditFinclStmtInd",
    "SchoolOperatingInd",
    "ForeignOfficeInd",
    "ForeignActivitiesInd",
    "MoreThan5000KToOrgInd",
    "MoreThan5000KToIndividualsInd",
    "ProfessionalFundraisingInd",
    "FundraisingActivitiesInd",
    "GamingActivitiesInd",
    "OperateHospitalInd",
    "GrantsToOrganizationsInd",
    "GrantsToIndividualsInd",
    "ScheduleJRequiredInd",
    "TaxExemptBondsInd",
    "EngagedInExcessBenefitTransInd",
    "PYExcessBenefitTransInd",
    "LoanOutstandingInd",
    "GrantToRelatedPersonInd",
    "BusinessRlnWithOrgMemInd",
    "BusinessRlnWithFamMemInd",
    "BusinessRlnWith35CtrlEntInd",
    "DeductibleNonCashContriInd",
    "DeductibleArtContributionInd",
    "TerminateOperationsInd",
    "PartialLiquidationInd",
    "DisregardedEntityInd",
    "RelatedEntityInd",
    "RelatedOrganizationCtrlEntInd",
    "TransactionWithControlEntInd",
    "TrnsfrExmptNonChrtblRltdOrgInd",
    "ActivitiesConductedPrtshpInd",
    # Part V: Other IRS Filings
    "BackupWthldComplianceInd",
    "EmploymentTaxReturnsFiledInd",
    "UnrelatedBusIncmOverLimitInd",
    "ForeignFinancialAccountInd",
    "ProhibitedTaxShelterTransInd",
    "TaxablePartyNotificationInd",
    "NondeductibleContributionsInd",
    "QuidProQuoContributionsInd",
    "Form8282PropertyDisposedOfInd",
    "RcvFndsToPayPrsnlBnftCntrctInd",
    "PayPremiumsPrsnlBnftCntrctInd",
    "Form8899Filedind",
    "Form1098CFiledInd",
    "DAFExcessBusinessHoldingsInd",
    "TaxableDistributionsInd",
    "DistributionToDonorInd",
    "IndoorTanningServicesInd",
    "SubjToTaxRmnrtnExPrchtPymtInd",
    "SubjectToExcsTaxNetInvstIncInd",
    # Part VI: Governance
    "FamilyOrBusinessRlnInd",
    "DelegationOfMgmtDutiesInd",
    "ChangeToOrgDocumentsInd",
    "MaterialDiversionOrMisuseInd",
    "MembersOrStockholdersInd",
    "ElectionOfBoardMembersInd",
    "DecisionsSubjectToApprovaInd",
    "MinutesOfGoverningBodyInd",
    "MinutesOfCommitteesInd",
    "OfficerMailingAddressInd",
    "LocalChaptersInd",
    "Form990ProvidedToGvrnBodyInd",
    "ConflictOfInterestPolicyInd",
    "AnnualDisclosureCoveredPrsnInd",
    "RegularMonitoringEnfrcInd",
    "WhistleblowerPolicyInd",
    "DocumentRetentionPolicyInd",
    "CompensationProcessCEOInd",
    "CompensationProcessOtherInd",
    "InvestmentInJointVentureInd",
    # Compensation / Part VII summary
    "FormerOfcrEmployeesListedInd",
    "TotalCompGreaterThan150KInd",
    "CompensationFromOtherSrcsInd",
    # Financial / accounting
    "MethodOfAccountingCashInd",
    "MethodOfAccountingAccrualInd",
    "AccountantCompileOrReviewInd",
    "FSAuditedInd",
    "FederalGrantAuditRequiredInd",
    # General
    "GroupReturnForAffiliatesInd",
    "DescribedInSection501c3Ind",
    "ScheduleBRequiredInd",
    "ScheduleORequiredInd",
    "InitialReturnInd",
    "FinalReturnInd",
    "AddressChangeInd",
    "AmendedReturnInd",
    "ApplicationPendingInd",
)


def _bool_val(el: ET.Element | None) -> bool | None:
    """Normalise IRS boolean indicators (true/false/X/x/1/0) to Python bool."""
    v = _text(el)
    if v is None:
        return None
    lv = v.lower()
    if lv in ("true", "x", "1", "yes"):
        return True
    if lv in ("false", "0", "no"):
        return False
    return None


def _collect_flags(irs_section: ET.Element) -> dict:
    """Walk an IRS990/EZ/PF element and return all known flag indicators."""
    flags: dict = {}
    tag_set = set(_FLAG_TAGS)
    for el in irs_section.iter():
        lname = _localname(el.tag)
        if lname in tag_set:
            val = _bool_val(el)
            if val is not None and lname not in flags:
                flags[lname] = val
    return flags


def _collect_raw_scalars(irs_section: ET.Element) -> dict:
    """Return all scalar (leaf-node) fields from an IRS990/EZ/PF element.

    Only captures elements with no children and non-empty text. Keys are
    the local tag names; duplicate tags keep the last value (rare in IRS XML).
    """
    raw: dict = {}
    for el in irs_section.iter():
        if len(el) == 0:  # leaf node
            v = (el.text or "").strip()
            if v:
                raw[_localname(el.tag)] = v
    return raw


def _find_irs_section(root: ET.Element) -> ET.Element | None:
    """Find the first IRS990, IRS990EZ, or IRS990PF element."""
    for name in ("IRS990", "IRS990EZ", "IRS990PF", "IRS990T"):
        el = _find(root, f"ReturnData/{name}")
        if el is not None:
            return el
    return None


def extract(xml_bytes: bytes) -> dict | None:
    """Parse one 990 XML into a dict of header fields, or None on failure."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        log.debug("parse error: %s", e)
        return None

    hdr = _find(root, "ReturnHeader")
    if hdr is None:
        return None

    filer = _find(hdr, "Filer")
    filer_ctx = filer if filer is not None else hdr
    address = _find(filer_ctx, "USAddress")
    if address is None:
        address = _find(filer_ctx, "ForeignAddress")

    irs = _find_irs_section(root)

    # Try common revenue/assets field names across 990 variants.
    total_rev = _int(_find(root,
        "ReturnData/IRS990/TotalRevenueGrp/TotalRevenueColumnAmt",
        "ReturnData/IRS990/CYTotalRevenueAmt",
        "ReturnData/IRS990EZ/TotalRevenueAmt",
        "ReturnData/IRS990PF/AnalysisOfRevenueAndExpenses/TotalRevAndExpnssAmt",
    ))
    total_exp = _int(_find(root,
        "ReturnData/IRS990/TotalFunctionalExpensesGrp/TotalAmt",
        "ReturnData/IRS990/CYTotalExpensesAmt",
        "ReturnData/IRS990EZ/TotalExpensesAmt",
        "ReturnData/IRS990PF/AnalysisOfRevenueAndExpenses/TotalExpensesRevAndExpnssAmt",
    ))
    assets_eoy = _int(_find(root,
        "ReturnData/IRS990/TotalAssetsEOYAmt",
        "ReturnData/IRS990EZ/TotalAssetsEOYAmt",
        "ReturnData/IRS990PF/FMVAssetsEOYAmt",
    ))
    liab_eoy = _int(_find(root,
        "ReturnData/IRS990/TotalLiabilitiesEOYAmt",
        "ReturnData/IRS990EZ/SumOfTotalLiabilitiesAmt",
    ))
    net_eoy = _int(_find(root,
        "ReturnData/IRS990/NetAssetsOrFundBalancesEOYAmt",
        "ReturnData/IRS990EZ/NetAssetsOrFundBalancesEOYAmt",
    ))
    mission = _text(_find(root,
        "ReturnData/IRS990/ActivityOrMissionDesc",
        "ReturnData/IRS990/MissionDesc",
        "ReturnData/IRS990EZ/PrimaryExemptPurposeTxt",
    ))
    website = _text(_find(root,
        "ReturnData/IRS990/WebsiteAddressTxt",
        "ReturnData/IRS990EZ/WebsiteAddressTxt",
    ))

    # Extended fields
    gross_receipts = _int(_find(root,
        "ReturnData/IRS990/GrossReceiptsAmt",
        "ReturnData/IRS990EZ/GrossReceiptsAmt",
    ))
    formation_yr = _int(_find(root,
        "ReturnData/IRS990/FormationYr",
        "ReturnData/IRS990EZ/FormationYr",
    ))
    legal_domicile = _text(_find(root,
        "ReturnData/IRS990/LegalDomicileStateCd",
        "ReturnData/IRS990EZ/LegalDomicileStateCd",
    ))
    principal_officer = _text(_find(root,
        "ReturnData/IRS990/PrincipalOfficerNm",
        "ReturnData/IRS990EZ/PrincipalOfcrNm",
    ))
    phone = _text(_find(filer_ctx, "PhoneNum"))
    voting_members = _int(_find(root,
        "ReturnData/IRS990/VotingMembersGoverningBodyCnt",
        "ReturnData/IRS990/GoverningBodyVotingMembersCnt",
    ))
    independent_members = _int(_find(root,
        "ReturnData/IRS990/VotingMembersIndependentCnt",
        "ReturnData/IRS990/IndependentVotingMemberCnt",
    ))
    total_employees = _int(_find(root,
        "ReturnData/IRS990/TotalEmployeeCnt",
        "ReturnData/IRS990EZ/EmployeeCnt",
    ))
    total_volunteers = _int(_find(root,
        "ReturnData/IRS990/TotalVolunteersCnt",
    ))
    total_gross_ubi = _int(_find(root,
        "ReturnData/IRS990/TotalGrossUBIAmt",
        "ReturnData/IRS990EZ/TotalGrossUBIAmt",
    ))
    py_total_revenue = _int(_find(root,
        "ReturnData/IRS990/PYTotalRevenueAmt",
        "ReturnData/IRS990EZ/PYTotalRevenueAmt",
    ))
    cy_contributions = _int(_find(root,
        "ReturnData/IRS990/CYContributionsGrantsAmt",
        "ReturnData/IRS990/TotalContributionsAmt",
        "ReturnData/IRS990EZ/TotalContributionsAmt",
    ))
    cy_program_svc_rev = _int(_find(root,
        "ReturnData/IRS990/CYProgramServiceRevenueAmt",
        "ReturnData/IRS990EZ/ProgramServiceRevenueAmt",
    ))
    cy_investment_income = _int(_find(root,
        "ReturnData/IRS990/CYInvestmentIncomeAmt",
        "ReturnData/IRS990EZ/InvestmentIncomeAmt",
    ))
    cy_salaries = _int(_find(root,
        "ReturnData/IRS990/CYSalariesCompEmpBnftPaidAmt",
        "ReturnData/IRS990EZ/SalariesOtherCompEmpBnftAmt",
    ))
    cy_grants_paid = _int(_find(root,
        "ReturnData/IRS990/CYGrantsAndSimilarPaidAmt",
        "ReturnData/IRS990EZ/GrantsAndSimilarAmountsPaidAmt",
    ))
    cy_fundraising_exp = _int(_find(root,
        "ReturnData/IRS990/CYTotalFundraisingExpenseAmt",
    ))
    assets_boy = _int(_find(root,
        "ReturnData/IRS990/TotalAssetsBOYAmt",
        "ReturnData/IRS990EZ/TotalAssetsBOYAmt",
    ))
    liab_boy = _int(_find(root,
        "ReturnData/IRS990/TotalLiabilitiesBOYAmt",
    ))
    net_boy = _int(_find(root,
        "ReturnData/IRS990/NetAssetsOrFundBalancesBOYAmt",
        "ReturnData/IRS990EZ/NetAssetsOrFundBalancesBOYAmt",
    ))
    total_reportable_comp = _int(_find(root,
        "ReturnData/IRS990/TotalReportableCompFromOrgAmt",
    ))
    indiv_gt_100k = _int(_find(root,
        "ReturnData/IRS990/IndivRcvdGreaterThan100KCnt",
    ))

    flags = _collect_flags(irs) if irs is not None else {}
    raw_data = _collect_raw_scalars(irs) if irs is not None else {}

    ein = _text(_find(hdr, "Filer/EIN")) or ""
    tax_year = _int(_find(hdr, "TaxYr"))

    return {
        "ein": ein,
        "return_type": _text(_find(hdr, "ReturnTypeCd")),
        "tax_year": tax_year,
        "tax_period_begin": _text(_find(hdr, "TaxPeriodBeginDt")),
        "tax_period_end": _text(_find(hdr, "TaxPeriodEndDt")),
        "org_name": _text(_find(filer_ctx, "BusinessName/BusinessNameLine1Txt")),
        "org_address": _text(_find(address, "AddressLine1Txt")) if address is not None else None,
        "city": _text(_find(address, "CityNm")) if address is not None else None,
        "state": _text(_find(address, "StateAbbreviationCd")) if address is not None else None,
        "zip": _text(_find(address, "ZIPCd")) if address is not None else None,
        "mission": mission,
        "website": website,
        "total_revenue": total_rev,
        "total_expenses": total_exp,
        "total_assets_eoy": assets_eoy,
        "total_liabilities_eoy": liab_eoy,
        "net_assets_eoy": net_eoy,
        "officers": _officers(root),
        "persons": _extract_persons(root, ein, tax_year),
        # extended
        "gross_receipts": gross_receipts,
        "formation_yr": formation_yr,
        "legal_domicile_state": legal_domicile,
        "principal_officer": principal_officer,
        "phone": phone,
        "voting_members_cnt": voting_members,
        "independent_members_cnt": independent_members,
        "total_employees": total_employees,
        "total_volunteers": total_volunteers,
        "total_gross_ubi": total_gross_ubi,
        "py_total_revenue": py_total_revenue,
        "cy_contributions": cy_contributions,
        "cy_program_service_revenue": cy_program_svc_rev,
        "cy_investment_income": cy_investment_income,
        "cy_salaries": cy_salaries,
        "cy_grants_paid": cy_grants_paid,
        "cy_fundraising_expense": cy_fundraising_exp,
        "total_assets_boy": assets_boy,
        "total_liabilities_boy": liab_boy,
        "net_assets_boy": net_boy,
        "total_reportable_comp": total_reportable_comp,
        "indiv_rcvd_greater_100k_cnt": indiv_gt_100k,
        "flags": flags,
        "raw_data": raw_data,
    }


def _oid_from_name(name: str) -> str:
    stem = Path(name).stem
    if stem.endswith("_public"):
        stem = stem[: -len("_public")]
    return stem


def iter_xml_in_zip(zip_path: Path) -> Iterator[tuple[str, bytes]]:
    """Yield (object_id, xml_bytes) for every XML member of a batch ZIP.

    Filenames inside are like `202410229349201231_public.xml`. We strip the
    suffix to recover the OBJECT_ID.

    At least one IRS batch (2020_TEOS_XML_CT1.zip) uses DEFLATE64 which
    Python's stdlib zipfile cannot decompress. In that case we fall back
    to the system `unzip` binary, extracting the whole archive to a
    tempdir once and iterating the files from disk.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            try:
                for name in names:
                    with zf.open(name) as f:
                        yield _oid_from_name(name), f.read()
                return
            except NotImplementedError:
                # Fall through to external-tool fallback below.
                log.warning(
                    "zipfile cannot decompress %s (unsupported method); "
                    "falling back to external archive tool",
                    zip_path.name,
                )
    except zipfile.BadZipFile:
        raise

    # Fallback: extract the whole archive with an external tool. We try
    # 7z first (handles DEFLATE64 and tolerates minor header damage in
    # 2020_TEOS_XML_CT1.zip); unzip second.
    tool = None
    for candidate in ("7z", "unzip"):
        if shutil.which(candidate):
            tool = candidate
            break
    if tool is None:
        raise RuntimeError(
            f"{zip_path}: zipfile cannot decompress and neither 7z nor unzip are installed"
        )
    with tempfile.TemporaryDirectory(prefix="i990-unzip-") as td:
        if tool == "7z":
            cmd = ["7z", "x", "-y", f"-o{td}", str(zip_path)]
        else:
            cmd = ["unzip", "-q", "-o", str(zip_path), "-d", td]
        res = subprocess.run(cmd, check=False, capture_output=True)
        # 7z: 0 ok, 1 warnings, 2 fatal. unzip: 0 ok, 1 warnings.
        if res.returncode not in (0, 1):
            raise RuntimeError(
                f"{tool} failed on {zip_path}: "
                f"{res.stderr.decode(errors='replace')[:500]}"
            )
        n = 0
        for p in Path(td).rglob("*.xml"):
            try:
                yield _oid_from_name(p.name), p.read_bytes()
                n += 1
            except OSError as e:
                log.warning("%s-extract read fail %s: %s", tool, p.name, e)
        log.info("%s fallback extracted %d xml files from %s", tool, n, zip_path.name)


_DETAILS_INSERT_SQL = """
INSERT INTO filing_details(
    object_id, ein, return_type, tax_year,
    tax_period_begin, tax_period_end,
    org_name, org_address, city, state, zip,
    mission, website,
    total_revenue, total_expenses,
    total_assets_eoy, total_liabilities_eoy, net_assets_eoy,
    officers_json,
    gross_receipts, formation_yr, legal_domicile_state,
    principal_officer, phone,
    voting_members_cnt, independent_members_cnt,
    total_employees, total_volunteers, total_gross_ubi,
    py_total_revenue, cy_contributions, cy_program_service_revenue,
    cy_investment_income, cy_salaries, cy_grants_paid, cy_fundraising_expense,
    total_assets_boy, total_liabilities_boy, net_assets_boy,
    total_reportable_comp, indiv_rcvd_greater_100k_cnt,
    flags_json, raw_data_json
) VALUES (
    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
)
ON CONFLICT(object_id) DO UPDATE SET
    ein                          = excluded.ein,
    return_type                  = excluded.return_type,
    tax_year                     = excluded.tax_year,
    tax_period_begin             = excluded.tax_period_begin,
    tax_period_end               = excluded.tax_period_end,
    org_name                     = excluded.org_name,
    org_address                  = excluded.org_address,
    city                         = excluded.city,
    state                        = excluded.state,
    zip                          = excluded.zip,
    mission                      = excluded.mission,
    website                      = excluded.website,
    total_revenue                = excluded.total_revenue,
    total_expenses               = excluded.total_expenses,
    total_assets_eoy             = excluded.total_assets_eoy,
    total_liabilities_eoy        = excluded.total_liabilities_eoy,
    net_assets_eoy               = excluded.net_assets_eoy,
    officers_json                = excluded.officers_json,
    gross_receipts               = excluded.gross_receipts,
    formation_yr                 = excluded.formation_yr,
    legal_domicile_state         = excluded.legal_domicile_state,
    principal_officer            = excluded.principal_officer,
    phone                        = excluded.phone,
    voting_members_cnt           = excluded.voting_members_cnt,
    independent_members_cnt      = excluded.independent_members_cnt,
    total_employees              = excluded.total_employees,
    total_volunteers             = excluded.total_volunteers,
    total_gross_ubi              = excluded.total_gross_ubi,
    py_total_revenue             = excluded.py_total_revenue,
    cy_contributions             = excluded.cy_contributions,
    cy_program_service_revenue   = excluded.cy_program_service_revenue,
    cy_investment_income         = excluded.cy_investment_income,
    cy_salaries                  = excluded.cy_salaries,
    cy_grants_paid               = excluded.cy_grants_paid,
    cy_fundraising_expense       = excluded.cy_fundraising_expense,
    total_assets_boy             = excluded.total_assets_boy,
    total_liabilities_boy        = excluded.total_liabilities_boy,
    net_assets_boy               = excluded.net_assets_boy,
    total_reportable_comp        = excluded.total_reportable_comp,
    indiv_rcvd_greater_100k_cnt  = excluded.indiv_rcvd_greater_100k_cnt,
    flags_json                   = excluded.flags_json,
    raw_data_json                = excluded.raw_data_json,
    parsed_at                    = datetime('now')
"""


def _row_from_extract(object_id: str, data: dict) -> tuple:
    return (
        object_id,
        data.get("ein") or "",
        data.get("return_type"),
        data.get("tax_year"),
        data.get("tax_period_begin"),
        data.get("tax_period_end"),
        data.get("org_name"),
        data.get("org_address"),
        data.get("city"),
        data.get("state"),
        data.get("zip"),
        data.get("mission"),
        data.get("website"),
        data.get("total_revenue"),
        data.get("total_expenses"),
        data.get("total_assets_eoy"),
        data.get("total_liabilities_eoy"),
        data.get("net_assets_eoy"),
        json.dumps(data.get("officers") or []),
        data.get("gross_receipts"),
        data.get("formation_yr"),
        data.get("legal_domicile_state"),
        data.get("principal_officer"),
        data.get("phone"),
        data.get("voting_members_cnt"),
        data.get("independent_members_cnt"),
        data.get("total_employees"),
        data.get("total_volunteers"),
        data.get("total_gross_ubi"),
        data.get("py_total_revenue"),
        data.get("cy_contributions"),
        data.get("cy_program_service_revenue"),
        data.get("cy_investment_income"),
        data.get("cy_salaries"),
        data.get("cy_grants_paid"),
        data.get("cy_fundraising_expense"),
        data.get("total_assets_boy"),
        data.get("total_liabilities_boy"),
        data.get("net_assets_boy"),
        data.get("total_reportable_comp"),
        data.get("indiv_rcvd_greater_100k_cnt"),
        json.dumps(data.get("flags") or {}),
        json.dumps(data.get("raw_data") or {}),
    )


_PERSONS_INSERT_SQL = """
INSERT OR IGNORE INTO filing_persons(
    object_id, ein, tax_year, person_role,
    name, name_norm, title,
    reportable_comp, other_comp, related_org_comp,
    hours_per_week, hours_related,
    is_officer, is_director, is_key_employee, is_hce, is_former,
    services_desc
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def _person_row(object_id: str, p: dict) -> tuple:
    return (
        object_id,
        p.get("ein") or "",
        p.get("tax_year"),
        p.get("person_role") or "unknown",
        p.get("name"),
        p.get("name_norm"),
        p.get("title"),
        p.get("reportable_comp"),
        p.get("other_comp"),
        p.get("related_org_comp"),
        p.get("hours_per_week"),
        p.get("hours_related"),
        p.get("is_officer"),
        p.get("is_director"),
        p.get("is_key_employee"),
        p.get("is_hce"),
        p.get("is_former"),
        p.get("services_desc"),
    )


def run_parse(
    years: list[int] | None = None,
    limit_zips: int | None = None,
    limit_per_zip: int | None = None,
) -> dict:
    """Parse every on-disk batch ZIP and populate filing_details.

    Batches inserts per-zip for speed. filings.parsed is back-filled
    in one SQL pass at the end rather than per row.
    """
    parsed_count = 0
    failed = 0
    zips_done = 0

    with session() as conn:
        # Speed knobs. WAL + NORMAL are already set in schema; this run
        # only writes filing_details (plus one final UPDATE on filings),
        # so we can relax synchronous for the duration.
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA cache_size = -262144")   # ~256 MB page cache

        run_id = record_run_start(
            conn, "parse",
            f"years={years} limit_zips={limit_zips} limit_per_zip={limit_per_zip}",
        )

        q = "SELECT * FROM xml_batches WHERE status='done'"
        params: tuple = ()
        if years:
            q += " AND year IN (" + ",".join("?" * len(years)) + ")"
            params = tuple(years)
        q += " ORDER BY year, batch_id"
        batches = list(conn.execute(q, params))
        if limit_zips:
            batches = batches[:limit_zips]

        for b in batches:
            zp = Path(b["local_path"]) if b["local_path"] else XML_DIR / str(b["year"]) / f"{b['batch_id']}.zip"
            if not zp.exists():
                continue
            try:
                rows: list[tuple] = []
                person_rows: list[tuple] = []
                zip_failed = 0
                for i, (object_id, xml_bytes) in enumerate(iter_xml_in_zip(zp)):
                    if limit_per_zip and i >= limit_per_zip:
                        break
                    data = extract(xml_bytes)
                    if not data:
                        zip_failed += 1
                        continue
                    rows.append(_row_from_extract(object_id, data))
                    for p in data.get("persons") or []:
                        person_rows.append(_person_row(object_id, p))

                if rows:
                    conn.executemany(_DETAILS_INSERT_SQL, rows)
                if person_rows:
                    conn.executemany(_PERSONS_INSERT_SQL, person_rows)
                conn.commit()
                parsed_count += len(rows)
                failed += zip_failed
                zips_done += 1
                log.info(
                    "parsed %s: +%d rows +%d persons (total=%d failed=%d)",
                    b["batch_id"], len(rows), len(person_rows), parsed_count, failed,
                )
            except Exception as e:
                # One bad batch shouldn't kill a 5M-row run. Log + move on.
                log.error(
                    "batch %s failed: %s: %s",
                    b["batch_id"], type(e).__name__, e,
                )
                conn.rollback()

        # Single bulk update: mark every filings row that now has a
        # filing_details entry as parsed. Way faster than per-row updates
        # in the hot loop.
        log.info("back-filling filings.parsed...")
        conn.execute(
            """
            UPDATE filings
               SET parsed = 1
             WHERE object_id IN (SELECT object_id FROM filing_details)
            """
        )
        conn.commit()

        record_run_end(
            conn, run_id, "ok",
            rows_added=parsed_count,
            notes=f"zips={zips_done} failed={failed}",
        )
    return {"parsed": parsed_count, "failed": failed, "zips": zips_done}
