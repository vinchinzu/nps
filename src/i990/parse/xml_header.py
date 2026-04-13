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


def _officers(root: ET.Element, limit: int = 10) -> list[dict]:
    """Extract Form 990 Part VII officer/key employee rows."""
    out: list[dict] = []
    for el in root.iter():
        if _localname(el.tag) != "Form990PartVIISectionAGrp":
            continue
        name = _text(_find(el, "PersonNm")) or _text(_find(el, "BusinessName/BusinessNameLine1Txt"))
        title = _text(_find(el, "TitleTxt"))
        comp = _int(_find(el, "ReportableCompFromOrgAmt"))
        other = _int(_find(el, "OtherCompensationAmt"))
        hours = _text(_find(el, "AverageHoursPerWeekRt"))
        out.append({
            "name": name,
            "title": title,
            "reportable_comp": comp,
            "other_comp": other,
            "hours_per_week": hours,
        })
        if len(out) >= limit:
            break
    return out


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
        "ReturnData/IRS990/NetAssetsOrFundBalancesEOYAmt",
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

    return {
        "ein": _text(_find(hdr, "Filer/EIN")),
        "return_type": _text(_find(hdr, "ReturnTypeCd")),
        "tax_year": _int(_find(hdr, "TaxYr")),
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
    officers_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(object_id) DO UPDATE SET
    ein                  = excluded.ein,
    return_type          = excluded.return_type,
    tax_year             = excluded.tax_year,
    tax_period_begin     = excluded.tax_period_begin,
    tax_period_end       = excluded.tax_period_end,
    org_name             = excluded.org_name,
    org_address          = excluded.org_address,
    city                 = excluded.city,
    state                = excluded.state,
    zip                  = excluded.zip,
    mission              = excluded.mission,
    website              = excluded.website,
    total_revenue        = excluded.total_revenue,
    total_expenses       = excluded.total_expenses,
    total_assets_eoy     = excluded.total_assets_eoy,
    total_liabilities_eoy = excluded.total_liabilities_eoy,
    net_assets_eoy       = excluded.net_assets_eoy,
    officers_json        = excluded.officers_json,
    parsed_at            = datetime('now')
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
                zip_parsed = 0
                zip_failed = 0
                for i, (object_id, xml_bytes) in enumerate(iter_xml_in_zip(zp)):
                    if limit_per_zip and i >= limit_per_zip:
                        break
                    data = extract(xml_bytes)
                    if not data:
                        zip_failed += 1
                        continue
                    rows.append(_row_from_extract(object_id, data))

                if rows:
                    conn.executemany(_DETAILS_INSERT_SQL, rows)
                conn.commit()
                parsed_count += len(rows)
                failed += zip_failed
                zips_done += 1
                log.info(
                    "parsed %s: +%d rows (total=%d failed=%d)",
                    b["batch_id"], len(rows), parsed_count, failed,
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
