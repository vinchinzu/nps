#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "i990.sqlite"
ARTIFACT_ROOT = ROOT / "data" / "external" / "doj_nonprofit_fraud_cases"
OUTPUT_ROOT = ROOT / "docs" / "samples"
JSON_OUT = OUTPUT_ROOT / "nonprofit_fraud_cases_sample.json"
CSV_OUT = OUTPUT_ROOT / "nonprofit_fraud_cases_sample.csv"
MANIFEST_OUT = OUTPUT_ROOT / "nonprofit_fraud_cases_manifest.json"

USER_AGENT = "i990-case-sample/1.0"

STATE_BY_TOKEN = {
    "AL": "AL", "ALABAMA": "AL", "AK": "AK", "ALASKA": "AK", "AZ": "AZ", "ARIZONA": "AZ",
    "AR": "AR", "ARKANSAS": "AR", "CA": "CA", "CALIFORNIA": "CA", "CO": "CO", "COLORADO": "CO",
    "CT": "CT", "CONNECTICUT": "CT", "DC": "DC", "DISTRICT OF COLUMBIA": "DC", "DE": "DE", "DELAWARE": "DE",
    "FL": "FL", "FLORIDA": "FL", "GA": "GA", "GEORGIA": "GA", "HI": "HI", "HAWAII": "HI",
    "IA": "IA", "IOWA": "IA", "ID": "ID", "IDAHO": "ID", "IL": "IL", "ILLINOIS": "IL",
    "IN": "IN", "INDIANA": "IN", "KS": "KS", "KANSAS": "KS", "KY": "KY", "KENTUCKY": "KY",
    "LA": "LA", "LOUISIANA": "LA", "MA": "MA", "MASSACHUSETTS": "MA", "MD": "MD", "MARYLAND": "MD",
    "ME": "ME", "MAINE": "ME", "MI": "MI", "MICHIGAN": "MI", "MN": "MN", "MINNESOTA": "MN",
    "MO": "MO", "MISSOURI": "MO", "MS": "MS", "MISSISSIPPI": "MS", "MT": "MT", "MONTANA": "MT",
    "NC": "NC", "NORTH CAROLINA": "NC", "ND": "ND", "NORTH DAKOTA": "ND", "NE": "NE", "NEBRASKA": "NE",
    "NH": "NH", "NEW HAMPSHIRE": "NH", "NJ": "NJ", "NEW JERSEY": "NJ", "NM": "NM", "NEW MEXICO": "NM",
    "NV": "NV", "NEVADA": "NV", "NY": "NY", "NEW YORK": "NY", "OH": "OH", "OHIO": "OH",
    "OK": "OK", "OKLAHOMA": "OK", "OR": "OR", "OREGON": "OR", "PA": "PA", "PENNSYLVANIA": "PA",
    "RI": "RI", "RHODE ISLAND": "RI", "SC": "SC", "SOUTH CAROLINA": "SC", "SD": "SD", "SOUTH DAKOTA": "SD",
    "TN": "TN", "TENNESSEE": "TN", "TX": "TX", "TEXAS": "TX", "UT": "UT", "UTAH": "UT",
    "VA": "VA", "VIRGINIA": "VA", "VT": "VT", "VERMONT": "VT", "WA": "WA", "WASHINGTON": "WA",
    "WI": "WI", "WISCONSIN": "WI", "WV": "WV", "WEST VIRGINIA": "WV", "WY": "WY", "WYOMING": "WY",
}


@dataclass(frozen=True)
class SeedCase:
    case_id: str
    event_date: str
    primary_entity_name: str
    entity_aliases: tuple[str, ...]
    location: str
    source_urls: tuple[str, ...]
    scheme_tags: tuple[str, ...]
    summary: str
    amount_text: str | None = None


SEED_CASES: tuple[SeedCase, ...] = (
    SeedCase(
        case_id="splc-2026",
        event_date="2026-04-21",
        primary_entity_name="SOUTHERN POVERTY LAW CENTER",
        entity_aliases=("SOUTHERN POVERTY LAW CENTER INC",),
        location="Montgomery, AL",
        source_urls=(
            "https://www.justice.gov/opa/pr/federal-grand-jury-charges-southern-poverty-law-center-wire-fraud-false-statements-and",
        ),
        scheme_tags=(
            "donor-fund-misuse",
            "fictitious-entities",
            "bank-false-statements",
            "concealment-money-laundering",
            "counterparty-risk",
        ),
        summary="DOJ alleges SPLC diverted donor funds to extremist-linked individuals through covert accounts and fictitious entities.",
        amount_text="more than $3 million",
    ),
    SeedCase(
        case_id="encouraging-leaders-2026",
        event_date="2026-01-09",
        primary_entity_name="ENCOURAGING LEADERS",
        entity_aliases=(),
        location="Minneapolis, MN",
        source_urls=(
            "https://www.justice.gov/usao-mn/pr/minneapolis-non-profit-director-charged-fraud",
        ),
        scheme_tags=(
            "grant-fraud",
            "false-progress-reports",
            "fabricated-events",
            "false-beneficiary-counts",
        ),
        summary="DOJ alleges the nonprofit retained grant money by submitting false reports about events, services, and beneficiaries.",
    ),
    SeedCase(
        case_id="community-academic-success-2026",
        event_date="2026-04-08",
        primary_entity_name="CENTER FOR COMMUNITY ACADEMIC SUCCESS PARTNERSHIPS",
        entity_aliases=("SOUTH SUBURBAN COMMUNITY SERVICES",),
        location="Chicago area, IL",
        source_urls=(
            "https://www.justice.gov/usao-ndil/pr/former-executive-chicago-area-non-profit-sentenced-prison-19-million-fraud-schemes",
        ),
        scheme_tags=(
            "grant-fraud",
            "americorps-fraud",
            "sham-subcontractors",
            "double-dipping",
            "budget-inflation",
        ),
        summary="Executive sentenced for multiple nonprofit fraud schemes involving inflated grant budgets, sham vendors, and AmeriCorps misuse.",
        amount_text="$1.9 million",
    ),
    SeedCase(
        case_id="philadelphia-religious-nonprofit-2026",
        event_date="2026-03-03",
        primary_entity_name="PHILADELPHIA RELIGIOUS NONPROFIT FUND",
        entity_aliases=(),
        location="Philadelphia, PA",
        source_urls=(
            "https://www.justice.gov/usao-edpa/pr/former-executive-director-philadelphia-non-profit-fund-charged-stealing-over-16",
            "https://www.justice.gov/usao-edpa/pr/former-philadelphia-nonprofit-executive-pleads-guilty-fraud-and-money-laundering",
        ),
        scheme_tags=(
            "beneficiary-fund-theft",
            "false-ledgers",
            "luxury-spending",
            "money-laundering",
        ),
        summary="DOJ says a nonprofit executive diverted beneficiary funds, hid the theft in false ledgers, and laundered proceeds through personal purchases.",
        amount_text="more than $1.6 million",
    ),
    SeedCase(
        case_id="special-needs-trust-2025",
        event_date="2025-06-24",
        primary_entity_name="CENTER FOR SPECIAL NEEDS TRUST ADMINISTRATION",
        entity_aliases=(),
        location="Florida",
        source_urls=(
            "https://www.justice.gov/usao-mdfl/pr/florida-non-profit-founder-and-accountant-charged-stealing-over-100-million-special",
        ),
        scheme_tags=(
            "slush-fund",
            "false-account-statements",
            "trust-diversion",
            "money-laundering-conspiracy",
        ),
        summary="DOJ alleges a nonprofit handling special-needs trusts was used as a slush fund and concealed losses with false account statements.",
        amount_text="over $100 million",
    ),
    SeedCase(
        case_id="viet-america-society-2025",
        event_date="2025-06-06",
        primary_entity_name="VIET AMERICA SOCIETY",
        entity_aliases=("HAND-TO-HAND RELIEF ORGANIZATION",),
        location="Orange County, CA",
        source_urls=(
            "https://www.justice.gov/usao-cdca/pr/founder-oc-based-non-profit-charged-15-count-indictment-alleging-he-bribed-county",
        ),
        scheme_tags=(
            "grant-fraud",
            "bribery",
            "controlled-entities",
            "concealment-laundering",
        ),
        summary="DOJ alleges county grant funds were steered and concealed through controlled entities and false certifications.",
    ),
    SeedCase(
        case_id="bay-city-theatre-2025",
        event_date="2025-09-23",
        primary_entity_name="BAY CITY STATE THEATRE",
        entity_aliases=("BAY CITY HISTORICAL SOCIETY",),
        location="Bay City, MI",
        source_urls=(
            "https://www.justice.gov/usao-edmi/pr/former-non-profit-executive-director-and-city-development-official-pleads-guilty",
        ),
        scheme_tags=(
            "mission-fund-diversion",
            "fictitious-board-minutes",
            "fake-invoices",
            "fraudulent-grant-replenishment",
        ),
        summary="Funds earmarked for nonprofit missions were diverted and covered with fake minutes, fake invoices, and a fraudulent grant application.",
    ),
    SeedCase(
        case_id="washington-crime-victim-advocates-2024",
        event_date="2024-07-17",
        primary_entity_name="WASHINGTON COALITION OF CRIME VICTIM ADVOCATES",
        entity_aliases=(),
        location="Washington",
        source_urls=(
            "https://www.justice.gov/usao-wdwa/pr/woman-who-fraudulently-used-state-grant-monies-sentenced-probation-and-home",
        ),
        scheme_tags=(
            "grant-fraud",
            "false-invoices",
            "no-show-executive",
            "salary-extraction",
        ),
        summary="DOJ says the executive extracted salary and fabricated invoices for trainings that never occurred.",
    ),
    SeedCase(
        case_id="blm-greater-atlanta-2024",
        event_date="2024-10-06",
        primary_entity_name="BLACK LIVES MATTER OF GREATER ATLANTA",
        entity_aliases=(),
        location="Georgia",
        source_urls=(
            "https://www.justice.gov/usao-ndoh/pr/blm-activist-sentenced-prison-wire-fraud-and-money-laundering",
        ),
        scheme_tags=(
            "donation-diversion",
            "personal-spending",
            "real-estate-concealment",
            "money-laundering",
        ),
        summary="Sentencing case involving donation diversion, property-related concealment, and money laundering.",
    ),
    SeedCase(
        case_id="michele-fiore-charity-2024",
        event_date="2024-07-17",
        primary_entity_name="FIRENZA MEMORIAL CHARITY FUND",
        entity_aliases=(),
        location="Las Vegas, NV",
        source_urls=(
            "https://www.justice.gov/archives/opa/pr/former-las-vegas-city-councilwoman-charged-charity-fraud-scheme",
        ),
        scheme_tags=(
            "charity-fraud",
            "donor-use-misrepresentation",
            "personal-spending",
        ),
        summary="DOJ alleges donations solicited for a memorial charity purpose were used for personal and family expenses.",
    ),
    SeedCase(
        case_id="citadel-community-2024",
        event_date="2024-05-07",
        primary_entity_name="CITADEL COMMUNITY DEVELOPMENT CORPORATION",
        entity_aliases=("CITADEL COMMUNITY CARE FACILITY",),
        location="California",
        source_urls=(
            "https://www.justice.gov/usao-cdca/pr/former-inland-empire-nonprofit-ceo-arrested-indictment-alleging-she-embezzled-federal",
        ),
        scheme_tags=(
            "federal-grant-embezzlement",
            "personal-spending",
            "crypto",
        ),
        summary="CEO charged with embezzling federal grant funds for personal spending including wedding costs, travel, and crypto.",
    ),
    SeedCase(
        case_id="latino-coalition-foundation-2023",
        event_date="2023-04-21",
        primary_entity_name="LATINO COALITION FOUNDATION",
        entity_aliases=("HISPANIC BUSINESS ROUNDTABLE INSTITUTE",),
        location="San Antonio, TX",
        source_urls=(
            "https://www.justice.gov/usao-wdtx/pr/former-nonprofit-leader-pleads-guilty-fraud-san-antonio",
        ),
        scheme_tags=(
            "donation-diversion",
            "false-990",
            "insider-bank-control",
        ),
        summary="Former nonprofit leader pleaded guilty after diverting donations and causing false Form 990 filings.",
    ),
    SeedCase(
        case_id="douglas-sailors-charity-network-2022",
        event_date="2022-11-04",
        primary_entity_name="CANCER FUND OF AMERICA",
        entity_aliases=("BREAST CANCER SOCIETY", "CHILDREN'S CANCER FUND OF AMERICA", "THE FIREFIGHTERS CHARITABLE FOUNDATION"),
        location="Florida / national",
        source_urls=(
            "https://www.justice.gov/usao-sdfl/pr/charity-operator-charged-diverting-millions-dollars-charitable-funds-and-evading",
        ),
        scheme_tags=(
            "nominee-directors",
            "lookalike-charities",
            "management-company-extraction",
            "tax-fraud",
        ),
        summary="DOJ charged a charity operator for diverting millions using nominee structures, related management companies, and multiple charities.",
        amount_text="millions of dollars",
    ),
    SeedCase(
        case_id="on-your-feet-2020",
        event_date="2020-03-13",
        primary_entity_name="ON YOUR FEET FOUNDATION",
        entity_aliases=("FAMILY RESOURCE CENTER",),
        location="California",
        source_urls=(
            "https://www.justice.gov/usao-sdca/pr/charity-founders-plead-guilty-using-non-profit-defraud-donors-and-illegally-evade-taxes",
        ),
        scheme_tags=(
            "donor-fraud",
            "false-charitable-returns",
            "personal-spending",
            "tax-evasion",
        ),
        summary="Founders admitted using a nonprofit to defraud donors and evade taxes.",
    ),
    SeedCase(
        case_id="montana-native-womens-coalition-2019",
        event_date="2019-08-29",
        primary_entity_name="MONTANA NATIVE WOMEN'S COALITION",
        entity_aliases=(),
        location="Montana",
        source_urls=(
            "https://www.justice.gov/usao-mt/pr/montana-native-women-s-coalition-board-ex-chairwoman-charged-fraud-embezzlement-grant",
        ),
        scheme_tags=(
            "federal-grant-theft",
            "travel-fraud",
            "duplicate-payments",
            "internal-control-failure",
        ),
        summary="Grant-funded victim-services nonprofit case involving unauthorized payments and travel-related fraud despite prior warning signs.",
    ),
    SeedCase(
        case_id="providence-plan-2017",
        event_date="2017-03-22",
        primary_entity_name="PROVIDENCE PLAN",
        entity_aliases=(),
        location="Providence, RI",
        source_urls=(
            "https://www.justice.gov/usao-ri/pr/former-finance-director-pleads-guilty-embezzlement",
        ),
        scheme_tags=(
            "forged-checks",
            "insider-owned-vendor",
            "grant-diversion",
            "casino-withdrawals",
        ),
        summary="Finance director admitted diverting federal and private grant funds through forged checks and an insider-controlled entity.",
    ),
    SeedCase(
        case_id="birmingham-health-care-2016",
        event_date="2016-06-17",
        primary_entity_name="BIRMINGHAM HEALTH CARE",
        entity_aliases=("CENTRAL ALABAMA COMPREHENSIVE HEALTH",),
        location="Alabama",
        source_urls=(
            "https://www.justice.gov/usao-ndal/pr/federal-jury-convicts-former-non-profit-health-clinics-ceo-funneling-millions-grant",
        ),
        scheme_tags=(
            "federal-grant-funneling",
            "controlled-companies",
            "bank-fraud",
            "money-laundering",
        ),
        summary="Former nonprofit health-clinic CEO convicted of routing federal grant funds through companies he controlled.",
        amount_text="millions of dollars",
    ),
    SeedCase(
        case_id="frontline-initiative-2015",
        event_date="2015-09-11",
        primary_entity_name="FRONTLINE INITIATIVE",
        entity_aliases=("HERO PROGRAM",),
        location="Pennsylvania",
        source_urls=(
            "https://www.justice.gov/usao-wdpa/pr/charity-director-admits-using-funds-personal-use-filing-false-tax-returns",
        ),
        scheme_tags=(
            "donation-diversion",
            "grant-diversion",
            "personal-spending",
            "false-tax-returns",
        ),
        summary="Charity director admitted using donor and grant funds for extensive personal expenses and filing false tax returns.",
    ),
    SeedCase(
        case_id="national-relief-charities-2013",
        event_date="2013-10-21",
        primary_entity_name="NATIONAL RELIEF CHARITIES",
        entity_aliases=("CHARITY ONE",),
        location="Oregon / national",
        source_urls=(
            "https://www.justice.gov/usao-or/pr/former-president-national-charity-arrested-and-charged-4-million-fraud-and-money",
        ),
        scheme_tags=(
            "charity-to-charity-pass-through",
            "false-financial-statements",
            "scholarship-pretext",
            "money-laundering",
        ),
        summary="DOJ alleged a charity executive diverted funds through a second nonprofit and false scholarship-related representations.",
        amount_text="$4 million",
    ),
    SeedCase(
        case_id="keelys-district-boxing-2013",
        event_date="2013-06-26",
        primary_entity_name="KEELY'S DISTRICT BOXING AND YOUTH CENTER",
        entity_aliases=(),
        location="Washington, DC",
        source_urls=(
            "https://www.justice.gov/usao-dc/pr/executive-director-non-profit-pleads-guilty-wire-fraud-admits-using-more-200000-grants",
        ),
        scheme_tags=(
            "grant-diversion",
            "gambling",
            "personal-expenses",
        ),
        summary="Executive director admitted using youth-program grants for gambling and other personal expenses.",
        amount_text="more than $200,000",
    ),
    SeedCase(
        case_id="global-missions-2013",
        event_date="2013-05-21",
        primary_entity_name="GLOBAL MISSIONS",
        entity_aliases=(),
        location="California",
        source_urls=(
            "https://www.justice.gov/usao-ndca/pr/oakland-man-sentenced-121-months-and-ordered-pay-337-million-charity-fraud-scheme",
        ),
        scheme_tags=(
            "charity-fraud",
            "donor-fraud",
            "luxury-spending",
            "money-laundering",
        ),
        summary="Sentencing in a long-running charity fraud scheme involving phony representations and personal luxury spending.",
        amount_text="$33.7 million restitution",
    ),
    SeedCase(
        case_id="usa-harvest-2013",
        event_date="2013-01-09",
        primary_entity_name="USA HARVEST",
        entity_aliases=(),
        location="Kentucky",
        source_urls=(
            "https://www.justice.gov/usao-wdky/pr/founder-usa-harvest-charged-seven-count-federal-indictment-charges-include-stealing",
        ),
        scheme_tags=(
            "donation-theft",
            "travel-spending",
            "entertainment-spending",
            "money-laundering",
            "tax-fraud",
        ),
        summary="Founder charged with stealing donations and using charity funds for travel, entertainment, and personal expenses.",
    ),
)


def normalize_name(value: str) -> str:
    value = re.sub(r"[^A-Z0-9]+", " ", value.upper())
    tokens = [t for t in value.split() if t not in {"THE", "INC", "INCORPORATED", "CORP", "CORPORATION", "LLC", "LTD", "FOUNDATION"}]
    return " ".join(tokens).strip()


def slugify(value: str) -> str:
    out = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return out or hashlib.sha1(value.encode()).hexdigest()[:12]


def location_state_hint(location: str) -> str | None:
    upper = location.upper()
    for token, state in STATE_BY_TOKEN.items():
        if re.search(rf"\b{re.escape(token)}\b", upper):
            return state
    return None


def fetch_url(url: str) -> tuple[bytes | None, str | None]:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.read(), resp.headers.get_content_type()
    except (HTTPError, URLError, TimeoutError):
        return None, None


def extract_title(html_text: str) -> str | None:
    for pattern in (
        r'<meta property="og:title" content="([^"]+)"',
        r'<meta name="title" content="([^"]+)"',
        r"<title>(.*?)</title>",
    ):
        m = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return re.sub(r"\s+", " ", unescape(m.group(1))).strip()
    return None


def extract_text_excerpt(html_text: str) -> str | None:
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", html_text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", unescape(text)).strip()
    return text[:400] if text else None


def extract_doc_links(base_url: str, html_text: str) -> list[str]:
    links: list[str] = []
    for href in re.findall(r'href="([^"]+)"', html_text, flags=re.IGNORECASE):
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if "justice.gov" not in parsed.netloc:
            continue
        if parsed.path.endswith(".pdf") or "/media/" in parsed.path or parsed.path.endswith("/dl"):
            links.append(full)
    # Preserve order, drop dupes.
    seen: set[str] = set()
    out: list[str] = []
    for link in links:
        if link not in seen:
            seen.add(link)
            out.append(link)
    return out[:5]


def write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def select_filename(url: str, content_type: str | None, default_stem: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    if not name or name in {"dl", "download"}:
        suffix = ".pdf" if content_type == "application/pdf" else ".html"
        name = f"{default_stem}{suffix}"
    return slugify(Path(name).stem) + Path(name).suffix


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def find_i990_match(conn: sqlite3.Connection, names: Iterable[str], location: str) -> dict:
    raw_names = tuple(n for n in names if n)
    norm_names = [normalize_name(n) for n in raw_names]
    if not norm_names:
        return {"matched": False}
    like_params: list[str] = []
    for raw in raw_names:
        upper = raw.upper()
        like_params.extend([upper, f"{upper} %", f"% {upper}", f"% {upper} %"])
    where = " OR ".join(["UPPER(o.name) = ?", "UPPER(o.name) LIKE ?", "UPPER(o.name) LIKE ?", "UPPER(o.name) LIKE ?"] * len(raw_names))
    rows = conn.execute(
        f"""
        SELECT o.*,
               rs.total_score,
               rs.tier,
               (
                 SELECT MAX(d.tax_year) FROM filing_details d WHERE d.ein = o.ein
               ) AS latest_tax_year,
               (
                 SELECT d.total_revenue
                   FROM filing_details d
                  WHERE d.ein = o.ein
                    AND d.total_revenue IS NOT NULL
               ORDER BY d.tax_year DESC LIMIT 1
               ) AS latest_revenue,
               (
                 SELECT d.total_assets_eoy
                   FROM filing_details d
                  WHERE d.ein = o.ein
                    AND d.total_assets_eoy IS NOT NULL
               ORDER BY d.tax_year DESC LIMIT 1
               ) AS latest_assets,
               (
                 SELECT d.website
                   FROM filing_details d
                  WHERE d.ein = o.ein
                    AND d.website IS NOT NULL
               ORDER BY d.tax_year DESC LIMIT 1
               ) AS latest_website
          FROM organizations o
          LEFT JOIN risk_scores rs USING (ein)
         WHERE o.name IS NOT NULL
           AND ({where})
        """,
        like_params,
    ).fetchall()
    best = None
    best_score = 0
    state_hint = location_state_hint(location)
    for row in rows:
        org_norm = normalize_name(row["name"])
        for target in norm_names:
            score = 0
            if org_norm == target:
                score = 100
            if score and state_hint and row["state"] and row["state"] != state_hint:
                score = 0
            if score > best_score:
                best_score = score
                best = row
    if best is None or best_score < 75:
        return {"matched": False, "match_score": best_score}
    return {
        "matched": True,
        "match_score": best_score,
        "ein": best["ein"],
        "org_name": best["name"],
        "state": best["state"],
        "city": best["city"],
        "zip": best["zip"],
        "subsection": best["subsection"],
        "ntee_cd": best["ntee_cd"],
        "ruling": best["ruling"],
        "bmf_region": best["bmf_region"],
        "latest_tax_year": best["latest_tax_year"],
        "latest_revenue": best["latest_revenue"],
        "latest_assets": best["latest_assets"],
        "latest_website": best["latest_website"],
        "risk_total_score": best["total_score"],
        "risk_tier": best["tier"],
    }


def fetch_json(url: str, payload: dict | None = None) -> dict | None:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=data,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def find_usaspending_match(names: Iterable[str]) -> dict:
    for name in names:
        cat = fetch_json(
            "https://api.usaspending.gov/api/v2/search/spending_by_category/recipient/",
            {"filters": {"recipient_search_text": [name]}, "limit": 10, "page": 1},
        )
        if not cat:
            continue
        for row in cat.get("results", []):
            if normalize_name(name) != normalize_name(row.get("name") or ""):
                continue
            detail = fetch_json(
                f"https://api.usaspending.gov/api/v2/recipient/{row['recipient_id']}/"
            )
            if not detail:
                continue
            agencies = fetch_json(
                "https://api.usaspending.gov/api/v2/search/spending_by_category/awarding_agency/",
                {"filters": {"recipient_search_text": [name]}, "limit": 10, "page": 1},
            )
            over_time = fetch_json(
                "https://api.usaspending.gov/api/v2/search/spending_over_time/",
                {"filters": {"recipient_search_text": [name]}, "group": "fiscal_year"},
            )
            return {
                "matched": True,
                "recipient_id": detail.get("recipient_id"),
                "recipient_name": detail.get("name"),
                "uei": detail.get("uei"),
                "duns": detail.get("duns"),
                "alternate_names": detail.get("alternate_names") or [],
                "address": detail.get("location") or {},
                "business_types": detail.get("business_types") or [],
                "agencies": agencies.get("results", []) if agencies else [],
                "spending_over_time": [
                    item for item in (over_time.get("results", []) if over_time else [])
                    if item.get("aggregated_amount") not in (0, 0.0, None)
                ],
            }
    return {"matched": False}


def find_sam_match(names: Iterable[str]) -> dict:
    for name in names:
        url = (
            "https://sam.gov/api/prod/entity-information/v2/entities"
            f"?api_key=public&legalBusinessName={name.replace(' ', '%20')}&page=0&size=10"
        )
        payload = fetch_json(url)
        if not payload:
            continue
        for row in payload.get("entityData", []):
            legal_name = (((row.get("entityRegistration") or {}).get("legalBusinessName")) or "")
            if normalize_name(legal_name) != normalize_name(name):
                continue
            entity_reg = row.get("entityRegistration") or {}
            core = row.get("coreData") or {}
            general = (core.get("generalInformation") or {})
            return {
                "matched": True,
                "legal_business_name": legal_name,
                "uei": entity_reg.get("ueiSAM"),
                "cage": entity_reg.get("cageCode"),
                "registration_status": entity_reg.get("registrationStatus"),
                "registration_date": entity_reg.get("registrationDate"),
                "last_update_date": entity_reg.get("lastUpdateDate"),
                "expiration_date": entity_reg.get("registrationExpirationDate"),
                "purpose_desc": entity_reg.get("purposeOfRegistrationDesc"),
                "no_public_display_flag": entity_reg.get("noPublicDisplayFlag"),
                "address": core.get("physicalAddress") or {},
                "entity_type_desc": general.get("entityTypeDesc"),
                "profit_structure_desc": general.get("profitStructureDesc"),
            }
    return {"matched": False}


def flatten_case(case: dict) -> dict:
    i990 = case.get("i990") or {}
    usa = case.get("usaspending") or {}
    sam = case.get("sam") or {}
    doj = case.get("doj") or {}
    return {
        "case_id": case["case_id"],
        "event_date": case["event_date"],
        "primary_entity_name": case["primary_entity_name"],
        "location": case["location"],
        "scheme_tags": "|".join(case["scheme_tags"]),
        "amount_text": case.get("amount_text") or "",
        "source_titles": "|".join([src.get("title") or "" for src in doj.get("sources", [])]),
        "source_urls": "|".join([src.get("url") or "" for src in doj.get("sources", [])]),
        "downloaded_docs_count": len(doj.get("artifacts", [])),
        "i990_matched": i990.get("matched", False),
        "i990_match_score": i990.get("match_score", ""),
        "ein": i990.get("ein", ""),
        "i990_org_name": i990.get("org_name", ""),
        "i990_state": i990.get("state", ""),
        "i990_ntee_cd": i990.get("ntee_cd", ""),
        "i990_latest_tax_year": i990.get("latest_tax_year", ""),
        "i990_latest_revenue": i990.get("latest_revenue", ""),
        "i990_latest_assets": i990.get("latest_assets", ""),
        "risk_total_score": i990.get("risk_total_score", ""),
        "risk_tier": i990.get("risk_tier", ""),
        "usaspending_matched": usa.get("matched", False),
        "usaspending_recipient_name": usa.get("recipient_name", ""),
        "usaspending_recipient_id": usa.get("recipient_id", ""),
        "usaspending_uei": usa.get("uei", ""),
        "usaspending_duns": usa.get("duns", ""),
        "usaspending_agencies": "|".join(
            f"{r.get('name')}:{r.get('amount')}" for r in (usa.get("agencies") or [])
        ),
        "sam_matched": sam.get("matched", False),
        "sam_legal_business_name": sam.get("legal_business_name", ""),
        "sam_uei": sam.get("uei", ""),
        "sam_cage": sam.get("cage", ""),
        "sam_registration_status": sam.get("registration_status", ""),
    }


def download_case_sources(case: SeedCase) -> dict:
    case_dir = ARTIFACT_ROOT / case.case_id
    case_dir.mkdir(parents=True, exist_ok=True)
    source_entries: list[dict] = []
    artifact_entries: list[dict] = []

    for idx, url in enumerate(case.source_urls, start=1):
        payload, content_type = fetch_url(url)
        if payload is None:
            source_entries.append({"url": url, "downloaded": False})
            continue
        html_path = case_dir / f"source-{idx}.html"
        write_bytes(html_path, payload)
        html_text = payload.decode("utf-8", errors="replace")
        title = extract_title(html_text)
        source_entries.append(
            {
                "url": url,
                "downloaded": True,
                "title": title,
                "content_type": content_type,
                "local_path": str(html_path.relative_to(ROOT)),
                "text_excerpt": extract_text_excerpt(html_text),
            }
        )
        artifact_entries.append(
            {
                "kind": "html",
                "url": url,
                "local_path": str(html_path.relative_to(ROOT)),
                "content_type": content_type,
            }
        )
        for doc_url in extract_doc_links(url, html_text):
            doc_payload, doc_type = fetch_url(doc_url)
            if doc_payload is None:
                continue
            file_name = select_filename(doc_url, doc_type, f"doc-{len(artifact_entries)+1}")
            doc_path = case_dir / file_name
            if not doc_path.exists():
                write_bytes(doc_path, doc_payload)
            artifact_entries.append(
                {
                    "kind": "pdf" if (doc_type or "").endswith("pdf") or doc_path.suffix == ".pdf" else "download",
                    "url": doc_url,
                    "local_path": str(doc_path.relative_to(ROOT)),
                    "content_type": doc_type,
                }
            )
    return {"sources": source_entries, "artifacts": artifact_entries}


def main() -> int:
    if not DB_PATH.exists():
        print(f"missing database: {DB_PATH}", file=sys.stderr)
        return 2

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    conn = connect_db()
    results: list[dict] = []

    for case in SEED_CASES:
        print(f"building {case.case_id}...", file=sys.stderr)
        names = (case.primary_entity_name,) + case.entity_aliases
        doj = download_case_sources(case)
        i990 = find_i990_match(conn, names, case.location)
        usaspending = find_usaspending_match(names)
        sam = find_sam_match(names)
        results.append(
            {
                "case_id": case.case_id,
                "event_date": case.event_date,
                "primary_entity_name": case.primary_entity_name,
                "entity_aliases": list(case.entity_aliases),
                "location": case.location,
                "scheme_tags": list(case.scheme_tags),
                "summary": case.summary,
                "amount_text": case.amount_text,
                "doj": doj,
                "i990": i990,
                "usaspending": usaspending,
                "sam": sam,
            }
        )

    JSON_OUT.write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    with CSV_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(flatten_case(results[0]).keys()))
        writer.writeheader()
        for row in results:
            writer.writerow(flatten_case(row))
    manifest = {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(),
        "json_path": str(JSON_OUT.relative_to(ROOT)),
        "csv_path": str(CSV_OUT.relative_to(ROOT)),
        "artifact_root": str(ARTIFACT_ROOT.relative_to(ROOT)),
        "case_count": len(results),
        "artifact_count": sum(len(row["doj"]["artifacts"]) for row in results),
    }
    MANIFEST_OUT.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
