"""Paths and source URLs. Single source of truth."""
from __future__ import annotations

import os
from pathlib import Path

# --- Paths --------------------------------------------------------------
ROOT = Path(os.environ.get("I990_ROOT", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"
RAW = DATA / "raw"
BMF_DIR = RAW / "bmf"
INDEX_DIR = RAW / "index"
XML_DIR = DATA / "xml"
LOG_DIR = DATA / "logs"
EXPORT_DIR = DATA / "exports"
DB_PATH = Path(os.environ.get("I990_DB", DATA / "i990.sqlite"))

for p in (DATA, RAW, BMF_DIR, INDEX_DIR, XML_DIR, LOG_DIR, EXPORT_DIR):
    p.mkdir(parents=True, exist_ok=True)

# --- Sources ------------------------------------------------------------

# IRS Exempt Organizations Business Master File. Regions:
#   eo1 = Northeast   (CT, ME, MA, NH, NY, RI, VT)
#   eo2 = Mid-Atlantic/Great Lakes (DE, DC, IL, IN, KY, MD, MI, NC, NJ, OH, PA, TN, VA, WV)
#   eo3 = All other states + US territories
#   eo4 = International organizations
BMF_URLS = {
    "eo1": "https://www.irs.gov/pub/irs-soi/eo1.csv",
    "eo2": "https://www.irs.gov/pub/irs-soi/eo2.csv",
    "eo3": "https://www.irs.gov/pub/irs-soi/eo3.csv",
    "eo4": "https://www.irs.gov/pub/irs-soi/eo4.csv",
}

# Per-year index CSV of every e-filed 990 the IRS has published.
# Pattern: https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv
IRS_XML_BASE = "https://apps.irs.gov/pub/epostcard/990/xml"

# Years the IRS currently publishes. 2016-2018 use an older S3 layout and a
# different ZIP naming scheme; code handles both via index discovery.
IRS_XML_YEARS = list(range(2016, 2027))

# ProPublica Nonprofit Explorer (pre-2016 gap filler, rate-limited, opt-in).
PROPUBLICA_API = "https://projects.propublica.org/nonprofits/api/v2"

USER_AGENT = "i990-ingest/0.1 (+https://example.invalid; contact=local)"
