"""IRS Exempt Organizations Business Master File.

The BMF is the authoritative list of every registered 501(c) organization.
It is split into regional CSVs (eo1..eo4). We download each, normalize
fields, and upsert into the `organizations` table keyed by EIN.
"""
from __future__ import annotations

import csv
import logging
import sqlite3
from pathlib import Path

from ..config import BMF_DIR, BMF_URLS
from ..db import record_run_end, record_run_start, session
from ..http import download_resumable

log = logging.getLogger(__name__)


# Column name in CSV -> column name in DB. BMF uses uppercase headers with
# a handful of SQL-reserved words (GROUP, STATUS) that need renaming.
COLUMN_MAP = {
    "EIN": "ein",
    "NAME": "name",
    "ICO": "ico",
    "STREET": "street",
    "CITY": "city",
    "STATE": "state",
    "ZIP": "zip",
    "GROUP": "group_code",
    "SUBSECTION": "subsection",
    "AFFILIATION": "affiliation",
    "CLASSIFICATION": "classification",
    "RULING": "ruling",
    "DEDUCTIBILITY": "deductibility",
    "FOUNDATION": "foundation",
    "ACTIVITY": "activity",
    "ORGANIZATION": "organization",
    "STATUS": "status",
    "TAX_PERIOD": "tax_period",
    "ASSET_CD": "asset_cd",
    "INCOME_CD": "income_cd",
    "FILING_REQ_CD": "filing_req_cd",
    "PF_FILING_REQ_CD": "pf_filing_req_cd",
    "ACCT_PD": "acct_pd",
    "ASSET_AMT": "asset_amt",
    "INCOME_AMT": "income_amt",
    "REVENUE_AMT": "revenue_amt",
    "NTEE_CD": "ntee_cd",
    "SORT_NAME": "sort_name",
}

INT_COLS = {"asset_amt", "income_amt", "revenue_amt"}


def _to_int(v: str) -> int | None:
    v = (v or "").strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _pad_ein(v: str) -> str:
    return (v or "").strip().zfill(9)


def fetch_region(region: str, force: bool = False) -> Path:
    """Download one regional BMF CSV to data/raw/bmf/{region}.csv."""
    url = BMF_URLS[region]
    dest = BMF_DIR / f"{region}.csv"
    if force and dest.exists():
        dest.unlink()
    log.info("fetching %s from %s", region, url)
    return download_resumable(url, dest)


def ingest_region(conn: sqlite3.Connection, region: str, csv_path: Path) -> tuple[int, int]:
    """Upsert rows from csv_path into organizations. Returns (added, updated)."""
    added = 0
    updated = 0

    db_cols = list(COLUMN_MAP.values()) + ["bmf_region"]
    placeholders = ", ".join(["?"] * len(db_cols))
    col_list = ", ".join(db_cols)
    update_list = ", ".join(f"{c}=excluded.{c}" for c in db_cols if c != "ein")
    sql = (
        f"INSERT INTO organizations({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT(ein) DO UPDATE SET {update_list}"
    )

    # Pre-seen set for add/update accounting (cheap single query).
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _seen(ein TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM _seen")

    batch: list[tuple] = []
    BATCH_SIZE = 5000

    def flush() -> None:
        nonlocal added, updated, batch
        if not batch:
            return
        # Which of these EINs already exist?
        eins = [row[0] for row in batch]
        existing = {
            r[0]
            for r in conn.execute(
                "SELECT ein FROM organizations WHERE ein IN ("
                + ",".join("?" * len(eins)) + ")",
                eins,
            )
        }
        conn.executemany(sql, batch)
        for ein in eins:
            if ein in existing:
                updated += 1
            else:
                added += 1
        batch = []

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        missing = set(COLUMN_MAP) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"BMF {region}: missing expected columns: {missing}")
        for row in reader:
            values: list = []
            for csv_col, db_col in COLUMN_MAP.items():
                v = row.get(csv_col, "")
                if db_col == "ein":
                    values.append(_pad_ein(v))
                elif db_col in INT_COLS:
                    values.append(_to_int(v))
                else:
                    values.append((v or "").strip() or None)
            values.append(region)
            batch.append(tuple(values))
            if len(batch) >= BATCH_SIZE:
                flush()

    flush()
    return added, updated


def run(regions: list[str] | None = None, force: bool = False) -> dict:
    """Fetch and ingest BMF regions. Returns per-region stats."""
    regions = regions or list(BMF_URLS.keys())
    stats: dict[str, dict] = {}

    with session() as conn:
        run_id = record_run_start(conn, "bmf", ",".join(regions))
        total_added = 0
        total_updated = 0
        try:
            for region in regions:
                path = fetch_region(region, force=force)
                added, updated = ingest_region(conn, region, path)
                conn.commit()
                stats[region] = {"added": added, "updated": updated, "path": str(path)}
                total_added += added
                total_updated += updated
                log.info("bmf %s: +%d ~%d", region, added, updated)
            record_run_end(
                conn, run_id, "ok",
                rows_added=total_added, rows_updated=total_updated,
                notes=str(stats),
            )
        except Exception as e:
            record_run_end(conn, run_id, "error", notes=f"{type(e).__name__}: {e}")
            raise

    return stats
