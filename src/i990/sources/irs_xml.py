"""IRS Form 990 XML bulk source.

Two layers:
  1. Index ingest:  per-year `index_{YYYY}.csv` maps every published filing
     to its containing ZIP batch (XML_BATCH_ID). This is small (~50 MB per
     year combined) and makes the `filings` table usable without any XML.
  2. Bulk XML:      opt-in download of `{YYYY}_TEOS_XML_*.zip` archives,
     roughly 100-200 MB each, dozens per year.

Year coverage:
  - 2020-2026 use the `{year}_TEOS_XML_{NN}{A|B|C|D}.zip` naming.
  - 2016-2019 have an older mixed naming; we discover batches from the
    year's index CSV (the XML_BATCH_ID column contains the ZIP stem).
"""
from __future__ import annotations

import csv
import fcntl
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ..config import DATA, INDEX_DIR, IRS_XML_BASE, IRS_XML_YEARS, XML_DIR
from ..db import record_run_end, record_run_start, session
from ..http import download_resumable, head


@contextmanager
def _download_lock() -> Iterator[None]:
    """Exclusive file lock so only one download-xml run can execute at a time.

    Two concurrent runs would append to the same .part file and corrupt it.
    """
    lock_path = DATA / "download-xml.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                f"another download-xml run holds the lock at {lock_path}"
            )
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, f"{os.getpid()}\n".encode())
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

log = logging.getLogger(__name__)


def index_url(year: int) -> str:
    return f"{IRS_XML_BASE}/{year}/index_{year}.csv"


_BATCH_RE = re.compile(r"^(\d{4}_TEOS_XML_\d+)([A-Za-z])$")


def normalize_batch_id(batch_id: str) -> str:
    """The 2024 IRS index CSV has a typo: `2024_TEOS_XML_04a` (lowercase).
    The actual ZIP on the server is uppercase. Normalize the trailing
    letter suffix on TEOS-style ids; leave other formats (e.g.
    `download990xml_2019_1`) untouched."""
    m = _BATCH_RE.match(batch_id)
    if m:
        return f"{m.group(1)}{m.group(2).upper()}"
    return batch_id


def batch_url(year: int, batch_id: str) -> str:
    # batch_id e.g. "2024_TEOS_XML_01A" -> .zip file in that year's dir.
    return f"{IRS_XML_BASE}/{year}/{normalize_batch_id(batch_id)}.zip"


# Manifest of ZIP batches per year as published on
# https://www.irs.gov/charities-non-profits/form-990-series-downloads .
# Years 2016-2018 have index CSVs but no ZIP archive is hosted on
# apps.irs.gov (they were previously in the irs-form-990 S3 bucket).
# For 2019-2020, the year's index CSV does NOT contain a batch_id column
# so we cannot map individual filings to their ZIP until the ZIPs are
# downloaded and scanned. Until then, rows in `filings` for those years
# will have xml_batch_id=NULL, and the batches are registered manually
# in `xml_batches` so `download-xml` still has something to fetch.
BATCH_MANIFEST: dict[int, list[str]] = {
    2019: [f"download990xml_2019_{i}" for i in range(1, 9)] + ["2019_TEOS_XML_CT1"],
    2020: [f"download990xml_2020_{i}" for i in range(1, 9)] + ["2020_TEOS_XML_CT1"],
    2021: ["2021_TEOS_XML_01A"],
    2022: ["2022_TEOS_XML_01A"],
    2023: [f"2023_TEOS_XML_{m:02d}A" for m in range(1, 13)],
    2024: [f"2024_TEOS_XML_{m:02d}A" for m in range(1, 13)],
    2025: (
        [f"2025_TEOS_XML_{m:02d}A" for m in range(1, 13)]
        + ["2025_TEOS_XML_05B", "2025_TEOS_XML_11B", "2025_TEOS_XML_11C", "2025_TEOS_XML_11D"]
    ),
    2026: ["2026_TEOS_XML_01A", "2026_TEOS_XML_02A"],
}


def fetch_index(year: int, force: bool = False) -> Path:
    dest = INDEX_DIR / f"index_{year}.csv"
    if force and dest.exists():
        dest.unlink()
    return download_resumable(index_url(year), dest)


def _to_int(v: str) -> int | None:
    v = (v or "").strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _parse_sub_year(v: str) -> int | None:
    """SUB_DATE is sometimes an integer year ("2024") and sometimes a full
    date string like "1/3/2017 8:55:06 AM". Return just the 4-digit year."""
    v = (v or "").strip()
    if not v:
        return None
    # Integer-like year
    try:
        n = int(v)
        if 1900 < n < 2100:
            return n
    except ValueError:
        pass
    # Look for any 4-digit year in the string
    import re
    m = re.search(r"\b(19|20)\d{2}\b", v)
    if m:
        return int(m.group(0))
    return None


def _pad_ein(v: str) -> str:
    return (v or "").strip().zfill(9)


def ingest_index(conn: sqlite3.Connection, year: int, csv_path: Path) -> tuple[int, int]:
    """Upsert one year of the IRS index into filings. Returns (added, updated)."""
    added = 0
    updated = 0

    sql = """
        INSERT INTO filings(
            object_id, ein, return_id, filing_type, return_type,
            tax_period, sub_year, taxpayer_name, dln,
            xml_batch_id, zip_url
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(object_id) DO UPDATE SET
            ein            = excluded.ein,
            return_id      = excluded.return_id,
            filing_type    = excluded.filing_type,
            return_type    = excluded.return_type,
            tax_period     = excluded.tax_period,
            sub_year       = excluded.sub_year,
            taxpayer_name  = excluded.taxpayer_name,
            dln            = excluded.dln,
            xml_batch_id   = excluded.xml_batch_id,
            zip_url        = excluded.zip_url
    """

    batch: list[tuple] = []
    object_ids_in_batch: list[str] = []
    BATCH_SIZE = 5000
    batch_ids_seen: set[str] = set()

    def flush() -> None:
        nonlocal added, updated, batch, object_ids_in_batch
        if not batch:
            return
        existing = {
            r[0]
            for r in conn.execute(
                "SELECT object_id FROM filings WHERE object_id IN ("
                + ",".join("?" * len(object_ids_in_batch)) + ")",
                object_ids_in_batch,
            )
        }
        conn.executemany(sql, batch)
        for oid in object_ids_in_batch:
            if oid in existing:
                updated += 1
            else:
                added += 1
        batch = []
        object_ids_in_batch = []

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        fields = set(reader.fieldnames or [])
        required = {"RETURN_ID", "EIN", "RETURN_TYPE", "OBJECT_ID"}
        missing = required - fields
        if missing:
            raise ValueError(f"index {year}: missing columns: {missing}")
        has_batch_col = "XML_BATCH_ID" in fields
        for row in reader:
            object_id = (row.get("OBJECT_ID") or "").strip()
            if not object_id:
                continue
            batch_id = ""
            if has_batch_col:
                batch_id = normalize_batch_id((row.get("XML_BATCH_ID") or "").strip())
                if batch_id:
                    batch_ids_seen.add(batch_id)
            batch.append((
                object_id,
                _pad_ein(row.get("EIN") or ""),
                (row.get("RETURN_ID") or "").strip() or None,
                (row.get("FILING_TYPE") or "").strip() or None,
                (row.get("RETURN_TYPE") or "").strip() or None,
                (row.get("TAX_PERIOD") or "").strip() or None,
                _parse_sub_year(row.get("SUB_DATE") or ""),
                (row.get("TAXPAYER_NAME") or "").strip() or None,
                (row.get("DLN") or "").strip() or None,
                batch_id or None,
                batch_url(year, batch_id) if batch_id else None,
            ))
            object_ids_in_batch.append(object_id)
            if len(batch) >= BATCH_SIZE:
                flush()
    flush()

    # Register batches for this year. Prefer the CSV's own XML_BATCH_ID
    # column when present; otherwise fall back to the hardcoded manifest.
    bids_to_register = batch_ids_seen or set(BATCH_MANIFEST.get(year, []))
    for bid in sorted(bids_to_register):
        conn.execute(
            """
            INSERT INTO xml_batches(batch_id, year, url, status)
            VALUES (?, ?, ?, 'pending')
            ON CONFLICT(batch_id) DO UPDATE SET url=excluded.url
            """,
            (bid, year, batch_url(year, bid)),
        )
    return added, updated


def run_fetch_index(years: list[int] | None = None, force: bool = False) -> dict:
    years = years or IRS_XML_YEARS
    stats: dict[int, dict] = {}
    with session() as conn:
        run_id = record_run_start(conn, "irs_index", ",".join(str(y) for y in years))
        total_added = 0
        total_updated = 0
        try:
            for y in years:
                try:
                    path = fetch_index(y, force=force)
                except Exception as e:
                    log.warning("index %d not available: %s", y, e)
                    stats[y] = {"error": str(e)}
                    continue
                added, updated = ingest_index(conn, y, path)
                conn.commit()
                stats[y] = {"added": added, "updated": updated}
                total_added += added
                total_updated += updated
                log.info("index %d: +%d ~%d", y, added, updated)
            record_run_end(
                conn, run_id, "ok",
                rows_added=total_added, rows_updated=total_updated,
                notes=str(stats),
            )
        except Exception as e:
            record_run_end(conn, run_id, "error", notes=f"{type(e).__name__}: {e}")
            raise
    return stats


# --- Bulk XML download --------------------------------------------------

def _batches_for(conn: sqlite3.Connection, years: list[int] | None) -> list[sqlite3.Row]:
    if years:
        q = (
            "SELECT * FROM xml_batches WHERE year IN ("
            + ",".join("?" * len(years))
            + ") ORDER BY year, batch_id"
        )
        return list(conn.execute(q, years))
    return list(conn.execute("SELECT * FROM xml_batches ORDER BY year, batch_id"))


def run_download_xml(
    years: list[int] | None = None,
    limit: int | None = None,
) -> dict:
    """Download every registered batch ZIP to data/xml/{year}/.

    Resumable, idempotent. Flips filings.on_disk=1 per batch after success.
    """
    downloaded = 0
    skipped = 0
    errors: list[str] = []

    # Prevent two concurrent download-xml runs from stomping on the same
    # .part file — see the 2019-batch corruption incident.
    with _download_lock(), session() as conn:
        run_id = record_run_start(
            conn, "irs_xml",
            f"years={years} limit={limit}",
        )
        batches = _batches_for(conn, years)
        if limit is not None:
            batches = batches[:limit]

        for b in batches:
            year = int(b["year"])
            bid = b["batch_id"]
            dest_dir = XML_DIR / str(year)
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / f"{bid}.zip"

            if dest.exists():
                size = dest.stat().st_size
                conn.execute(
                    """
                    UPDATE xml_batches
                       SET status='done', bytes_on_disk=?, local_path=?,
                           downloaded_at = COALESCE(downloaded_at, datetime('now'))
                     WHERE batch_id=?
                    """,
                    (size, str(dest), bid),
                )
                conn.execute(
                    "UPDATE filings SET on_disk=1 WHERE xml_batch_id=?",
                    (bid,),
                )
                skipped += 1
                continue

            expected: int | None = None
            try:
                h = head(b["url"])
                if "content-length" in h:
                    expected = int(h["content-length"])
            except Exception:
                pass

            conn.execute(
                """
                UPDATE xml_batches
                   SET status='downloading', bytes_expected=?, url=?, local_path=?
                 WHERE batch_id=?
                """,
                (expected, b["url"], str(dest), bid),
            )
            conn.commit()

            try:
                log.info("downloading %s (%s bytes)", bid, expected)
                download_resumable(b["url"], dest)
                size = dest.stat().st_size
                conn.execute(
                    """
                    UPDATE xml_batches
                       SET status='done', bytes_on_disk=?,
                           downloaded_at=datetime('now')
                     WHERE batch_id=?
                    """,
                    (size, bid),
                )
                conn.execute(
                    "UPDATE filings SET on_disk=1 WHERE xml_batch_id=?",
                    (bid,),
                )
                conn.commit()
                downloaded += 1
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                errors.append(f"{bid}: {msg}")
                conn.execute(
                    "UPDATE xml_batches SET status='error', error=? WHERE batch_id=?",
                    (msg, bid),
                )
                conn.commit()
                log.error("download %s failed: %s", bid, msg)

        notes = f"downloaded={downloaded} skipped={skipped} errors={len(errors)}"
        record_run_end(
            conn, run_id, "ok" if not errors else "partial",
            rows_added=downloaded, notes=notes,
        )

    return {"downloaded": downloaded, "skipped": skipped, "errors": errors}
