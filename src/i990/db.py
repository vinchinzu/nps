"""SQLite schema, connection, and helpers."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import DB_PATH

SCHEMA = r"""
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA foreign_keys = ON;

-- Every registered exempt organization from the IRS BMF.
CREATE TABLE IF NOT EXISTS organizations (
    ein             TEXT PRIMARY KEY,            -- zero-padded 9-digit string
    name            TEXT NOT NULL,
    ico             TEXT,                        -- "in care of" name
    street          TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    group_code      TEXT,
    subsection      TEXT,                        -- 501(c)(X)
    affiliation     TEXT,
    classification  TEXT,
    ruling          TEXT,                        -- YYYYMM date of IRS ruling
    deductibility   TEXT,
    foundation      TEXT,
    activity        TEXT,
    organization    TEXT,                        -- e.g. 5 = nonprofit corp
    status          TEXT,
    tax_period      TEXT,                        -- YYYYMM
    asset_cd        TEXT,
    income_cd       TEXT,
    filing_req_cd   TEXT,
    pf_filing_req_cd TEXT,
    acct_pd         TEXT,
    asset_amt       INTEGER,
    income_amt      INTEGER,
    revenue_amt     INTEGER,
    ntee_cd         TEXT,
    sort_name       TEXT,
    bmf_region      TEXT NOT NULL,               -- eo1..eo4
    bmf_ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_org_state   ON organizations(state);
CREATE INDEX IF NOT EXISTS idx_org_ntee    ON organizations(ntee_cd);
CREATE INDEX IF NOT EXISTS idx_org_name    ON organizations(name);

-- One row per e-filed 990 published by the IRS.
CREATE TABLE IF NOT EXISTS filings (
    object_id       TEXT PRIMARY KEY,            -- canonical IRS object id
    ein             TEXT NOT NULL,
    return_id       TEXT,
    filing_type     TEXT,                        -- e.g. EFILE
    return_type     TEXT,                        -- 990, 990EZ, 990PF, 990T
    tax_period      TEXT,                        -- YYYYMM
    sub_year        INTEGER,                     -- year the IRS published
    taxpayer_name   TEXT,
    dln             TEXT,
    xml_batch_id    TEXT,                        -- e.g. 2024_TEOS_XML_01A; NULL for 2016-2020 where index has no batch column
    zip_url         TEXT,                        -- full URL to containing ZIP; NULL until batch is known
    on_disk         INTEGER NOT NULL DEFAULT 0,  -- 1 if zip containing it has been downloaded
    parsed          INTEGER NOT NULL DEFAULT 0,  -- 1 if XML has been parsed
    index_ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_filing_ein         ON filings(ein);
CREATE INDEX IF NOT EXISTS idx_filing_batch       ON filings(xml_batch_id);
CREATE INDEX IF NOT EXISTS idx_filing_type        ON filings(return_type);
CREATE INDEX IF NOT EXISTS idx_filing_year        ON filings(sub_year);

-- Parsed header fields per filing (populated by parse-xml step).
--
-- NOTE: No FK to filings(object_id). The IRS index CSVs are incomplete:
-- the 2019 zips contain filings whose object_ids are not listed in
-- index_2019.csv (~hundreds of thousands of extras). We want those rows
-- captured here anyway; filings is back-filled with stubs at the end of
-- the parse run.
CREATE TABLE IF NOT EXISTS filing_details (
    object_id       TEXT PRIMARY KEY,
    ein             TEXT NOT NULL,
    return_type     TEXT,
    tax_year        INTEGER,
    tax_period_begin TEXT,
    tax_period_end  TEXT,
    org_name        TEXT,
    org_address     TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    mission         TEXT,
    website         TEXT,
    total_revenue   INTEGER,
    total_expenses  INTEGER,
    total_assets_eoy INTEGER,
    total_liabilities_eoy INTEGER,
    net_assets_eoy  INTEGER,
    officers_json   TEXT,                        -- top-paid officers list
    ntee_cd         TEXT,
    parsed_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_details_ein       ON filing_details(ein);
CREATE INDEX IF NOT EXISTS idx_details_tax_year  ON filing_details(tax_year);
-- Composite for YoY self-joins (explosive_revenue_growth etc).
CREATE INDEX IF NOT EXISTS idx_details_ein_year  ON filing_details(ein, tax_year);

-- Tracks bulk-download batches. One row per ZIP.
CREATE TABLE IF NOT EXISTS xml_batches (
    batch_id        TEXT PRIMARY KEY,            -- 2024_TEOS_XML_01A
    year            INTEGER NOT NULL,
    url             TEXT NOT NULL,
    bytes_expected  INTEGER,
    bytes_on_disk   INTEGER,
    local_path      TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|downloading|done|error
    error           TEXT,
    downloaded_at   TEXT
);

-- Audit log of every ingest run.
CREATE TABLE IF NOT EXISTS source_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,               -- bmf, irs_index, irs_xml, parse, risk
    args            TEXT,
    started_at      TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at     TEXT,
    status          TEXT NOT NULL DEFAULT 'running',
    rows_added      INTEGER DEFAULT 0,
    rows_updated    INTEGER DEFAULT 0,
    notes           TEXT
);

-- Risk-signal catalogue. One row per named signal defined in
-- src/i990/risk/signals.py. Weights and thresholds are tunable; the
-- spec lives in docs/risk-signals.md.
CREATE TABLE IF NOT EXISTS risk_signals (
    signal_id       TEXT PRIMARY KEY,            -- slug, e.g. 'shell_org_zero_activity'
    weight          INTEGER NOT NULL,            -- 1..10
    category        TEXT NOT NULL,               -- financial|yoy|governance|footprint|jurisdictional|identity
    description     TEXT NOT NULL,
    logic           TEXT,                        -- human-readable predicate for auditability
    version         INTEGER NOT NULL DEFAULT 1,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Individual hits. One row per (ein, tax_year, signal) that fires.
-- tax_year is nullable for cross-EIN / aggregate signals that don't
-- tie to a single filing year (e.g. shared_address_cluster).
CREATE TABLE IF NOT EXISTS risk_hits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ein             TEXT NOT NULL,
    tax_year        INTEGER,
    signal_id       TEXT NOT NULL,
    severity        REAL NOT NULL DEFAULT 1.0,   -- 0.0=suppressed, 0.5=weak, 1.0=full
    score_contrib   INTEGER NOT NULL,            -- weight * severity, rounded
    evidence_json   TEXT,                        -- {field: value, ...} for the reviewer
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(ein, tax_year, signal_id)
);
CREATE INDEX IF NOT EXISTS idx_hits_ein     ON risk_hits(ein);
CREATE INDEX IF NOT EXISTS idx_hits_signal  ON risk_hits(signal_id);
CREATE INDEX IF NOT EXISTS idx_hits_year    ON risk_hits(tax_year);

-- Rolled-up per-EIN score. Recomputed from risk_hits; latest_tax_year
-- is the most recent filing year we considered.
CREATE TABLE IF NOT EXISTS risk_scores (
    ein             TEXT PRIMARY KEY,
    total_score     INTEGER NOT NULL,
    max_weight_hit  INTEGER NOT NULL DEFAULT 0,  -- highest single-signal weight fired
    n_hits          INTEGER NOT NULL DEFAULT 0,
    tier            INTEGER NOT NULL,            -- 1 critical, 2 elevated, 3 notable, 0 untiered
    latest_tax_year INTEGER,
    signals_csv     TEXT,                        -- comma-joined signal_ids for quick display
    scored_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_scores_tier  ON risk_scores(tier);
CREATE INDEX IF NOT EXISTS idx_scores_total ON risk_scores(total_score DESC);
"""


def connect(path: Path | None = None) -> sqlite3.Connection:
    p = Path(path) if path else DB_PATH
    conn = sqlite3.connect(p)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


@contextmanager
def session(path: Path | None = None) -> Iterator[sqlite3.Connection]:
    conn = connect(path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def record_run_start(conn: sqlite3.Connection, source: str, args: str = "") -> int:
    cur = conn.execute(
        "INSERT INTO source_runs(source, args) VALUES (?, ?)",
        (source, args),
    )
    conn.commit()
    return int(cur.lastrowid)


def record_run_end(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    rows_added: int = 0,
    rows_updated: int = 0,
    notes: str = "",
) -> None:
    conn.execute(
        """
        UPDATE source_runs
           SET finished_at = datetime('now'),
               status = ?,
               rows_added = ?,
               rows_updated = ?,
               notes = ?
         WHERE id = ?
        """,
        (status, rows_added, rows_updated, notes, run_id),
    )
    conn.commit()
