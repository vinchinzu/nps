# i990 — IRS 990 Non-Profit Database

A local SQLite database of every registered U.S. tax-exempt organization
and every electronically-filed Form 990 the IRS publishes.

Pure-stdlib Python (no pip deps). Resumable, idempotent. Designed to
ingest the "easy 80%" first and scale up to the full XML archive as needed.

The `data/` directory is local working state and is intentionally ignored
for Git. A fresh clone starts small; the SQLite DB, raw CSVs, ZIP archives,
lock files, and logs are all regenerated from the CLI.

## Data sources

| Source | What it covers | Rows | Notes |
|---|---|---|---|
| **IRS BMF** (eo1-eo4 CSVs) | Every registered 501(c) org | ~1.94M | Ground truth for organization metadata, updated monthly. |
| **IRS 990 XML index CSVs** | Every e-filed 990/990-EZ/990-PF/990-T since 2017 | ~5.24M | Maps each filing to the ZIP batch containing its XML. |
| **IRS 990 XML ZIP archives** | Raw 990 XML files | ~5M+ | Bulk downloaded on demand; 61 batches across 2019-2026. |
| **ProPublica Nonprofit Explorer** *(stub)* | Pre-2016 filings, PDF links | — | Not implemented yet; add via `src/i990/sources/propublica.py`. |

Pre-2016 is intentionally out of scope for v1. 2016 index is not hosted
by the IRS on apps.irs.gov; 2017-2018 index CSVs exist but no ZIPs are
hosted (would need the legacy S3 bucket `irs-form-990` or ProPublica).

## Layout

```
i990/
├── bin/i990                  # CLI launcher (no install needed)
├── pyproject.toml            # Optional: `pip install -e .` for `i990` entry
├── src/i990/
│   ├── config.py             # Paths + source URLs (single source of truth)
│   ├── db.py                 # SQLite schema, connection, run audit log
│   ├── http.py               # urllib-based resumable downloader
│   ├── cli.py                # Unified `i990 <subcommand>` entrypoint
│   ├── risk/                 # Heuristic risk scoring over parsed filings
│   ├── sources/
│   │   ├── bmf.py            # BMF fetch + ingest
│   │   └── irs_xml.py        # Index ingest + bulk ZIP downloader
│   └── parse/
│       └── xml_header.py     # XML -> filing_details (header fields)
├── docs/
│   └── risk-signals.md       # Risk model notes and signal catalogue
└── data/
    ├── i990.sqlite           # The database
    ├── raw/bmf/*.csv         # Cached BMF regional CSVs
    ├── raw/index/*.csv       # Cached per-year index CSVs
    ├── xml/{year}/*.zip      # Downloaded batch archives (opt-in)
    └── logs/                 # Background-run logs
```

## Usage

```bash
# 1. Populate organizations table (fast, ~2 min, ~200 MB download)
./bin/i990 fetch-bmf

# 2. Populate filings table from yearly index CSVs (fast, ~4 min, ~400 MB)
./bin/i990 fetch-index

# 3. (Optional) Download raw XML archives for bulk parsing
./bin/i990 download-xml --years 2024 2025 2026       # recent years
./bin/i990 download-xml                              # all 61 batches

# 4. (Optional) Parse downloaded XMLs into filing_details
./bin/i990 parse-xml --years 2024 2025 2026

# 5. (Optional) Score higher-risk filings
./bin/i990 risk-score
./bin/i990 risk-top --tier 1 --limit 25
./bin/i990 risk-explain 043405570

# 6. (Optional) Export plain files by tax year
./bin/i990 export-year --years 2024 2025 2026
./bin/i990 export-year --years 2024 --full

# Anytime: see progress
./bin/i990 status
```

Every step is **resumable**: rerunning downloads skips files that are
already on disk, and upserts in the DB are idempotent.

## Scopes

The user asked for three scopes with a clean path to expand:

1. **Metadata + BMF** (this is the default, done after steps 1-2 above):
   1.94M orgs + 5.24M filings indexed, ~2 GB SQLite, no raw XML.
2. **Metadata + parsed XML headers** (add step 3 + 4):
   Adds ~5-20 GB of ZIPs and populates `filing_details` with revenue,
   assets, mission, officers, etc.
3. **Full XML archive + parsed**: Same as scope 2; to go "full text"
   across every schedule, extend `parse/xml_header.py` to walk and
   index every element of interest.

## Schema highlights

- `organizations(ein PK, name, state, subsection, ntee_cd, ..., bmf_region)`
- `filings(object_id PK, ein, return_type, tax_period, xml_batch_id, on_disk, parsed)`
- `filing_details(object_id PK -> filings, total_revenue, total_expenses, total_assets_eoy, mission, officers_json, ...)`
- `xml_batches(batch_id PK, year, status, bytes_on_disk, local_path)`
- `source_runs(id, source, started_at, finished_at, rows_added, ...)` — audit log

Useful indexes on `state`, `ntee_cd`, `ein`, `return_type`, `sub_year`.

## Example queries

```sql
-- Top 10 biggest private foundations by revenue in the latest filing year
SELECT o.name, o.state, d.tax_year, d.total_revenue
FROM filing_details d
JOIN organizations o USING (ein)
WHERE d.return_type = '990PF'
ORDER BY d.total_revenue DESC NULLS LAST
LIMIT 10;

-- Every filing for one EIN
SELECT object_id, return_type, tax_period, xml_batch_id, on_disk, parsed
FROM filings WHERE ein = '043405570' ORDER BY tax_period DESC;

-- Breakdown of orgs by state
SELECT state, COUNT(*) FROM organizations GROUP BY state ORDER BY 2 DESC;
```

## Extending

- **Pre-2016 coverage**: add a `sources/propublica.py` module that walks
  the Nonprofit Explorer API and upserts into `filings` + `filing_details`.
- **Full schedule parsing**: extend `parse/xml_header.py` to extract
  Schedule A (public support), Schedule B (contributors — note that this
  is redacted in the public XML), Schedule I (grants), etc.
- **Incremental updates**: `fetch-bmf` and `fetch-index` can safely run on
  a cron (`source_runs` records every invocation). Add `--force` to
  re-download source files.
- **New sources**: drop a new module under `src/i990/sources/`, give it a
  `run()` function, and wire it into `cli.py`.

## GitHub Push Notes

- No non-data source file in this tree is over 50 MB.
- `data/` artifacts are local-only and ignored by `.gitignore`.
- Generated Python caches are also ignored and safe to delete.
- `export-year` writes denormalized `csv.gz` files under `data/exports/`
  so the data can be used without SQLite or SQL. The default export is a
  lean profile and splits each year into multiple parts; `--full` adds
  longer text fields.
