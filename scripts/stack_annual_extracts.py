#!/usr/bin/env python3
"""Normalize IRS/NBER annual Form 990 extracts into i990 export-shaped CSVs.

The annual extract data is not the same source as the XML parser output:
it has no IRS object id and lacks long text fields.  This script keeps the
export schema compatible with data/exports/filings_*_full_part*.csv.gz by
mapping only equivalent fields and leaving unavailable fields empty.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_SOURCE_DIR = Path("data/external/irs_soi_annual_extracts/form990")
DEFAULT_OUT_DIR = Path("data/exports/annual_extracts")
DEFAULT_DB_PATH = Path("data/i990.sqlite")
MAX_PART_BYTES = 49_000_000

EXPORT_COLUMNS = [
    "tax_year",
    "object_id",
    "ein",
    "org_name",
    "state",
    "subsection",
    "ntee_cd",
    "bmf_region",
    "return_type",
    "filing_sub_year",
    "xml_batch_id",
    "total_revenue",
    "total_expenses",
    "total_assets_eoy",
    "total_liabilities_eoy",
    "net_assets_eoy",
    "risk_total_score",
    "risk_tier",
    "org_address",
    "city",
    "zip",
    "phone",
    "website",
    "mission",
    "principal_officer",
    "legal_domicile_state",
    "formation_yr",
    "gross_receipts",
    "py_total_revenue",
    "cy_contributions",
    "cy_program_service_revenue",
    "cy_investment_income",
    "cy_salaries",
    "cy_grants_paid",
    "cy_fundraising_expense",
    "total_assets_boy",
    "total_liabilities_boy",
    "net_assets_boy",
    "total_gross_ubi",
    "voting_members_cnt",
    "independent_members_cnt",
    "total_employees",
    "total_volunteers",
    "total_reportable_comp",
    "indiv_rcvd_greater_100k_cnt",
    "flags_json",
    "risk_signals",
]

FIELD_MAP = {
    "total_revenue": "totrevenue",
    "total_expenses": "totfuncexpns",
    "total_assets_eoy": "totassetsend",
    "total_liabilities_eoy": "totliabend",
    "net_assets_eoy": "totnetassetend",
    "cy_contributions": "totcntrbgfts",
    "cy_program_service_revenue": "totprgmrevnue",
    "cy_investment_income": "invstmntinc",
    "cy_salaries": "othrsalwages",
    "cy_grants_paid": "grntstogovt",
    "cy_fundraising_expense": "profndraising",
    "total_reportable_comp": "compnsatncurrofcr",
    "indiv_rcvd_greater_100k_cnt": "noemplyeesw3cnt",
}


def pad_ein(value: str | None) -> str:
    value = (value or "").strip()
    return value.zfill(9) if value else ""


def int_text(value: str | None) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    try:
        return str(int(float(value)))
    except ValueError:
        return value


def parse_year(value: str | None) -> int | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def row_tax_year(row: dict[str, str]) -> int | None:
    tax_year = parse_year(row.get("tax_year"))
    if tax_year is not None:
        return tax_year
    tax_period = (row.get("tax_pd") or "").strip()
    if len(tax_period) >= 4 and tax_period[:4].isdigit():
        return int(tax_period[:4])
    return None


def source_year_for(path: Path) -> int:
    name = path.name
    digits = "".join(ch for ch in name if ch.isdigit())
    for i in range(0, max(0, len(digits) - 3)):
        year = int(digits[i:i + 4])
        if 1900 < year < 2100:
            return year
    if name[:2].isdigit():
        return 2000 + int(name[:2])
    raise ValueError(f"cannot infer source year from {path}")


def load_orgs(db_path: Path) -> dict[str, sqlite3.Row]:
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ein, name, street, city, state, zip, subsection, ntee_cd, bmf_region
              FROM organizations
            """
        )
        return {row["ein"]: row for row in rows}
    finally:
        conn.close()


class SplitGzipWriter:
    def __init__(self, out_dir: Path, prefix: str, max_part_bytes: int) -> None:
        self.out_dir = out_dir
        self.prefix = prefix
        self.max_part_bytes = max_part_bytes
        self.part_no = 0
        self.part_rows = 0
        self.total_rows = 0
        self.parts: list[dict] = []
        self.raw = None
        self.gz = None
        self.text = None
        self.writer = None

    def open_part(self) -> None:
        self.part_no += 1
        path = self.out_dir / f"{self.prefix}_part{self.part_no:02d}.csv.gz"
        self.raw = path.open("wb")
        self.gz = gzip.GzipFile(fileobj=self.raw, mode="wb", mtime=0)
        self.text = io.TextIOWrapper(self.gz, encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.text, fieldnames=EXPORT_COLUMNS)
        self.writer.writeheader()
        self.part_rows = 0

    def close_part(self) -> None:
        if self.writer is None:
            return
        assert self.text is not None and self.raw is not None
        path = Path(self.raw.name)
        self.text.close()
        size = path.stat().st_size
        self.parts.append({"file": path.name, "rows": self.part_rows, "bytes": size})
        self.raw = self.gz = self.text = self.writer = None
        self.part_rows = 0

    def write(self, row: dict) -> None:
        if self.writer is None:
            self.open_part()
        assert self.writer is not None and self.raw is not None
        self.writer.writerow(row)
        self.part_rows += 1
        self.total_rows += 1
        if self.part_rows % 1000 == 0:
            self.text.flush()
            if self.raw.tell() >= self.max_part_bytes:
                self.close_part()

    def close(self) -> None:
        if self.writer is not None:
            self.close_part()


def normalize_row(
    row: dict[str, str],
    source_file: Path,
    source_year: int,
    row_number: int,
    orgs: dict[str, sqlite3.Row],
) -> dict:
    ein = pad_ein(row.get("ein"))
    tax_year = row_tax_year(row)
    org = orgs.get(ein)
    out = {col: "" for col in EXPORT_COLUMNS}
    out.update({
        "tax_year": tax_year or "",
        "object_id": f"annual_extract_990_{source_year}_{row_number:08d}",
        "ein": ein,
        "return_type": "990",
        "filing_sub_year": source_year,
        "xml_batch_id": "",
        "risk_total_score": "0",
        "risk_tier": "0",
    })
    if org is not None:
        out.update({
            "org_name": org["name"] or "",
            "state": org["state"] or "",
            "subsection": org["subsection"] or "",
            "ntee_cd": org["ntee_cd"] or "",
            "bmf_region": org["bmf_region"] or "",
            "org_address": org["street"] or "",
            "city": org["city"] or "",
            "zip": org["zip"] or "",
        })
    for export_col, source_col in FIELD_MAP.items():
        if source_col in row:
            out[export_col] = int_text(row.get(source_col))
    return out


def iter_source_files(source_dir: Path) -> list[Path]:
    files = sorted(source_dir.glob("f99020*.csv"))
    files += sorted(source_dir.glob("*eoextract990.csv"))
    seen: set[Path] = set()
    out: list[Path] = []
    for path in files:
        if path not in seen:
            seen.add(path)
            out.append(path)
    return out


def run(args: argparse.Namespace) -> dict:
    source_dir = Path(args.source_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for old_part in out_dir.glob(f"{args.prefix}_part*.csv.gz"):
        old_part.unlink()
    old_manifest = out_dir / f"{args.prefix}_manifest.json"
    if old_manifest.exists():
        old_manifest.unlink()
    orgs = load_orgs(Path(args.db))
    writer = SplitGzipWriter(out_dir, args.prefix, args.max_part_bytes)
    source_summaries = []

    try:
        for path in iter_source_files(source_dir):
            source_year = source_year_for(path)
            if args.source_years and source_year not in args.source_years:
                continue
            rows_in = 0
            rows_out = 0
            with path.open(newline="", encoding="utf-8", errors="replace") as fh:
                reader = csv.DictReader(fh)
                for rows_in, row in enumerate(reader, start=1):
                    tax_year = row_tax_year(row)
                    if args.min_tax_year is not None and (tax_year is None or tax_year < args.min_tax_year):
                        continue
                    if args.max_tax_year is not None and (tax_year is None or tax_year > args.max_tax_year):
                        continue
                    writer.write(normalize_row(row, path, source_year, rows_in, orgs))
                    rows_out += 1
            source_summaries.append({
                "source_file": path.name,
                "source_year": source_year,
                "rows_read": rows_in,
                "rows_written": rows_out,
            })
    finally:
        writer.close()

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "format": "csv.gz",
        "profile": "annual_extract_full_compatible",
        "columns": EXPORT_COLUMNS,
        "max_part_bytes": args.max_part_bytes,
        "rows": writer.total_rows,
        "files": len(writer.parts),
        "parts": writer.parts,
        "filters": {
            "min_tax_year": args.min_tax_year,
            "max_tax_year": args.max_tax_year,
            "source_years": args.source_years,
        },
        "sources": source_summaries,
        "notes": [
            "Kaggle's irs/irs-990 page is a BigQuery-backed dataset; the Kaggle file API did not expose downloadable CSV files.",
            "These CSVs come from the IRS annual extract/NBER mirror of the same public Form 990 annual extract data.",
            "object_id is synthetic; annual extracts do not include IRS XML object ids.",
            "org identity columns are enriched from the local BMF organizations table where EINs match.",
            "Unavailable XML-only fields are left empty.",
        ],
    }
    manifest_path = out_dir / f"{args.prefix}_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--prefix", default="annual_extract_990_2011_2018_full")
    parser.add_argument("--min-tax-year", type=int, default=2011)
    parser.add_argument("--max-tax-year", type=int, default=2018)
    parser.add_argument("--source-years", type=int, nargs="*", default=None)
    parser.add_argument("--max-part-bytes", type=int, default=MAX_PART_BYTES)
    args = parser.parse_args()
    manifest = run(args)
    print(
        f"rows={manifest['rows']:,} files={manifest['files']} "
        f"manifest={Path(args.out_dir) / f'{args.prefix}_manifest.json'}"
    )
    for part in manifest["parts"]:
        print(
            f"  {Path(args.out_dir) / part['file']} "
            f"rows={part['rows']:,} bytes={part['bytes']:,}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
