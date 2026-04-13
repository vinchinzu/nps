"""Flat-file exports for users who do not want to query SQLite directly."""
from __future__ import annotations

import csv
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

from .config import EXPORT_DIR
from .db import session

YEAR_EXPORT_COLUMNS_LITE = [
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
]

YEAR_EXPORT_COLUMNS_FULL = YEAR_EXPORT_COLUMNS_LITE + [
    "website",
    "mission",
    "risk_signals",
]


def _years_with_data(conn) -> list[int]:
    return [
        int(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT tax_year
              FROM filing_details
             WHERE tax_year IS NOT NULL
             ORDER BY tax_year
            """
        )
    ]


def _export_query(limit: int | None = None) -> str:
    sql = """
        SELECT
            d.tax_year                                   AS tax_year,
            d.object_id                                  AS object_id,
            d.ein                                        AS ein,
            COALESCE(o.name, d.org_name)                 AS org_name,
            COALESCE(o.state, d.state)                   AS state,
            o.subsection                                 AS subsection,
            o.ntee_cd                                    AS ntee_cd,
            o.bmf_region                                 AS bmf_region,
            d.return_type                                AS return_type,
            f.sub_year                                   AS filing_sub_year,
            f.xml_batch_id                               AS xml_batch_id,
            d.total_revenue                              AS total_revenue,
            d.total_expenses                             AS total_expenses,
            d.total_assets_eoy                           AS total_assets_eoy,
            d.total_liabilities_eoy                      AS total_liabilities_eoy,
            d.net_assets_eoy                             AS net_assets_eoy,
            d.website                                    AS website,
            d.mission                                    AS mission,
            rs.total_score                               AS risk_total_score,
            rs.tier                                      AS risk_tier,
            rs.signals_csv                               AS risk_signals
          FROM filing_details d
          LEFT JOIN filings f USING (object_id)
          LEFT JOIN organizations o USING (ein)
          LEFT JOIN risk_scores rs USING (ein)
         WHERE d.tax_year = ?
         ORDER BY d.ein, d.object_id
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql


def export_years(
    years: list[int] | None = None,
    outdir: Path | None = None,
    limit: int | None = None,
    db_path: Path | None = None,
    profile: str = "lite",
    rows_per_file: int | None = 200000,
) -> dict:
    outdir = Path(outdir or EXPORT_DIR)
    outdir.mkdir(parents=True, exist_ok=True)

    summary: dict[int, dict] = {}
    query = _export_query(limit=limit)
    if profile not in {"lite", "full"}:
        raise ValueError(f"unknown export profile: {profile}")
    columns = YEAR_EXPORT_COLUMNS_FULL if profile == "full" else YEAR_EXPORT_COLUMNS_LITE
    if rows_per_file is not None and rows_per_file <= 0:
        rows_per_file = None

    with session(db_path) as conn:
        years = years or _years_with_data(conn)
        for year in years:
            suffix = "_full" if profile == "full" else ""
            rows = 0
            total_bytes = 0
            part_rows = 0
            part_no = 0
            parts: list[dict] = []
            f = None
            writer = None
            path = None

            def close_part() -> None:
                nonlocal f, writer, part_rows, total_bytes, path
                if f is None or path is None:
                    return
                f.close()
                part_bytes = path.stat().st_size
                total_bytes += part_bytes
                parts.append({
                    "rows": part_rows,
                    "bytes": part_bytes,
                    "path": str(path),
                })
                f = None
                writer = None
                path = None
                part_rows = 0

            def open_part() -> None:
                nonlocal f, writer, path, part_no
                part_no += 1
                path = outdir / f"filings_{year}{suffix}_part{part_no:02d}.csv.gz"
                f = gzip.open(path, "wt", encoding="utf-8", newline="")
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()

            for row in conn.execute(query, (year,)):
                if writer is None:
                    open_part()
                elif rows_per_file is not None and part_rows >= rows_per_file:
                    close_part()
                    open_part()
                assert writer is not None
                writer.writerow({col: row[col] for col in columns})
                rows += 1
                part_rows += 1

            if writer is None:
                open_part()
            close_part()

            summary[int(year)] = {
                "rows": rows,
                "bytes": total_bytes,
                "files": len(parts),
                "parts": parts,
            }

    manifest_path = outdir / "manifest.json"
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "format": "csv.gz",
        "profile": profile,
        "rows_per_file": rows_per_file,
        "columns": columns,
        "years": summary,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"years": summary, "manifest": str(manifest_path)}
