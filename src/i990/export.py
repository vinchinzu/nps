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
    # contact / identity
    "org_address",
    "city",
    "zip",
    "phone",
    "website",
    "mission",
    "principal_officer",
    "legal_domicile_state",
    "formation_yr",
    # financial summary
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
    # governance / workforce
    "voting_members_cnt",
    "independent_members_cnt",
    "total_employees",
    "total_volunteers",
    "total_reportable_comp",
    "indiv_rcvd_greater_100k_cnt",
    # self-reported boolean flags (JSON dict)
    "flags_json",
    # risk
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
            COALESCE(rs.total_score, 0)                  AS risk_total_score,
            COALESCE(rs.tier, 0)                         AS risk_tier,
            -- full-profile fields
            d.org_address                                AS org_address,
            d.city                                       AS city,
            d.zip                                        AS zip,
            d.phone                                      AS phone,
            d.website                                    AS website,
            d.mission                                    AS mission,
            d.principal_officer                          AS principal_officer,
            d.legal_domicile_state                       AS legal_domicile_state,
            d.formation_yr                               AS formation_yr,
            d.gross_receipts                             AS gross_receipts,
            d.py_total_revenue                           AS py_total_revenue,
            d.cy_contributions                           AS cy_contributions,
            d.cy_program_service_revenue                 AS cy_program_service_revenue,
            d.cy_investment_income                       AS cy_investment_income,
            d.cy_salaries                                AS cy_salaries,
            d.cy_grants_paid                             AS cy_grants_paid,
            d.cy_fundraising_expense                     AS cy_fundraising_expense,
            d.total_assets_boy                           AS total_assets_boy,
            d.total_liabilities_boy                      AS total_liabilities_boy,
            d.net_assets_boy                             AS net_assets_boy,
            d.total_gross_ubi                            AS total_gross_ubi,
            d.voting_members_cnt                         AS voting_members_cnt,
            d.independent_members_cnt                    AS independent_members_cnt,
            d.total_employees                            AS total_employees,
            d.total_volunteers                           AS total_volunteers,
            d.total_reportable_comp                      AS total_reportable_comp,
            d.indiv_rcvd_greater_100k_cnt                AS indiv_rcvd_greater_100k_cnt,
            d.flags_json                                 AS flags_json,
            COALESCE(rs.signals_csv, '')                 AS risk_signals
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


PERSONS_COLUMNS = [
    "object_id",
    "ein",
    "tax_year",
    "person_role",
    "name",
    "name_norm",
    "title",
    "reportable_comp",
    "other_comp",
    "related_org_comp",
    "hours_per_week",
    "hours_related",
    "is_officer",
    "is_director",
    "is_key_employee",
    "is_hce",
    "is_former",
    "services_desc",
    # denormalised for convenience
    "org_name",
    "state",
    "ntee_cd",
    "subsection",
    "total_revenue",
]

_PERSONS_QUERY = """
    SELECT
        p.object_id, p.ein, p.tax_year, p.person_role,
        p.name, p.name_norm, p.title,
        p.reportable_comp, p.other_comp, p.related_org_comp,
        p.hours_per_week, p.hours_related,
        p.is_officer, p.is_director, p.is_key_employee,
        p.is_hce, p.is_former, p.services_desc,
        COALESCE(o.name, d.org_name) AS org_name,
        COALESCE(o.state, d.state)   AS state,
        o.ntee_cd                    AS ntee_cd,
        o.subsection                 AS subsection,
        d.total_revenue              AS total_revenue
      FROM filing_persons p
      LEFT JOIN filing_details d USING (object_id)
      LEFT JOIN organizations o ON o.ein = p.ein
     ORDER BY p.ein, p.tax_year, p.person_role, p.name_norm
"""


def export_persons(
    outdir: Path | None = None,
    db_path: Path | None = None,
    rows_per_file: int = 200000,
) -> dict:
    """Export filing_persons to chunked csv.gz files for network analysis."""
    outdir = Path(outdir or EXPORT_DIR)
    outdir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    total_bytes = 0
    parts: list[dict] = []
    part_no = 0
    f = None
    writer = None
    path: Path | None = None

    def close_part() -> None:
        nonlocal f, writer, total_bytes, path
        if f is None or path is None:
            return
        f.close()
        sz = path.stat().st_size
        total_bytes += sz
        parts.append({"rows": part_rows, "bytes": sz, "path": str(path)})
        f = None
        writer = None
        path = None

    def open_part() -> None:
        nonlocal f, writer, path, part_no
        part_no += 1
        path = outdir / f"persons_part{part_no:02d}.csv.gz"
        f = gzip.open(path, "wt", encoding="utf-8", newline="")
        writer = csv.DictWriter(f, fieldnames=PERSONS_COLUMNS)
        writer.writeheader()

    part_rows = 0
    with session(db_path) as conn:
        for row in conn.execute(_PERSONS_QUERY):
            if writer is None:
                open_part()
            elif part_rows >= rows_per_file:
                close_part()
                part_rows = 0
                open_part()
            assert writer is not None
            writer.writerow({col: row[col] for col in PERSONS_COLUMNS})
            total_rows += 1
            part_rows += 1

    close_part()

    manifest_path = outdir / "persons_manifest.json"
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "format": "csv.gz",
        "rows_per_file": rows_per_file,
        "columns": PERSONS_COLUMNS,
        "total_rows": total_rows,
        "parts": parts,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"total_rows": total_rows, "total_bytes": total_bytes,
            "files": len(parts), "manifest": str(manifest_path)}
