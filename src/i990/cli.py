"""Unified CLI: `i990 <subcommand>`."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import DATA, DB_PATH, EXPORT_DIR, XML_DIR
from .db import session


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _cmd_fetch_bmf(args: argparse.Namespace) -> int:
    from .sources import bmf
    stats = bmf.run(regions=args.regions or None, force=args.force)
    for region, s in stats.items():
        print(f"bmf {region}: +{s['added']} ~{s['updated']}")
    return 0


def _cmd_fetch_index(args: argparse.Namespace) -> int:
    from .sources import irs_xml
    years = args.years or None
    stats = irs_xml.run_fetch_index(years=years, force=args.force)
    for year, s in stats.items():
        if "error" in s:
            print(f"index {year}: ERROR {s['error']}")
        else:
            print(f"index {year}: +{s['added']} ~{s['updated']}")
    return 0


def _cmd_download_xml(args: argparse.Namespace) -> int:
    from .sources import irs_xml
    result = irs_xml.run_download_xml(years=args.years or None, limit=args.limit)
    print(f"xml: downloaded={result['downloaded']} skipped={result['skipped']} errors={len(result['errors'])}")
    for e in result["errors"][:10]:
        print("  -", e)
    return 0 if not result["errors"] else 2


def _cmd_parse_xml(args: argparse.Namespace) -> int:
    from .parse import xml_header
    result = xml_header.run_parse(
        years=args.years or None,
        limit_zips=args.limit_zips,
        limit_per_zip=args.limit_per_zip,
    )
    print(f"parse: parsed={result['parsed']} failed={result['failed']} zips={result['zips']}")
    return 0


def _cmd_risk_score(args: argparse.Namespace) -> int:
    from .risk import engine
    result = engine.run_scoring(only=args.only or None, clear=not args.no_clear)
    print(f"risk: hits={result['hits']:,} scored_eins={result['scored']:,}")
    t = result["tiers"]
    print(f"  tier 1 (critical): {t[1]:,}")
    print(f"  tier 2 (elevated): {t[2]:,}")
    print(f"  tier 3 (notable):  {t[3]:,}")
    print(f"  untiered:          {t[0]:,}")
    print("\nper-signal hit counts:")
    for sid, n in sorted(result["per_signal"].items(), key=lambda x: -x[1]):
        print(f"  {sid:<34} {n:>8,}")
    return 0


def _cmd_risk_top(args: argparse.Namespace) -> int:
    from .risk import engine
    rows = engine.top_risks(limit=args.limit, tier=args.tier, min_score=args.min_score)
    if not rows:
        print("(no results)")
        return 0
    hdr = f"{'score':>5} {'tier':>4} {'EIN':<10} {'ST':<3} {'subsect':<7} {'name':<50} signals"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        name = (r.get("name") or "(no BMF)")[:50]
        sigs = (r.get("signals_csv") or "")
        if len(sigs) > 80:
            sigs = sigs[:77] + "..."
        print(
            f"{r['total_score']:>5} {r['tier']:>4} {r['ein']:<10} "
            f"{(r.get('state') or ''):<3} {(r.get('subsection') or ''):<7} "
            f"{name:<50} {sigs}"
        )
    return 0


def _cmd_risk_explain(args: argparse.Namespace) -> int:
    import json as _json
    from .risk import engine
    result = engine.explain(args.ein)
    print(_json.dumps(result, indent=2, default=str))
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    with session() as conn:
        org_count = conn.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
        filing_count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
        detail_count = conn.execute("SELECT COUNT(*) FROM filing_details").fetchone()[0]
        batches_total = conn.execute("SELECT COUNT(*) FROM xml_batches").fetchone()[0]
        batches_done = conn.execute(
            "SELECT COUNT(*) FROM xml_batches WHERE status='done'"
        ).fetchone()[0]
        batches_err = conn.execute(
            "SELECT COUNT(*) FROM xml_batches WHERE status='error'"
        ).fetchone()[0]
        by_year = conn.execute(
            "SELECT sub_year, COUNT(*) FROM filings GROUP BY sub_year ORDER BY sub_year"
        ).fetchall()
        by_type = conn.execute(
            "SELECT return_type, COUNT(*) FROM filings GROUP BY return_type ORDER BY 2 DESC"
        ).fetchall()
        runs = conn.execute(
            "SELECT source, status, started_at, finished_at, rows_added, rows_updated, notes "
            "FROM source_runs ORDER BY id DESC LIMIT 10"
        ).fetchall()

    print(f"db: {DB_PATH}")
    print(f"organizations: {org_count:,}")
    print(f"filings:       {filing_count:,}")
    print(f"filing_details:{detail_count:,}")
    print(f"xml_batches:   {batches_done}/{batches_total} done ({batches_err} error)")

    # disk
    try:
        total_xml = sum(p.stat().st_size for p in XML_DIR.rglob("*.zip"))
    except Exception:
        total_xml = 0
    print(f"xml on disk:   {total_xml / (1 << 30):.2f} GB")

    print("\nfilings by year:")
    for y, c in by_year:
        print(f"  {y}: {c:,}")

    print("\nfilings by return type:")
    for t, c in by_type:
        print(f"  {t or '(unknown)'}: {c:,}")

    print("\nrecent runs:")
    for r in runs:
        print(f"  {r['source']:<10} {r['status']:<8} {r['started_at']} -> {r['finished_at'] or '…'} +{r['rows_added']} ~{r['rows_updated']}")
    return 0


def _cmd_export_year(args: argparse.Namespace) -> int:
    from . import export as export_mod

    result = export_mod.export_years(
        years=args.years or None,
        outdir=Path(args.outdir) if args.outdir else EXPORT_DIR,
        limit=args.limit,
        profile="full" if args.full else "lite",
        rows_per_file=args.rows_per_file,
    )
    for year, stats in result["years"].items():
        print(
            f"export {year}: rows={stats['rows']:,} files={stats['files']} "
            f"bytes={stats['bytes']:,}"
        )
        for part in stats["parts"]:
            print(f"  part: rows={part['rows']:,} bytes={part['bytes']:,} path={part['path']}")
    print(f"manifest: {result['manifest']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="i990", description="IRS 990 non-profit database.")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("fetch-bmf", help="Download + ingest IRS BMF CSVs.")
    sp.add_argument("--regions", nargs="*", choices=["eo1", "eo2", "eo3", "eo4"])
    sp.add_argument("--force", action="store_true")
    sp.set_defaults(func=_cmd_fetch_bmf)

    sp = sub.add_parser("fetch-index", help="Download + ingest IRS 990 XML index CSVs.")
    sp.add_argument("--years", nargs="*", type=int)
    sp.add_argument("--force", action="store_true")
    sp.set_defaults(func=_cmd_fetch_index)

    sp = sub.add_parser("download-xml", help="Download every batch ZIP for given years.")
    sp.add_argument("--years", nargs="*", type=int)
    sp.add_argument("--limit", type=int, default=None, help="Max batches (for testing).")
    sp.set_defaults(func=_cmd_download_xml)

    sp = sub.add_parser("parse-xml", help="Parse header fields from downloaded ZIPs.")
    sp.add_argument("--years", nargs="*", type=int)
    sp.add_argument("--limit-zips", type=int, default=None)
    sp.add_argument("--limit-per-zip", type=int, default=None)
    sp.set_defaults(func=_cmd_parse_xml)

    sp = sub.add_parser("risk-score", help="Run the risk-scoring pipeline over filing_details.")
    sp.add_argument("--only", nargs="*", help="Run only these signal ids.")
    sp.add_argument("--no-clear", action="store_true",
                    help="Keep existing risk_hits rather than truncating.")
    sp.set_defaults(func=_cmd_risk_score)

    sp = sub.add_parser("risk-top", help="List highest-scoring EINs.")
    sp.add_argument("--limit", type=int, default=50)
    sp.add_argument("--tier", type=int, choices=[0, 1, 2, 3])
    sp.add_argument("--min-score", type=int)
    sp.set_defaults(func=_cmd_risk_top)

    sp = sub.add_parser("risk-explain", help="Show every hit and evidence for one EIN.")
    sp.add_argument("ein")
    sp.set_defaults(func=_cmd_risk_explain)

    sp = sub.add_parser("status", help="Print DB status and recent runs.")
    sp.set_defaults(func=_cmd_status)

    sp = sub.add_parser(
        "export-year",
        help="Write one denormalized csv.gz per tax year under data/exports/.",
    )
    sp.add_argument("--years", nargs="*", type=int)
    sp.add_argument("--outdir", help="Override output directory.")
    sp.add_argument("--limit", type=int, default=None,
                    help="Optional per-year row cap for testing.")
    sp.add_argument("--full", action="store_true",
                    help="Include wider text fields like mission/website/signals.")
    sp.add_argument("--rows-per-file", type=int, default=200000,
                    help="Split yearly exports into chunks of this many rows.")
    sp.set_defaults(func=_cmd_export_year)

    args = p.parse_args(argv)
    _configure_logging(args.verbose)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    sys.exit(main())
