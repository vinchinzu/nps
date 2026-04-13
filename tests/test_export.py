from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from i990.db import connect
from i990.export import export_years


class ExportYearsTest(unittest.TestCase):
    def test_export_years_writes_csv_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "test.sqlite"
            outdir = root / "exports"

            conn = connect(db_path)
            conn.execute(
                """
                INSERT INTO organizations(ein, name, state, subsection, ntee_cd, bmf_region)
                VALUES ('123456789', 'Example Org', 'TX', '03', 'B12', 'eo1')
                """
            )
            conn.execute(
                """
                INSERT INTO filings(
                    object_id, ein, return_type, sub_year, xml_batch_id
                ) VALUES ('oid1', '123456789', '990', 2025, '2025_TEOS_XML_01A')
                """
            )
            conn.execute(
                """
                INSERT INTO filing_details(
                    object_id, ein, return_type, tax_year, org_name, state,
                    total_revenue, total_expenses, total_assets_eoy
                ) VALUES ('oid1', '123456789', '990', 2024, 'Example Org', 'TX', 100, 80, 250)
                """
            )
            conn.execute(
                """
                INSERT INTO risk_scores(
                    ein, total_score, max_weight_hit, n_hits, tier, latest_tax_year, signals_csv
                ) VALUES ('123456789', 9, 9, 1, 1, 2024, 'bmf_unmapped_filer')
                """
            )
            conn.commit()
            conn.close()

            result = export_years(years=[2024], outdir=outdir, db_path=db_path, profile="full")

            export_path = outdir / "filings_2024_full_part01.csv.gz"
            self.assertTrue(export_path.exists())
            self.assertEqual(result["years"][2024]["rows"], 1)
            self.assertEqual(result["years"][2024]["files"], 1)

            with gzip.open(export_path, "rt", encoding="utf-8") as f:
                body = f.read()
            self.assertIn("Example Org", body)
            self.assertIn("bmf_unmapped_filer", body)

            manifest = json.loads((outdir / "manifest.json").read_text())
            self.assertEqual(manifest["format"], "csv.gz")
            self.assertEqual(manifest["profile"], "full")
            self.assertIn("2024", manifest["years"])

    def test_export_coalesces_missing_risk_scores_to_zero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "test.sqlite"
            outdir = root / "exports"

            conn = connect(db_path)
            conn.execute(
                """
                INSERT INTO filing_details(
                    object_id, ein, return_type, tax_year, org_name, state,
                    total_revenue, total_expenses, total_assets_eoy
                ) VALUES ('oid2', '987654321', '990', 2024, 'No Risk Org', 'WA', 100, 80, 250)
                """
            )
            conn.commit()
            conn.close()

            export_years(years=[2024], outdir=outdir, db_path=db_path)

            export_path = outdir / "filings_2024_part01.csv.gz"
            with gzip.open(export_path, "rt", encoding="utf-8") as f:
                header = f.readline().strip().split(",")
                row = f.readline().strip().split(",")

            values = dict(zip(header, row))
            self.assertEqual(values["risk_total_score"], "0")
            self.assertEqual(values["risk_tier"], "0")


if __name__ == "__main__":
    unittest.main()
