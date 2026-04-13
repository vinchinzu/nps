from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from i990.db import connect
from i990.risk.engine import _rollup


class RiskRollupTest(unittest.TestCase):
    def test_rollup_creates_zero_score_rows_for_eins_with_no_hits(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "test.sqlite"
            conn = connect(db_path)
            conn.execute(
                """
                INSERT INTO filing_details(object_id, ein, tax_year, org_name)
                VALUES
                    ('oid1', '111111111', 2024, 'Org One'),
                    ('oid2', '222222222', 2023, 'Org Two')
                """
            )
            conn.execute(
                """
                INSERT INTO risk_signals(signal_id, weight, category, description, logic)
                VALUES ('sig1', 7, 'financial', 'desc', 'logic')
                """
            )
            conn.execute(
                """
                INSERT INTO risk_hits(ein, tax_year, signal_id, severity, score_contrib, evidence_json)
                VALUES ('111111111', 2024, 'sig1', 1.0, 7, '{}')
                """
            )

            scored = _rollup(conn)
            self.assertEqual(scored, 2)

            rows = {
                row["ein"]: dict(row)
                for row in conn.execute(
                    "SELECT ein, total_score, n_hits, tier, latest_tax_year, signals_csv "
                    "FROM risk_scores ORDER BY ein"
                )
            }
            self.assertEqual(rows["111111111"]["total_score"], 7)
            self.assertEqual(rows["111111111"]["tier"], 3)
            self.assertEqual(rows["111111111"]["signals_csv"], "sig1")

            self.assertEqual(rows["222222222"]["total_score"], 0)
            self.assertEqual(rows["222222222"]["n_hits"], 0)
            self.assertEqual(rows["222222222"]["tier"], 0)
            self.assertEqual(rows["222222222"]["latest_tax_year"], 2023)
            self.assertEqual(rows["222222222"]["signals_csv"], "")

            conn.close()


if __name__ == "__main__":
    unittest.main()
