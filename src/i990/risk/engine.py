"""Risk scoring engine.

Runs every Signal from signals.SIGNALS against the SQLite DB, writes
one row per hit into `risk_hits`, and rolls up totals per EIN into
`risk_scores`. Idempotent: rerunning clears the old output and
recomputes from scratch. That's cheaper than trying to track which
filings changed since the last run, and keeps the scoring
deterministic.

Tiering:
    1 (critical): total_score >= 25 OR any single weight-9 signal fired
    2 (elevated): 15 <= total_score < 25
    3 (notable):  6 <= total_score < 15
    0 (untiered): total_score < 6
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Iterable

from ..db import record_run_end, record_run_start, session
from .signals import SIGNALS, Signal

log = logging.getLogger(__name__)

TIER1_SCORE = 25
TIER2_SCORE = 15
TIER3_SCORE = 6


def _build_related_entity_allowlist(conn: sqlite3.Connection) -> dict[str, int]:
    """Identify addresses hosting related-entity corporate groups.

    A "related entity group" is a street+zip where 2+ EINs share the
    first 6 characters of their org name (e.g. BRONSON * LLC at one HQ,
    VOLUNTEERS OF AMERICA * at one HQ, MERCY HEALTH *). These addresses
    produce false positives in shared_address_cluster and
    officer_name_collision because the underlying pattern is legitimate
    corporate structure, not nominee laundering.

    Materialized into a small helper table that the signals can NOT IN
    against. Rebuilt every risk-score run from the current BMF.
    """
    conn.execute("DROP TABLE IF EXISTS risk_helper_related_addrs")
    conn.execute("DROP TABLE IF EXISTS risk_helper_related_eins")
    # Group first (cheap), then find either:
    #   1. addresses where a single name-prefix appears 2+ times, or
    #   2. institutional campuses where 3+ B/E/L entities share a
    #      street+zip even if the legal names differ.
    # This scales linearly in #orgs, unlike a self-join on 1.9M rows.
    conn.execute(
        """
        CREATE TABLE risk_helper_related_addrs AS
        WITH prefix_groups AS (
            SELECT UPPER(TRIM(street)) || '|' || TRIM(zip) AS key
              FROM organizations
             WHERE street IS NOT NULL AND LENGTH(street) > 4
               AND zip IS NOT NULL
               AND LENGTH(name) >= 6
             GROUP BY UPPER(TRIM(street)) || '|' || TRIM(zip), SUBSTR(UPPER(name),1,6)
            HAVING COUNT(*) >= 2
        ),
        institutional_campuses AS (
            SELECT UPPER(TRIM(street)) || '|' || TRIM(zip) AS key
              FROM organizations
             WHERE street IS NOT NULL AND LENGTH(street) > 4
               AND zip IS NOT NULL
               AND SUBSTR(COALESCE(ntee_cd,''),1,1) IN ('B', 'E', 'L')
             GROUP BY UPPER(TRIM(street)) || '|' || TRIM(zip), SUBSTR(COALESCE(ntee_cd,''),1,1)
            HAVING COUNT(DISTINCT ein) >= 3
        )
        SELECT DISTINCT key
          FROM (
            SELECT key FROM prefix_groups
            UNION
            SELECT key FROM institutional_campuses
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX idx_related_addrs_key ON risk_helper_related_addrs(key)")

    # Related systems sometimes operate adjacent legal entities from
    # different campus addresses. Allowlist those EINs for the
    # officer-name collision signal when they look like a local
    # institutional family (same city/state, same sector, same stem).
    conn.execute(
        """
        CREATE TABLE risk_helper_related_eins AS
        WITH base AS (
            SELECT ein,
                   UPPER(TRIM(COALESCE(city, ''))) AS city,
                   TRIM(COALESCE(state, '')) AS state,
                   COALESCE(SUBSTR(ntee_cd, 1, 1), '') AS sector,
                   CASE
                       WHEN UPPER(TRIM(name)) LIKE 'THE %' THEN SUBSTR(UPPER(TRIM(name)), 5)
                       ELSE UPPER(TRIM(name))
                   END AS stem
              FROM organizations
             WHERE LENGTH(COALESCE(name, '')) >= 5
               AND LENGTH(COALESCE(city, '')) >= 2
               AND LENGTH(COALESCE(state, '')) = 2
        ),
        tokens AS (
            SELECT ein,
                   city,
                   state,
                   sector,
                   stem,
                   CASE
                       WHEN INSTR(stem, ' ') > 0 THEN SUBSTR(stem, 1, INSTR(stem, ' ') - 1)
                       ELSE stem
                   END AS token1
              FROM base
        ),
        families AS (
            SELECT 'token' AS kind, city, state, sector, token1 AS family_key
              FROM tokens
             WHERE sector IN ('B', 'E', 'L')
               AND LENGTH(token1) >= 5
               AND token1 NOT IN ('FOUNDATION', 'COMMUNITY', 'CHILDRENS')
             GROUP BY city, state, sector, token1
            HAVING COUNT(*) >= 3
            UNION
            SELECT 'stem' AS kind, city, state, sector, SUBSTR(stem, 1, 12) AS family_key
              FROM tokens
             WHERE sector IN ('B', 'E', 'L')
               AND LENGTH(stem) >= 8
             GROUP BY city, state, sector, SUBSTR(stem, 1, 12)
            HAVING COUNT(*) >= 3
        )
        SELECT DISTINCT t.ein
          FROM tokens t
          JOIN families f
            ON f.city = t.city
           AND f.state = t.state
           AND f.sector = t.sector
           AND (
               (f.kind = 'token' AND t.token1 = f.family_key)
            OR (f.kind = 'stem'  AND SUBSTR(t.stem, 1, 12) = f.family_key)
           )
        """
    )
    conn.execute("CREATE UNIQUE INDEX idx_related_eins_ein ON risk_helper_related_eins(ein)")

    addrs = conn.execute("SELECT COUNT(*) FROM risk_helper_related_addrs").fetchone()[0]
    eins = conn.execute("SELECT COUNT(*) FROM risk_helper_related_eins").fetchone()[0]
    log.info("related-entity allowlist: %d addresses, %d EINs", addrs, eins)
    conn.commit()
    return {"addresses": int(addrs), "eins": int(eins)}


def _upsert_signal_catalogue(conn: sqlite3.Connection) -> None:
    """Keep risk_signals in sync with the Python SIGNALS list."""
    conn.executemany(
        """
        INSERT INTO risk_signals(signal_id, weight, category, description, logic, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(signal_id) DO UPDATE SET
            weight      = excluded.weight,
            category    = excluded.category,
            description = excluded.description,
            logic       = excluded.logic,
            updated_at  = datetime('now')
        """,
        [(s.id, s.weight, s.category, s.description, s.sql.strip()) for s in SIGNALS],
    )


def _run_signal(conn: sqlite3.Connection, sig: Signal) -> int:
    """Execute one signal's SQL and insert results into risk_hits. Returns hit count."""
    t0 = time.time()
    try:
        cur = conn.execute(sig.sql)
    except sqlite3.Error as e:
        log.error("signal %s SQL error: %s", sig.id, e)
        return 0

    rows: list[tuple] = []
    for ein, tax_year, severity, evidence in cur:
        if not ein:
            continue
        sev = float(severity) if severity is not None else 1.0
        if sev <= 0:
            continue
        contrib = int(round(sig.weight * sev))
        rows.append((ein, tax_year, sig.id, sev, contrib, evidence))

    if rows:
        # INSERT OR IGNORE in case of intra-run duplicates (same ein/year/signal
        # returned twice by a poorly-bounded query).
        conn.executemany(
            """
            INSERT OR IGNORE INTO risk_hits
                (ein, tax_year, signal_id, severity, score_contrib, evidence_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    conn.commit()
    dur = time.time() - t0
    log.info("signal %-32s weight=%d hits=%7d (%.1fs)", sig.id, sig.weight, len(rows), dur)
    return len(rows)


def _rollup(conn: sqlite3.Connection) -> int:
    """Aggregate risk_hits into risk_scores. Returns number of scored EINs."""
    log.info("rolling up risk_scores...")
    conn.execute("DELETE FROM risk_scores")
    # SQLite supports group_concat. We compute the tier after insertion
    # so it stays in SQL.
    conn.execute(
        """
        INSERT INTO risk_scores(
            ein, total_score, max_weight_hit, n_hits, tier,
            latest_tax_year, signals_csv, scored_at
        )
        SELECT
            h.ein,
            SUM(h.score_contrib)                                   AS total_score,
            COALESCE(MAX(CASE WHEN h.severity >= 1.0 THEN s.weight END), 0) AS max_weight_hit,
            COUNT(*)                                               AS n_hits,
            0                                                      AS tier,
            MAX(h.tax_year)                                        AS latest_tax_year,
            (SELECT GROUP_CONCAT(DISTINCT signal_id)
               FROM risk_hits h2 WHERE h2.ein = h.ein)             AS signals_csv,
            datetime('now')
          FROM risk_hits h
          JOIN risk_signals s USING (signal_id)
         GROUP BY h.ein
        """
    )
    # Assign tiers.
    conn.execute(
        f"""
        UPDATE risk_scores
           SET tier = CASE
               WHEN total_score >= {TIER1_SCORE} OR max_weight_hit >= 9 THEN 1
               WHEN total_score >= {TIER2_SCORE}                        THEN 2
               WHEN total_score >= {TIER3_SCORE}                        THEN 3
               ELSE 0
           END
        """
    )
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM risk_scores").fetchone()[0]
    log.info("risk_scores: %d EINs scored", n)
    return int(n)


def run_scoring(
    only: Iterable[str] | None = None,
    clear: bool = True,
) -> dict:
    """Run the full risk pipeline.

    Args:
        only:  if set, run only signals whose id is in this set
        clear: if True (default) truncate risk_hits before running
    """
    only_set = set(only) if only else None
    signals = [s for s in SIGNALS if not only_set or s.id in only_set]

    stats: dict[str, int] = {}
    with session() as conn:
        # Relax durability during the batch write; we can always rerun.
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA cache_size = -262144")

        run_id = record_run_start(
            conn, "risk",
            f"signals={[s.id for s in signals]} clear={clear}",
        )

        _build_related_entity_allowlist(conn)
        _upsert_signal_catalogue(conn)

        if clear:
            if only_set:
                conn.execute(
                    "DELETE FROM risk_hits WHERE signal_id IN ("
                    + ",".join("?" * len(only_set)) + ")",
                    list(only_set),
                )
            else:
                conn.execute("DELETE FROM risk_hits")
            conn.commit()

        total = 0
        for sig in signals:
            n = _run_signal(conn, sig)
            stats[sig.id] = n
            total += n

        scored = _rollup(conn)

        tier_counts = {
            t: conn.execute(
                "SELECT COUNT(*) FROM risk_scores WHERE tier=?", (t,)
            ).fetchone()[0]
            for t in (1, 2, 3, 0)
        }

        notes = (
            f"hits={total} scored_eins={scored} "
            f"tier1={tier_counts[1]} tier2={tier_counts[2]} "
            f"tier3={tier_counts[3]} untiered={tier_counts[0]}"
        )
        record_run_end(conn, run_id, "ok", rows_added=total, notes=notes)

    return {
        "hits": total,
        "scored": scored,
        "per_signal": stats,
        "tiers": tier_counts,
    }


def top_risks(
    limit: int = 50,
    tier: int | None = None,
    min_score: int | None = None,
) -> list[dict]:
    """Return the highest-scoring EINs for display."""
    sql = """
        SELECT rs.ein, rs.total_score, rs.tier, rs.max_weight_hit, rs.n_hits,
               rs.latest_tax_year, rs.signals_csv,
               o.name, o.state, o.ntee_cd, o.subsection, o.bmf_region
          FROM risk_scores rs
          LEFT JOIN organizations o USING (ein)
         WHERE 1=1
    """
    params: list = []
    if tier is not None:
        sql += " AND rs.tier = ?"
        params.append(tier)
    if min_score is not None:
        sql += " AND rs.total_score >= ?"
        params.append(min_score)
    sql += " ORDER BY rs.total_score DESC, rs.max_weight_hit DESC LIMIT ?"
    params.append(limit)

    with session() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [dict(r) for r in rows]


def explain(ein: str) -> dict:
    """Return the score and every hit for one EIN, for human review."""
    with session() as conn:
        score = conn.execute(
            "SELECT * FROM risk_scores WHERE ein=?", (ein,)
        ).fetchone()
        org = conn.execute(
            "SELECT name, street, city, state, zip, ntee_cd, subsection, ruling, status, bmf_region "
            "FROM organizations WHERE ein=?", (ein,)
        ).fetchone()
        hits = conn.execute(
            """
            SELECT h.signal_id, h.tax_year, h.severity, h.score_contrib,
                   h.evidence_json, s.weight, s.category, s.description
              FROM risk_hits h
              LEFT JOIN risk_signals s USING (signal_id)
             WHERE h.ein = ?
             ORDER BY h.score_contrib DESC, h.tax_year DESC
            """,
            (ein,),
        ).fetchall()

    return {
        "ein": ein,
        "score": dict(score) if score else None,
        "org": dict(org) if org else None,
        "hits": [
            {**dict(h), "evidence": json.loads(h["evidence_json"] or "{}")}
            for h in hits
        ],
    }
