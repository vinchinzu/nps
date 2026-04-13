"""Risk signal catalogue — implementation of docs/risk-signals.md.

Each signal is a SQL query over the existing `organizations`,
`filings`, and `filing_details` tables. The query must return rows
shaped as (ein, tax_year, severity, evidence) where:
    ein        : TEXT — the subject EIN
    tax_year   : INT or NULL — filing year, or NULL for aggregate signals
    severity   : REAL in [0,1] — 1.0 full, 0.5 "boost only"
    evidence   : TEXT (JSON string) — why this fired; for reviewer display

False-positive suppressors are baked into each WHERE clause rather
than run as a separate pass, because SQLite joins make it cheap and
keeps everything auditable in one place.

To add a new signal, append a Signal() to SIGNALS and (optionally)
add a catalogue row to docs/risk-signals.md. The engine upserts into
the risk_signals table on every run, so weight tweaks take effect
immediately.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Signal:
    id: str
    weight: int
    category: str  # financial|yoy|governance|footprint|jurisdictional|identity
    description: str
    sql: str


# --- Financial shape ----------------------------------------------------

SHELL_ORG_ZERO_ACTIVITY = Signal(
    id="shell_org_zero_activity",
    weight=7,
    category="financial",
    description="Reports assets but near-zero revenue/expenses — dormant shell pattern.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'total_assets_eoy', d.total_assets_eoy,
                   'total_revenue',    d.total_revenue,
                   'total_expenses',   d.total_expenses
               ) AS evidence
          FROM filing_details d
          LEFT JOIN organizations o USING (ein)
         WHERE d.total_assets_eoy > 100000
           AND COALESCE(d.total_revenue, 0)  < 5000
           AND COALESCE(d.total_expenses, 0) < 5000
           AND COALESCE(d.return_type,'') != '990PF'
           AND COALESCE(SUBSTR(o.ntee_cd,1,3),'') NOT IN ('B82','T20')
           AND COALESCE(SUBSTR(o.ntee_cd,1,2),'') != 'T2'
    """,
)

ASSET_REVENUE_MISMATCH = Signal(
    id="asset_revenue_mismatch",
    weight=5,
    category="financial",
    description="Assets disproportionate to revenue flow — possible parked funds.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'total_assets_eoy', d.total_assets_eoy,
                   'total_revenue',    d.total_revenue,
                   'ratio', ROUND(1.0 * d.total_assets_eoy / NULLIF(d.total_revenue,0), 1)
               ) AS evidence
          FROM filing_details d
          LEFT JOIN organizations o USING (ein)
         WHERE d.total_revenue > 10000
           AND d.total_assets_eoy > 20 * d.total_revenue
           AND COALESCE(d.return_type,'') != '990PF'
           AND COALESCE(SUBSTR(o.ntee_cd,1,1),'') != 'C'
           AND COALESCE(SUBSTR(o.ntee_cd,1,2),'') != 'A5'
    """,
)

NEGATIVE_NET_ASSETS_LARGE = Signal(
    id="negative_net_assets_large",
    weight=6,
    category="financial",
    description="Liabilities materially exceed assets — layering or insolvency cover.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'net_assets_eoy',       d.net_assets_eoy,
                   'total_liabilities_eoy', d.total_liabilities_eoy,
                   'total_assets_eoy',      d.total_assets_eoy
               ) AS evidence
          FROM filing_details d
          LEFT JOIN organizations o USING (ein)
         WHERE d.net_assets_eoy < -50000
           AND d.total_liabilities_eoy > 2 * COALESCE(d.total_assets_eoy,0)
           AND COALESCE(SUBSTR(o.ntee_cd,1,2),'') NOT IN ('E2','B4','B5')
    """,
)

REVENUE_EXPENSE_BLOWTHROUGH = Signal(
    id="revenue_expense_blowthrough",
    weight=6,
    category="financial",
    description="Near 100% passthrough — classic conduit pattern.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'total_revenue',  d.total_revenue,
                   'total_expenses', d.total_expenses,
                   'delta_pct', ROUND(100.0 * ABS(d.total_expenses - d.total_revenue) / NULLIF(d.total_revenue,0), 2)
               ) AS evidence
          FROM filing_details d
          LEFT JOIN organizations o USING (ein)
         WHERE d.total_revenue > 500000
           AND d.total_expenses IS NOT NULL
           AND ABS(d.total_expenses - d.total_revenue) * 1.0 / d.total_revenue < 0.02
           AND COALESCE(SUBSTR(o.ntee_cd,1,1),'') != 'T'
           AND LOWER(COALESCE(d.mission,'')) NOT LIKE '%fiscal sponsor%'
           AND LOWER(COALESCE(d.mission,'')) NOT LIKE '%regrant%'
    """,
)

PF_LOW_DISTRIBUTION_RATIO = Signal(
    id="pf_low_distribution_ratio",
    weight=5,
    category="financial",
    description="Private foundation hoards assets, distributes little (IRC §4942 ~5%).",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'total_assets_eoy', d.total_assets_eoy,
                   'total_expenses',   d.total_expenses,
                   'distribution_ratio',
                       ROUND(1.0 * d.total_expenses / NULLIF(d.total_assets_eoy,0), 4)
               ) AS evidence
          FROM filing_details d
         WHERE d.return_type = '990PF'
           AND d.total_assets_eoy > 500000
           AND COALESCE(d.total_expenses,0) < 0.03 * d.total_assets_eoy
    """,
)

# --- Year-over-year dynamics -------------------------------------------

EXPLOSIVE_REVENUE_GROWTH = Signal(
    id="explosive_revenue_growth",
    weight=7,
    category="yoy",
    description="10x+ YoY revenue jump on a small base.",
    sql="""
        SELECT curr.ein, curr.tax_year, 1.0 AS severity,
               json_object(
                   'prior_year',    prior.tax_year,
                   'prior_revenue', prior.total_revenue,
                   'curr_revenue',  curr.total_revenue,
                   'multiple', ROUND(1.0 * curr.total_revenue / NULLIF(prior.total_revenue,0), 1)
               ) AS evidence
          FROM filing_details curr
          JOIN filing_details prior
            ON prior.ein = curr.ein
           AND prior.tax_year = curr.tax_year - 1
          LEFT JOIN organizations o ON o.ein = curr.ein
         WHERE prior.total_revenue BETWEEN 1000 AND 250000
           AND curr.total_revenue > 10 * prior.total_revenue
           AND COALESCE(SUBSTR(o.ntee_cd,1,1),'') != 'M'
    """,
)

NEW_ORG_LARGE_FLOWS = Signal(
    id="new_org_large_flows",
    weight=6,
    category="yoy",
    description="Recently ruled exempt org already moving large sums.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'total_revenue', d.total_revenue,
                   'ruling',        o.ruling,
                   'years_since_ruling', d.tax_year - CAST(SUBSTR(o.ruling,1,4) AS INTEGER)
               ) AS evidence
          FROM filing_details d
          JOIN organizations o USING (ein)
         WHERE d.total_revenue > 1000000
           AND o.ruling IS NOT NULL
           AND o.ruling != '000000'          -- group exemptions, not real rulings
           AND LENGTH(o.ruling) >= 4
           AND d.tax_year - CAST(SUBSTR(o.ruling,1,4) AS INTEGER) BETWEEN 0 AND 2
    """,
)

FORM_DOWNGRADE_THEN_GROWTH = Signal(
    id="form_ez_near_threshold",
    weight=4,
    category="yoy",
    description="990EZ filer hugging the $200k threshold (possible suppression).",
    # The IRS efile rails reject EZ filings with revenue over $200,000,
    # so the original "EZ > threshold" signal cannot fire against this
    # data source. Instead we flag EZ filers right at the ceiling, where
    # an org may be shaping numbers to stay on the simpler form.
    sql="""
        SELECT ein, tax_year, 0.5 AS severity,
               json_object(
                   'return_type',   return_type,
                   'total_revenue', total_revenue
               ) AS evidence
          FROM filing_details
         WHERE return_type = '990EZ'
           AND total_revenue BETWEEN 190000 AND 199999
    """,
)

# --- Governance --------------------------------------------------------

OFFICER_COMP_VS_REVENUE = Signal(
    id="officer_comp_vs_revenue",
    weight=7,
    category="governance",
    description="Single officer consumes >25% of revenue.",
    # json_each unnests officers_json; we take the max comp ratio across officers.
    sql="""
        WITH o_max AS (
            SELECT d.ein, d.tax_year, d.total_revenue,
                   MAX(
                       COALESCE(CAST(json_extract(e.value,'$.reportable_comp') AS INTEGER),0) +
                       COALESCE(CAST(json_extract(e.value,'$.other_comp')      AS INTEGER),0)
                   ) AS top_comp,
                   (SELECT json_extract(value,'$.name')
                      FROM json_each(d.officers_json)
                     ORDER BY
                       (COALESCE(CAST(json_extract(value,'$.reportable_comp') AS INTEGER),0) +
                        COALESCE(CAST(json_extract(value,'$.other_comp')      AS INTEGER),0)) DESC
                     LIMIT 1) AS top_name
              FROM filing_details d, json_each(d.officers_json) e
             WHERE d.officers_json IS NOT NULL
               AND d.officers_json != '[]'
               AND d.total_revenue > 100000
             GROUP BY d.ein, d.tax_year, d.total_revenue
        )
        SELECT m.ein, m.tax_year, 1.0 AS severity,
               json_object(
                   'top_comp',      m.top_comp,
                   'total_revenue', m.total_revenue,
                   'top_name',      m.top_name,
                   'ratio', ROUND(1.0 * m.top_comp / NULLIF(m.total_revenue,0), 3)
               ) AS evidence
          FROM o_max m
          LEFT JOIN organizations o USING (ein)
         WHERE m.top_comp > 0.25 * m.total_revenue
           AND NOT (m.total_revenue < 500000
                    AND COALESCE(SUBSTR(o.ntee_cd,1,1),'') IN ('E','I'))
    """,
)

OFFICER_ZERO_HOURS_PAID = Signal(
    id="officer_zero_hours_paid",
    weight=6,
    category="governance",
    description="Paid officer reports ~0 hrs/week.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'name',  json_extract(e.value,'$.name'),
                   'comp',  COALESCE(CAST(json_extract(e.value,'$.reportable_comp') AS INTEGER),0)
                          + COALESCE(CAST(json_extract(e.value,'$.other_comp')      AS INTEGER),0),
                   'hours', json_extract(e.value,'$.hours_per_week')
               ) AS evidence
          FROM filing_details d, json_each(d.officers_json) e
         WHERE d.officers_json IS NOT NULL
           AND (COALESCE(CAST(json_extract(e.value,'$.reportable_comp') AS INTEGER),0)
               + COALESCE(CAST(json_extract(e.value,'$.other_comp')      AS INTEGER),0)) > 25000
           AND CAST(COALESCE(json_extract(e.value,'$.hours_per_week'),'0') AS REAL) < 1.0
         GROUP BY d.ein, d.tax_year
    """,
)

OFFICER_COUNT_ANOMALY = Signal(
    id="officer_count_anomaly",
    weight=4,
    category="governance",
    description="Fewer than 3 officers listed on a material filing.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'officer_count', json_array_length(d.officers_json),
                   'total_revenue', d.total_revenue
               ) AS evidence
          FROM filing_details d
          LEFT JOIN organizations o USING (ein)
         WHERE d.officers_json IS NOT NULL
           AND json_array_length(d.officers_json) < 3
           AND d.total_revenue > 100000
           AND COALESCE(o.foundation,'') NOT IN ('02','03','04')
    """,
)

OFFICER_NAME_COLLISION = Signal(
    id="officer_name_collision",
    weight=4,
    category="governance",
    description="Officer name reused across many unrelated EINs — possible nominee pattern.",
    # Heavy de-duplication: raw common-name collisions are mostly noise
    # (TOM WILLIAMS on 50 boards, nothing to see). We require the
    # colliding EINs to ALSO share a street+zip in the BMF — i.e. a
    # name clustered with an address clustered. That intersection is
    # much closer to "nominee at a shell farm" than either alone.
    # Emit one aggregate hit per EIN (not one per officer-name), and
    # downweight single-name overlaps. Without that cap, legitimate
    # hospital systems and housing sponsors can overwhelm the score
    # simply because the same board appears across related entities.
    sql="""
        WITH names AS (
            SELECT DISTINCT d.ein,
                   TRIM(UPPER(
                       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                           COALESCE(json_extract(e.value,'$.name'),''),
                       '.', ''), ',', ''), '  ', ' '), ' JR', ''), ' SR', '')
                   )) AS norm
              FROM filing_details d, json_each(d.officers_json) e
             WHERE d.officers_json IS NOT NULL
        ),
        addr_clusters AS (
            SELECT ein,
                   UPPER(TRIM(street)) || '|' || TRIM(zip) AS key
              FROM organizations
             WHERE street IS NOT NULL AND LENGTH(street) > 4
               AND zip IS NOT NULL
        ),
        name_addr AS (
            SELECT n.norm, n.ein, a.key
              FROM names n
              JOIN addr_clusters a USING (ein)
             WHERE LENGTH(n.norm) > 8
        ),
        collisions AS (
            -- same officer-name + same address across 3+ EINs
            SELECT norm, key, COUNT(DISTINCT ein) AS n
              FROM name_addr
             GROUP BY norm, key
            HAVING COUNT(DISTINCT ein) BETWEEN 3 AND 25
        ),
        org_hits AS (
            SELECT na.ein,
                   na.key,
                   COUNT(DISTINCT na.norm) AS colliding_officer_count,
                   MAX(c.n) AS max_cluster_size
              FROM name_addr na
              JOIN collisions c ON c.norm = na.norm AND c.key = na.key
             GROUP BY na.ein, na.key
        )
        SELECT oh.ein,
               NULL AS tax_year,
               CASE
                   WHEN oh.colliding_officer_count >= 2 THEN 1.0
                   ELSE 0.5
               END AS severity,
               json_object(
                   'cluster_key', oh.key,
                   'colliding_officer_count', oh.colliding_officer_count,
                   'max_cluster_size', oh.max_cluster_size,
                   'sample_officers', (
                       SELECT json_group_array(norm)
                         FROM (
                             SELECT DISTINCT na2.norm AS norm
                               FROM name_addr na2
                               JOIN collisions c2
                                 ON c2.norm = na2.norm AND c2.key = na2.key
                              WHERE na2.ein = oh.ein
                                AND na2.key = oh.key
                              ORDER BY na2.norm
                              LIMIT 5
                         )
                   )
               ) AS evidence
          FROM org_hits oh
         WHERE oh.key NOT IN (SELECT key FROM risk_helper_related_addrs)
           AND oh.ein NOT IN (SELECT ein FROM risk_helper_related_eins)
    """,
)

ROUND_NUMBER_REVENUE = Signal(
    id="round_number_revenue",
    weight=3,
    category="financial",
    description="Revenue is suspiciously round ($100k multiples, >$1M) — boost only.",
    sql="""
        SELECT ein, tax_year, 0.5 AS severity,
               json_object('total_revenue', total_revenue) AS evidence
          FROM filing_details
         WHERE total_revenue >= 1000000
           AND total_revenue % 100000 = 0
    """,
)

# --- Physical / digital footprint --------------------------------------

ADDRESS_PO_BOX_ONLY = Signal(
    id="address_po_box_only",
    weight=3,
    category="footprint",
    description="HQ is a PO Box / mail drop on a materially-sized org.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object('street', o.street, 'total_revenue', d.total_revenue) AS evidence
          FROM filing_details d
          JOIN organizations o USING (ein)
         WHERE d.total_revenue > 250000
           AND (
               UPPER(TRIM(COALESCE(o.street,''))) LIKE 'PO BOX%'
            OR UPPER(TRIM(COALESCE(o.street,''))) LIKE 'P.O.%'
            OR UPPER(TRIM(COALESCE(o.street,''))) LIKE 'POBOX%'
            OR UPPER(TRIM(COALESCE(o.street,''))) LIKE 'PMB %'
           )
    """,
)

SHARED_ADDRESS_CLUSTER = Signal(
    id="shared_address_cluster",
    weight=6,
    category="footprint",
    description=">=5 distinct EINs at the same street+zip — possible nominee cluster.",
    # tax_year NULL because this is a cross-EIN aggregate.
    # Cap the cluster size low: profile showed the top addresses are
    # legit aggregators (Wells Fargo trust HQ, Ducks Unlimited HQ,
    # Foundation Source, bank trustee POs). Restricting to 5..50
    # captures plausible nominee clusters. Also exclude
    # risk_helper_related_addrs (corporate HQ of a related-entity
    # group like a hospital system) which would otherwise dominate.
    sql="""
        WITH clusters AS (
            SELECT UPPER(TRIM(street)) AS s, TRIM(zip) AS z,
                   UPPER(TRIM(street)) || '|' || TRIM(zip) AS key,
                   COUNT(DISTINCT ein) AS n
              FROM organizations
             WHERE street IS NOT NULL AND LENGTH(street) > 4
               AND zip IS NOT NULL
             GROUP BY UPPER(TRIM(street)), TRIM(zip)
            HAVING COUNT(DISTINCT ein) BETWEEN 5 AND 50
        )
        SELECT o.ein, NULL AS tax_year, 1.0 AS severity,
               json_object('street', o.street, 'zip', o.zip, 'cluster_size', c.n) AS evidence
          FROM organizations o
          JOIN clusters c
            ON UPPER(TRIM(o.street)) = c.s AND TRIM(o.zip) = c.z
         WHERE c.key NOT IN (SELECT key FROM risk_helper_related_addrs)
    """,
)

MISSING_WEBSITE_LARGE_ORG = Signal(
    id="missing_website_large_org",
    weight=3,
    category="footprint",
    description="Large org with no web presence.",
    sql="""
        SELECT ein, tax_year, 1.0 AS severity,
               json_object('total_revenue', total_revenue) AS evidence
          FROM filing_details
         WHERE total_revenue > 500000
           AND (website IS NULL OR LENGTH(TRIM(website)) < 5 OR UPPER(website) = 'N/A')
           AND return_type != '990PF'
    """,
)

MISSION_EMPTY_OR_GENERIC = Signal(
    id="mission_empty_or_generic",
    weight=4,
    category="footprint",
    description="Blank / one-word mission despite material activity.",
    sql="""
        SELECT ein, tax_year, 1.0 AS severity,
               json_object('mission', mission, 'total_revenue', total_revenue) AS evidence
          FROM filing_details
         WHERE total_revenue > 250000
           AND (
               mission IS NULL
            OR LENGTH(TRIM(mission)) < 20
            OR LOWER(TRIM(mission)) IN ('charitable','education','religious','n/a','none')
           )
    """,
)

# --- Jurisdictional / categorical --------------------------------------

INTERNATIONAL_REGION_HIGH_FLOW = Signal(
    id="international_region_high_flow",
    weight=6,
    category="jurisdictional",
    description="International-region BMF with material revenue.",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object('bmf_region', o.bmf_region, 'total_revenue', d.total_revenue) AS evidence
          FROM filing_details d
          JOIN organizations o USING (ein)
         WHERE o.bmf_region = 'eo4'
           AND d.total_revenue > 250000
    """,
)

HIGH_RISK_NTEE_COMBO = Signal(
    id="high_risk_ntee_combo",
    weight=5,
    category="jurisdictional",
    description="X/Q/Y NTEE + weak governance (thin board, no web, no mission).",
    sql="""
        SELECT d.ein, d.tax_year, 1.0 AS severity,
               json_object(
                   'ntee_cd',       o.ntee_cd,
                   'officer_count', json_array_length(COALESCE(d.officers_json,'[]')),
                   'has_website',   (d.website IS NOT NULL AND LENGTH(TRIM(d.website))>4)
               ) AS evidence
          FROM filing_details d
          JOIN organizations o USING (ein)
         WHERE SUBSTR(COALESCE(o.ntee_cd,''),1,1) IN ('X','Q','Y')
           AND (
                json_array_length(COALESCE(d.officers_json,'[]')) < 3
             OR d.website IS NULL OR LENGTH(TRIM(COALESCE(d.website,''))) < 5
             OR d.mission IS NULL OR LENGTH(TRIM(COALESCE(d.mission,''))) < 20
           )
           AND d.total_revenue > 100000
    """,
)

SUBSECTION_RISK_PROFILE = Signal(
    id="subsection_risk_profile",
    weight=4,
    category="jurisdictional",
    description="501(c)(4)/(6)/(7) with large flows — boost only.",
    sql="""
        SELECT d.ein, d.tax_year, 0.5 AS severity,
               json_object('subsection', o.subsection, 'total_revenue', d.total_revenue) AS evidence
          FROM filing_details d
          JOIN organizations o USING (ein)
         WHERE o.subsection IN ('04','06','07')
           AND d.total_revenue > 1000000
    """,
)

# --- Identity / status --------------------------------------------------

BMF_UNMAPPED_FILER = Signal(
    id="bmf_unmapped_filer",
    weight=9,
    category="identity",
    description="Org has filings but no row in the current BMF (removed, never published, or EIN mismatch).",
    # The ingested BMF (eo1..eo4) only lists currently-active exempt
    # orgs — revoked/terminated codes are not present at all, so the
    # spec's `status IN (20,21,22,97)` signal is dead in this dataset.
    # We approximate it with the stronger fact that we parsed a filing
    # for an EIN that does NOT appear in the BMF. High weight because
    # every legitimately-exempt filer should be in the BMF; absence
    # suggests status change, EIN reuse, or impersonation.
    sql="""
        SELECT d.ein, MAX(d.tax_year) AS tax_year, 1.0 AS severity,
               json_object('total_revenue', MAX(d.total_revenue)) AS evidence
          FROM filing_details d
          LEFT JOIN organizations o USING (ein)
         WHERE o.ein IS NULL
           AND d.total_revenue > 100000
         GROUP BY d.ein
    """,
)


SIGNALS: list[Signal] = [
    SHELL_ORG_ZERO_ACTIVITY,
    ASSET_REVENUE_MISMATCH,
    NEGATIVE_NET_ASSETS_LARGE,
    REVENUE_EXPENSE_BLOWTHROUGH,
    PF_LOW_DISTRIBUTION_RATIO,
    EXPLOSIVE_REVENUE_GROWTH,
    NEW_ORG_LARGE_FLOWS,
    FORM_DOWNGRADE_THEN_GROWTH,
    OFFICER_COMP_VS_REVENUE,
    OFFICER_ZERO_HOURS_PAID,
    OFFICER_COUNT_ANOMALY,
    OFFICER_NAME_COLLISION,
    ROUND_NUMBER_REVENUE,
    ADDRESS_PO_BOX_ONLY,
    SHARED_ADDRESS_CLUSTER,
    MISSING_WEBSITE_LARGE_ORG,
    MISSION_EMPTY_OR_GENERIC,
    INTERNATIONAL_REGION_HIGH_FLOW,
    HIGH_RISK_NTEE_COMBO,
    SUBSECTION_RISK_PROFILE,
    BMF_UNMAPPED_FILER,
]

BY_ID: dict[str, Signal] = {s.id: s for s in SIGNALS}
