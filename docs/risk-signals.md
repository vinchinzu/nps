# Risk signal catalogue

Source: research agent drawing on FinCEN, FATF R8, Treasury NMLRA, GAO,
and academic nonprofit-fraud literature. Each signal is mapped to fields
we actually have in `organizations` / `filings` / `filing_details`.

Weights are integer 1-10. A signal may be suppressed for certain NTEE
codes, return types, or BMF regions where the pattern is legitimate.

> This file is the **spec**. Implementation lives in
> `src/i990/risk/signals.py`. Numeric thresholds may be tuned during
> calibration; update this doc when they are.

## Financial shape

### `shell_org_zero_activity` — weight 7
Organization reports assets but near-zero revenue/expenses, suggesting dormant shell.
- Logic: `total_assets_eoy > 100000 AND total_revenue < 5000 AND total_expenses < 5000`
- FP suppressors: NTEE `B82` (scholarships), `T20` (private foundations), or `return_type='990PF'`
- Source: FATF R8 §3.4; Treasury 2024 NMLRA.

### `asset_revenue_mismatch` — weight 5
Assets disproportionately large relative to revenue flow — possible parked funds.
- Logic: `total_assets_eoy > 20 * total_revenue AND total_revenue > 10000`
- FP suppressors: `return_type='990PF'`, NTEE `C*` (environment/land), `A5*` (museums).
- Source: FinCEN 2005; Harris & Neely (2016).

### `negative_net_assets_large` — weight 6
Liabilities materially exceed assets — layering or insolvency cover.
- Logic: `net_assets_eoy < -50000 AND total_liabilities_eoy > 2 * total_assets_eoy`
- FP suppressors: NTEE `E2` (hospitals), `B4`/`B5` (higher ed).
- Source: GAO-02-526; Krishnan & Yetman (2011).

### `revenue_expense_blowthrough` — weight 6
Near 100% passthrough — classic conduit pattern.
- Logic: `total_revenue > 500000 AND ABS(total_expenses - total_revenue) / total_revenue < 0.02`
- FP suppressors: NTEE `T*` (philanthropy), or mission contains "fiscal sponsor"/"regrant".
- Source: FATF R8 Best Practices §IV; FinCEN 2005.

### `revenue_expense_passthrough_loose` *(derived; weight 4)*
Like blowthrough but with a looser 5% band. Use as secondary boost only.

### `liabilities_spike` — weight 5
Liabilities jump YoY without matching asset growth.
- Logic: `curr.total_liabilities_eoy > 3 * prior.total_liabilities_eoy AND prior.total_liabilities_eoy > 10000 AND curr.total_assets_eoy < 1.5 * prior.total_assets_eoy`
- FP suppressors: NTEE `B*`, `E*` with stable multi-year revenue.
- Source: Krishnan & Yetman (2011); FinCEN layering indicators.

### `pf_low_distribution_ratio` — weight 5
Private foundation hoards assets, distributes little (IRC §4942 ~5% floor).
- Logic: `return_type='990PF' AND total_assets_eoy > 500000 AND total_expenses < 0.03 * total_assets_eoy`
- FP suppressors: require 2 consecutive years.
- Source: IRC §4942; GAO-14-79.

## Year-over-year dynamics

### `explosive_revenue_growth` — weight 7
10x+ YoY revenue jump on a small base.
- Logic: self-join on `ein`, consecutive tax years. `curr.total_revenue > 10 * prior.total_revenue AND prior.total_revenue BETWEEN 1000 AND 250000`
- FP suppressors: NTEE `M*` (disaster), ruling date within last 2 years.
- Source: Treasury NMLRA 2024; ProPublica investigations.

### `new_org_large_flows` — weight 6
Recently ruled exempt org already moving large sums.
- Logic: `(tax_year - year(ruling)) <= 2 AND total_revenue > 1000000`
- FP suppressors: spinoffs (manual review of mission).
- Source: FATF R8 §12; FinCEN 2005.

### `filing_gap_then_return` — weight 6
Org goes dark ≥2 years, returns with material flows.
- Logic: `tax_period` gap ≥ 24 months followed by `total_revenue > 250000`
- FP suppressors: orgs that filed 990-N during dormancy (not in our index).
- Source: FATF R8 §3.4.

### `form_ez_near_threshold` — weight 4
EZ filer hugging the filing threshold.
- Logic: `return_type='990EZ' AND total_revenue BETWEEN 190000 AND 199999`
- FP suppressors: half-severity boost only.
- Source: IRS Form 990 thresholds; Harris et al.

## Governance

### `officer_comp_vs_revenue` — weight 7
Single officer consumes >25% of revenue.
- Logic: `MAX(reportable_comp + other_comp) > 0.25 * total_revenue AND total_revenue > 100000`
- FP suppressors: revenue < $500k AND NTEE `E*`/`I*` (small clinics, legal aid).
- Source: Krishnan & Yetman (2011); GAO-07-563; NYT.

### `officer_zero_hours_paid` — weight 6
Paid officer reports ~0 hrs/week.
- Logic: `comp > 25000 AND hours_per_week < 1`
- FP suppressors: board stipends; paid via related entity.
- Source: IRS Form 990 instructions; Harris, Petrovits & Yetman (2017).

### `officer_count_anomaly` — weight 4
Fewer than 3 officers listed on a material filing.
- Logic: `json_array_length(officers_json) < 3 AND total_revenue > 100000`
- FP suppressors: `foundation` code = private foundation; family foundations.
- Source: FATF R8; GAO-02-526.

### `officer_name_collision` — weight 4
Same officer name appears across many unrelated EINs — nominee pattern.
- Logic: require `normalized_name + street+zip` to recur across `3..25` EINs, then collapse to one hit per EIN. Single-name overlaps are a half-severity boost; multi-name overlaps score full severity.
- FP suppressors: common names, professional board members, registered agents, and allowlisted related-entity systems sharing a campus or local institutional family.
- Source: FinCEN beneficial-ownership guidance; journalism on nominee directors.

## Physical and digital footprint

### `address_po_box_only` — weight 3
HQ is PO Box / mail drop.
- Logic: `street REGEXP '^(PO BOX|P\.O\.|POBOX|PMB)'`
- Combine with `total_revenue > 250000` to reduce FPs.
- Source: FinCEN 2005; FATF R8 §3.4.

### `address_state_mismatch` — weight 2
BMF state ≠ filing address state.
- Logic: `organizations.state <> filing_details.state`
- Use as context boost only.
- Source: GAO-14-405; Harris (2014).

### `shared_address_cluster` — weight 6
≥5 distinct EINs at the same normalized street address.
- Logic: `COUNT(DISTINCT ein) GROUP BY normalized(street, zip) >= 5`
- FP suppressors: known registered-agent addresses, fiscal sponsors, university incubators.
- Source: FATF R8 typologies; ProPublica.

### `missing_website_large_org` — weight 3
Large org with no web presence.
- Logic: `(website IS NULL OR LENGTH(website) < 5) AND total_revenue > 500000`
- FP suppressors: `return_type='990PF'`.
- Source: ProPublica methodology; FATF R8 transparency.

### `mission_empty_or_generic` — weight 4
Blank / one-word mission despite material activity.
- Logic: `(mission IS NULL OR LENGTH(mission) < 20 OR LOWER(mission) IN ('charitable','education','religious')) AND total_revenue > 250000`
- FP suppressors: may be in Schedule O; confirm across years.
- Source: Frumkin (2002); IRS Dirty Dozen.

## Jurisdictional / categorical

### `international_region_high_flow` — weight 6
International-region BMF with material revenue.
- Logic: `bmf_region='eo4' AND total_revenue > 250000`
- FP suppressors: allowlist Red Cross/MSF/etc.
- Source: FATF R8 §3; Treasury NMLRA.

### `high_risk_ntee_international` — weight 4
NTEE `Q*` (foreign affairs) with flows. Boost, not standalone.
- Source: FATF R8; FinCEN.

### `high_risk_ntee_combo` — weight 5
`X*`/`Q*`/`Y*` NTEE + weak governance.
- Logic: `ntee_cd LIKE 'X%' OR 'Q%' OR 'Y%' AND officers<3 AND (no website OR no mission)`
- Source: IRS Dirty Dozen; Archambeault et al.

### `subsection_risk_profile` — weight 4
501(c)(4)/(6)/(7) with large flows. Boost only.
- Logic: `subsection IN ('04','06','07') AND total_revenue > 1000000`
- FP suppressors: legitimate trade associations.
- Source: Treasury NMLRA 2024; FinCEN.

## Identity / status

### `name_similarity_to_known_charity` — weight 8
Name ≥ 0.85 similarity to well-known charity, not exact.
- Requires: curated top-charities list.
- FP suppressors: legitimate local chapters — require EIN not in affiliate hierarchy.
- Source: IRS Dirty Dozen; FTC.

### `bmf_unmapped_filer` — weight 9
Parsed filing exists for an EIN missing from the current BMF snapshot.
- Logic: `LEFT JOIN organizations USING (ein) WHERE organizations.ein IS NULL`
- FP suppressors: require material revenue to avoid tiny/noise filings.
- Source: operational proxy for revoked, removed, or mismatched EIN records in this dataset.

---

## Scoring approach

`risk_scores.total_score = Σ (signal.weight * hit_severity)` where
severity is 1.0 for binary hits, 0.5 for weak matches (e.g. signals
marked "boost only"), and 0.0 if a suppressor fires.

Tiers (tunable during validation):
- **Tier 1 (critical)**: score ≥ 25 OR any single weight-9 hit
- **Tier 2 (elevated)**: 15 ≤ score < 25
- **Tier 3 (notable)**: 6 ≤ score < 15
- **Untiered**: score < 6

Each tier is a watchlist, not an accusation. Every hit records
`evidence_json` so a human reviewer can see exactly what fired.
