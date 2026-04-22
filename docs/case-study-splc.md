# SPLC Case File

As of April 21, 2026, this document treats the Southern Poverty Law Center matter as a live DOJ charging event and a reusable nonprofit-fraud case study. The DOJ allegations are not convictions. The goal here is to gather the core public facts, resolve the entity across public systems, and turn the case into joinable fields and candidate risk flags.

## Case posture

- DOJ press release date: `2026-04-21`
- Charging source: U.S. Department of Justice, Office of Public Affairs
- Alleged charges: `11 counts` spanning `wire fraud`, `false statements to a federally insured bank`, and `conspiracy to commit concealment money laundering`
- Alleged scheme window: `2014-2023`
- Alleged diverted amount: `more than $3 million`
- Core allegation: donors were told SPLC was dismantling violent extremist groups while donor funds were allegedly routed to people associated with those groups through covert payment structures and fictitious entities
- Caveat: indictment allegations only, not proof

Primary DOJ sources:

- DOJ release: <https://www.justice.gov/opa/pr/federal-grand-jury-charges-southern-poverty-law-center-wire-fraud-false-statements-and>
- Indictment PDF: <https://www.justice.gov/opa/media/1437146/dl>

## Canonical subject

```text
entity_name: SOUTHERN POVERTY LAW CENTER
legal_name_local_bmf: SOUTHERN POVERTY LAW CENTER INC
aliases:
  - SOUTHERN POVERTY LAW CENTER
  - SOUTHERN POVERTY LAW CENTER INC
  - SOUTHERN POVERTY LAW CENTER INCORPORATED
ein: 630598743
uei: H2GRQHMF6A63
duns: 078962198
cage: 3VAG6
recipient_id_usaspending: d6b3baaa-a86d-29ea-d036-a72ccefbd3d4-C
address_line1: 400 WASHINGTON AVE
city: MONTGOMERY
state: AL
zip: 36104-4344
subsection: 03
ntee_cd: I83Z
ruling_date: 1971-08
bmf_region: eo3
```

## Cross-source resolution

| Source | Fields resolved | Notes |
|---|---|---|
| Local IRS BMF / `organizations` | EIN, legal name, address, subsection, NTEE, ruling date, asset and revenue snapshot | Strongest local anchor for this repo |
| Local IRS filings / `filings` + `filing_details` | 2017-2023 Form 990 history, mission text, websites, officers JSON | Good for year-over-year financial shape |
| DOJ release | entity name, city/state, charge date, scheme allegations | Best source for label creation |
| SPLC financial page | EIN, 501(c)(3) claim, donor-funding claim, no-government-funds claim, endowment/program-service metrics | Useful for disclosure-consistency checks |
| SAM.gov | UEI, CAGE, registration status, address, purpose of registration | Important bridge into federal award systems |
| USAspending | recipient ID, UEI, DUNS, alternate names, address, tiny award history | Confirms there is only a very small federal award footprint in the checked match |

## Public-source facts gathered

### DOJ

From the April 21, 2026 DOJ release:

- DOJ says a federal grand jury in Montgomery, Alabama returned an indictment charging SPLC.
- DOJ says SPLC secretly funneled more than `$3 million` in donated funds to individuals associated with extremist groups between `2014` and `2023`.
- DOJ says SPLC allegedly opened bank accounts tied to fictitious entities to disguise the source, ownership, and control of funds.
- DOJ says the case was investigated by `FBI` with assistance from `IRS-CI`.

### SPLC self-disclosure

SPLC’s own financial-information page states:

- it is a `501(c)(3)` organization
- EIN `63-0598743`
- support is derived primarily from donor contributions
- `No government funds are received or used for its efforts`
- latest program-service share shown on page: `73.9%`
- endowment shown on page: `$731.9 million`

Source:

- <https://www.splcenter.org/about/financial-information/>

### SAM.gov

SAM public entity query matched one record:

- legal business name: `SOUTHERN POVERTY LAW CENTER`
- UEI: `H2GRQHMF6A63`
- CAGE: `3VAG6`
- registration status: `Inactive`
- registration date: `2004-05-07`
- last update / expiration: `2023-05-25`
- purpose of registration: `Federal Assistance Awards`
- address matches Montgomery HQ

Query used:

- <https://sam.gov/api/prod/entity-information/v2/entities?api_key=public&legalBusinessName=Southern%20Poverty%20Law%20Center&page=0&size=10>

### USAspending

Resolved recipient:

- recipient ID: `d6b3baaa-a86d-29ea-d036-a72ccefbd3d4-C`
- UEI: `H2GRQHMF6A63`
- DUNS: `078962198`
- alternate names include `SOUTHERN POVERTY LAW CENTER INC`
- address matches Montgomery HQ

Direct recipient endpoint:

- <https://api.usaspending.gov/api/v2/recipient/d6b3baaa-a86d-29ea-d036-a72ccefbd3d4-C/>

Award history observed from the USAspending API:

| Fiscal year | Amount | Type |
|---|---:|---|
| 2008 | 1,280 | contract |
| 2009 | -800 | contract adjustment |
| 2021 | 800 | grant |
| 2022 | 7,700 | grant |

Awarding agencies observed:

- Department of State: `$8,500`
- Department of Homeland Security: `$480`

Interpretation:

- For this matched entity, the public federal-award footprint appears very small.
- That makes this a better case study for `donor-fund misuse`, `entity concealment`, and `counterparty risk` than for large-scale federal-award fraud.

## Local IRS filing profile

These figures come from the local SQLite database in this repo for EIN `630598743`.

| Tax year | Revenue | Expenses | Assets EOY | Liabilities EOY | Net assets EOY |
|---|---:|---:|---:|---:|---:|
| 2017 | 121,975,162 | 74,970,297 | 518,251,510 | 25,758,145 | 492,493,365 |
| 2018 | 117,034,012 | 88,428,653 | 569,403,418 | 26,259,368 | 543,144,050 |
| 2019 | 132,918,576 | 97,409,030 | 614,389,428 | 26,585,814 | 587,803,614 |
| 2020 | 132,750,377 | 106,617,578 | 801,148,661 | 31,421,095 | 769,727,566 |
| 2021 | 140,350,982 | 111,043,025 | 723,488,477 | 36,531,547 | 686,956,930 |
| 2022 | 169,857,376 | 122,131,443 | 749,083,798 | 37,750,494 | 711,333,304 |
| 2023 | 129,063,290 | 128,982,970 | 822,198,315 | 35,430,069 | 786,768,246 |

Local parsed mission text in recent filings:

- `THE SOUTHERN POVERTY LAW CENTER IS A CATALYST FOR RACIAL JUSTICE IN THE SOUTH AND BEYOND...`

Local parsed website fields:

- `SPLCENTER.ORG`
- `LEARNINGFORJUSTICE.ORG`
- older years also show `TEACHINGTOLERANCE.ORG`

Notable local observation:

- The stored `risk_scores` table shows `total_score = 0` for SPLC.
- A direct manual run of the current `revenue_expense_blowthrough` SQL, however, matches the `2023` filing because revenue and expenses differ by only about `0.06%`.
- That is useful as a case-study note: this entity was not flagged in the persisted score table, but one current signal query already sees a possible conduit-like financial shape in the latest filing year.

## Why this case matters for risk design

This case is valuable because it sits in the gap between:

- what a standard 990-only risk model can see, and
- what becomes visible only after joining in DOJ, SAM, USAspending, and adverse-event data

The local IRS data mostly shows a large, established nonprofit with stable filings and strong balance-sheet scale. The alleged misconduct instead turns on:

- mission-to-money contradiction
- concealed counterparties
- false statements to a bank
- fictitious entities used as payment shells
- donor-use misrepresentation

Those are not well captured by balance-sheet-only heuristics.

## Candidate risk flags from this case

### Flags available now from gathered sources

| Flag id | Source needed | Why it fits this case |
|---|---|---|
| `doj_charged_nonprofit` | DOJ press releases / indictments | Entity was formally charged by DOJ |
| `mission_counterparty_conflict` | DOJ + filing mission text | Stated mission opposes counterparties allegedly being funded |
| `donor_use_misrepresentation` | DOJ + organization fundraising language | DOJ alleges donor funds were used for concealed purposes |
| `fictitious_entity_banking` | DOJ indictment / bank-account allegations | DOJ alleges fictitious entities and covert accounts |
| `money_laundering_concealment_alleged` | DOJ charge list | Case includes concealment money laundering |
| `high_risk_counterparty_payments` | DOJ indictment + external watchlists | Alleged payees linked to extremist organizations |
| `sam_inactive_but_federal_identity_present` | SAM + USAspending | Public federal identity exists even though current SAM status is inactive |
| `tiny_federal_spend_vs_large_nonprofit` | USAspending + IRS | Useful context feature, not a fraud signal by itself |
| `revenue_expense_blowthrough_latest_year` | local `filing_details` | 2023 filing nearly exact revenue-expense parity |

### Flags that require more ingestion later

| Flag id | Extra data needed | Notes |
|---|---|---|
| `adverse_counterparty_match` | named payee / counterparty graph | match payees against extremist, sanctions, criminal, or adverse-event lists |
| `banking_shell_layering_pattern` | bank SAR-style or transaction data | not available in public 990 data |
| `fictitious_entity_name_cluster` | state business registries / bank-account metadata | useful if shell names can be resolved |
| `donor_claim_disclosure_mismatch` | fundraising archive + 990 + audited financials | compare public claims to actual flows and notes |

## Similar DOJ nonprofit cases

These are comparable public DOJ cases that help generalize scheme patterns beyond SPLC.

| Date | Entity / defendant | Pattern | Source |
|---|---|---|---|
| 2026-03-03 | Philadelphia religious nonprofit executive | diverted beneficiary funds, false internal ledgers, luxury condo purchase, money laundering transaction | <https://www.justice.gov/usao-edpa/pr/former-philadelphia-nonprofit-executive-pleads-guilty-fraud-and-money-laundering> |
| 2025-06-24 | Center for Special Needs Trust Administration | nonprofit used as slush fund, false account statements, complex financial concealment, money laundering conspiracy | <https://www.justice.gov/usao-mdfl/pr/florida-non-profit-founder-and-accountant-charged-stealing-over-100-million-special> |
| 2025-06-06 | Viet America Society / Hand-to-Hand Relief | false certifications on grant use, bribery, layered transfers to controlled entities, concealment laundering | <https://www.justice.gov/usao-cdca/pr/founder-oc-based-non-profit-charged-15-count-indictment-alleging-he-bribed-county> |
| 2025-09-23 | Bay City State Theatre / Bay City Historical Society | nonprofit mission funds diverted to unrelated pet project, fictitious board minutes and invoices, attempted fraudulent federal grant replenishment | <https://www.justice.gov/usao-edmi/pr/former-non-profit-executive-director-and-city-development-official-pleads-guilty> |
| 2024-10-06 update | Black Lives Matter of Greater Atlanta | donations solicited under activist nonprofit branding, personal use of funds, alternate-name concealment around property purchase, money laundering | <https://www.justice.gov/usao-ndoh/pr/blm-activist-sentenced-prison-wire-fraud-and-money-laundering> |
| 2024-07-17 | Michele Fiore charity scheme | promised 100% of donations to memorial purpose, then allegedly used funds for personal and family expenses | <https://www.justice.gov/archives/opa/pr/former-las-vegas-city-councilwoman-charged-charity-fraud-scheme> |
| 2024-05-07 | Citadel Community Development Corp / Citadel Community Care Facility | federal grant embezzlement, personal use including wedding, travel, and crypto | <https://www.justice.gov/usao-cdca/pr/former-inland-empire-nonprofit-ceo-arrested-indictment-alleging-she-embezzled-federal> |
| 2024-07-17 | Washington Coalition of Crime Victim Advocates | false invoices for trainings that never happened, remote no-show executive, salary extraction from grant-supported nonprofit | <https://www.justice.gov/usao-wdwa/pr/woman-who-fraudulently-used-state-grant-monies-sentenced-probation-and-home> |
| 2022-11-04 | Douglas Sailors charity network | nominee directors, look-alike charity names, management-company extraction, false tax treatment | <https://www.justice.gov/usao-sdfl/pr/charity-operator-charged-diverting-millions-dollars-charitable-funds-and-evading> |
| 2023-04-21 | Latino Coalition Foundation / Hispanic Business Roundtable Institute | donation funds diverted to personal credit-card use, false 990s, bank-account control concentrated in one insider | <https://www.justice.gov/usao-wdtx/pr/former-nonprofit-leader-pleads-guilty-fraud-san-antonio> |
| 2023-12-22 charge / 2026-03-03 plea | Philadelphia religious nonprofit fund | beneficiary payments masked as personal checks, false ledgers, luxury spending, post-investigation asset liquidation | <https://www.justice.gov/usao-edpa/pr/former-executive-director-philadelphia-non-profit-fund-charged-stealing-over-16> ; <https://www.justice.gov/usao-edpa/pr/former-philadelphia-nonprofit-executive-pleads-guilty-fraud-and-money-laundering> |
| 2026-04-08 | Center for Community Academic Success Partnerships / South Suburban Community Services | inflated grant budgets, sham subcontractors, double-dipping across nonprofit programs, AmeriCorps misuse | <https://www.justice.gov/usao-ndil/pr/former-executive-chicago-area-non-profit-sentenced-prison-19-million-fraud-schemes> |
| 2026-01-09 | Encouraging Leaders | false grant progress reports to DOJ and other funders, fabricated events and beneficiaries, retained grant money | <https://www.justice.gov/usao-mn/pr/minneapolis-non-profit-director-charged-fraud> |
| 2020-03-13 update | On Your Feet / Family Resource Center | nonprofit used to defraud donors, false charitable returns, personal spending from charitable receipts | <https://www.justice.gov/usao-sdca/pr/charity-founders-plead-guilty-using-non-profit-defraud-donors-and-illegally-evade-taxes> |
| 2019-08-29 | Montana Native Women’s Coalition | theft from federally funded victim-services nonprofit, travel fraud, duplicate or unauthorized payments despite prior fraud warnings | <https://www.justice.gov/usao-mt/pr/montana-native-women-s-coalition-board-ex-chairwoman-charged-fraud-embezzlement-grant> |
| 2017-03-22 | Providence Plan | forged checks to insider-owned entity, conversion of federal and private grant funds, casino withdrawals | <https://www.justice.gov/usao-ri/pr/former-finance-director-pleads-guilty-embezzlement> |
| 2016-06-17 | Birmingham Health Care / Central Alabama Comprehensive Health | millions in federal grant funds routed through private companies controlled by nonprofit CEO, bank fraud, laundering | <https://www.justice.gov/usao-ndal/pr/federal-jury-convicts-former-non-profit-health-clinics-ceo-funneling-millions-grant> |
| 2015-09-11 | Frontline Initiative / Hero Program | donor and grant money for terminally ill children used for utilities, restaurants, sports tickets, spa gifts, and home remodeling | <https://www.justice.gov/usao-wdpa/pr/charity-director-admits-using-funds-personal-use-filing-false-tax-returns> |
| 2013-05-21 | Global Missions | phony charity representations, donor fraud, personal luxury spending, money laundering | <https://www.justice.gov/usao-ndca/pr/oakland-man-sentenced-121-months-and-ordered-pay-337-million-charity-fraud-scheme> |
| 2013-01-09 | USA Harvest founder | stole donations, personal travel and entertainment, money laundering and tax fraud | <https://www.justice.gov/usao-wdky/pr/founder-usa-harvest-charged-seven-count-federal-indictment-charges-include-stealing> |
| 2013-10-21 | National Relief Charities / Charity One | charity-to-charity transfer abuse, false financial statements, scholarship pretext, $4 million diverted and laundered | <https://www.justice.gov/usao-or/pr/former-president-national-charity-arrested-and-charged-4-million-fraud-and-money> |
| 2013-06-26 | Keely’s District Boxing and Youth Center | public and private youth-program grants used for gambling and personal expenses | <https://www.justice.gov/usao-dc/pr/executive-director-non-profit-pleads-guilty-wire-fraud-admits-using-more-200000-grants> |

Recurring patterns across these DOJ cases:

- donor-use misrepresentation
- personal enrichment hidden behind nonprofit branding
- false books, false tax returns, or false certifications
- nominee directors or controlled intermediaries
- alternate entities used to move or disguise proceeds
- money laundering or laundering-adjacent concealment behavior
- mission branding used to lower donor skepticism

Additional recurring sub-patterns from the expanded set:

- fictitious board minutes, fake invoices, and fake QuickBooks entries used to paper over diversions
- insider-owned vendors or shell entities used as extraction points
- false progress reports and false beneficiary counts used to keep grants flowing
- grant-budget inflation and sham subcontractors used to justify larger awards
- charity-to-charity pass-through abuse where a second nonprofit is created as the siphon vehicle
- post-discovery liquidation or movement of assets after investigators close in

## Join recipe

### Join 1: local IRS core

This join is already available in the repo:

```sql
SELECT
  o.ein,
  o.name,
  o.street,
  o.city,
  o.state,
  o.zip,
  o.subsection,
  o.ntee_cd,
  o.ruling,
  f.object_id,
  f.return_type,
  f.tax_period,
  d.tax_year,
  d.total_revenue,
  d.total_expenses,
  d.total_assets_eoy,
  d.total_liabilities_eoy,
  d.net_assets_eoy,
  d.website,
  d.mission
FROM organizations o
JOIN filings f USING (ein)
LEFT JOIN filing_details d ON d.object_id = f.object_id
WHERE o.ein = '630598743'
ORDER BY f.tax_period DESC;
```

### Join 2: external identity crosswalk

Build one canonical identity row keyed by EIN, then attach external IDs:

```text
local IRS EIN        -> 630598743
SAM UEI              -> H2GRQHMF6A63
SAM CAGE             -> 3VAG6
USAspending DUNS     -> 078962198
USAspending recipient_id -> d6b3baaa-a86d-29ea-d036-a72ccefbd3d4-C
normalized address   -> 400 WASHINGTON AVE | MONTGOMERY | AL | 36104
```

Recommended matching order:

1. `EIN` when available
2. `UEI`
3. `DUNS`
4. exact normalized legal name + street + ZIP
5. alias name + exact address
6. manual review

### Join 3: DOJ adverse-event labels

DOJ pages often do not expose EINs. Use:

1. normalized entity name
2. city/state
3. address when available in court filings or organization pages
4. alias list from USAspending / SAM / local BMF
5. charge date and scheme window as event metadata

Suggested adverse-event table shape:

```sql
CREATE TABLE ext_doj_case_labels (
  entity_name TEXT,
  normalized_name TEXT,
  ein TEXT,
  uei TEXT,
  city TEXT,
  state TEXT,
  event_date TEXT,
  source_url TEXT,
  case_type TEXT,
  charges_json TEXT,
  allegations_json TEXT
);
```

### Join 4: federal spending context

Use `UEI`, `DUNS`, and normalized address to attach:

- USAspending recipient profile
- SAM registration metadata
- USAspending award counts
- USAspending award-by-agency and spending-over-time summaries

This is primarily a context join for SPLC, not the core misconduct join.

## Immediate takeaways

- This case shows why a 990-only model will miss some important nonprofit-fraud patterns.
- The strongest current risk features are not balance-sheet anomalies; they are `adverse-event`, `identity`, `counterparty`, and `disclosure-mismatch` features.
- SPLC is a useful case study specifically because the local filing history looks mostly normal while the DOJ allegations describe concealed use of funds and concealed counterparties.

## Minimal next ingestion targets

If this repo is extended beyond IRS data, the highest-value new sources for cases like this are:

- DOJ / USAO press releases and indictments
- SAM entity records and exclusions
- USAspending recipient and award summaries
- IRS TEOS / 990 PDFs or XML schedules beyond current header extraction
- state corporate registry records for shell or fictitious entities
- adverse-entity lists for extremist, sanctions, criminal, or exclusion screening
