# Nonprofit Fraud Case Sample

This folder contains a small structured sample of DOJ nonprofit-fraud cases enriched with:

- DOJ source metadata and downloaded local artifacts
- local IRS/BMF and parsed i990 joins when the match is conservative
- USAspending recipient joins when the recipient name resolves cleanly
- SAM entity joins when the legal business name resolves cleanly

## Files

- `nonprofit_fraud_cases_sample.json`: nested case records with DOJ, i990, USAspending, and SAM sections
- `nonprofit_fraud_cases_sample.csv`: flattened analyst-friendly export
- `nonprofit_fraud_cases_manifest.json`: generation metadata and artifact counts

## DOJ artifacts

Downloaded DOJ pages and linked documents are stored locally under:

- `data/external/doj_nonprofit_fraud_cases/`

Each case has its own subdirectory with:

- `source-*.html`: the DOJ or USAO press release pages
- downloaded `pdf` or `dl` artifacts when the source page exposed them

## Join policy

The sample uses a conservative join policy:

- `i990` joins require an exact normalized nonprofit-name match and, when the case location clearly names a state, matching state alignment
- `USAspending` joins require an exact normalized recipient-name match from recipient search results
- `SAM` joins require an exact normalized legal-business-name match

That means some real organizations remain unmatched in `i990` even when a looser heuristic could find a likely candidate. The sample prefers fewer false positives over higher recall.

## Useful fields

- `case_id`: stable case slug
- `event_date`: DOJ event date used for the sample row
- `scheme_tags`: normalized scheme-pattern tags
- `source_titles` / `source_urls`: DOJ source metadata
- `ein`, `i990_org_name`, `i990_state`, `i990_ntee_cd`: local IRS join outputs
- `usaspending_recipient_id`, `usaspending_uei`, `usaspending_duns`: federal spending identity fields
- `sam_uei`, `sam_cage`, `sam_registration_status`: SAM entity fields

## Caveats

- DOJ pages describe allegations, pleas, convictions, or sentencings depending on the case. They should not all be treated as the same outcome label.
- A DOJ case can name a person, a nonprofit, several nonprofits, or a mix. The sample keeps one primary entity name per row for ease of joining.
- Federal-spending and SAM matches are context joins, not proof that the fraud involved federal awards unless the DOJ source says so.
