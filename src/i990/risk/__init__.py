"""Risk-ranking pipeline for possible-money-laundering signals.

See docs/risk-signals.md for the spec. Each signal defined here has a
slug, weight, and a SQL query that emits rows of the form
(ein, tax_year, severity, evidence_json). The engine (engine.py) runs
them all, writes risk_hits, and rolls them up into risk_scores.
"""
