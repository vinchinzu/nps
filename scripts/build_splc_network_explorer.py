#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "i990.sqlite"
OUT_DIR = ROOT / "reports" / "person_networks"
SEED_EIN = "630598743"
SEED_LABEL = "SOUTHERN POVERTY LAW CENTER INC"

ROLE_LABELS = {
    "officer_director": "officer/director",
    "related_org_officer": "related org officer",
    "signing_officer": "signing officer",
    "contractor": "contractor",
    "unknown": "unknown",
}


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def clean(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def org_label(row: sqlite3.Row | dict[str, Any] | None, fallback_ein: str) -> str:
    if not row:
        return fallback_ein
    return clean(row["org_name"] if "org_name" in row.keys() else row["name"]) or fallback_ein


def ordered_join(values: set[str], limit: int = 14) -> str:
    ordered = sorted(v for v in values if v)
    if len(ordered) <= limit:
        return "; ".join(ordered)
    return "; ".join(ordered[:limit]) + f"; +{len(ordered) - limit} more"


def year_range(years: set[int]) -> str | None:
    if not years:
        return None
    lo = min(years)
    hi = max(years)
    return str(lo) if lo == hi else f"{lo}-{hi}"


class Builder:
    def __init__(self, db_path: Path, seed_ein: str) -> None:
        self.db_path = db_path
        self.seed_ein = seed_ein
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA query_only = ON")
        self.conn.execute("PRAGMA busy_timeout = 10000")
        self.org_cache: dict[str, dict[str, Any]] = {}

    def close(self) -> None:
        self.conn.close()

    def fetch_org(self, ein: str) -> dict[str, Any]:
        if ein in self.org_cache:
            return self.org_cache[ein]
        row = self.conn.execute(
            """
            SELECT o.ein,
                   COALESCE(o.name, d.org_name) AS org_name,
                   COALESCE(o.state, d.state) AS state,
                   o.ntee_cd,
                   o.subsection
              FROM organizations o
              LEFT JOIN (
                    SELECT ein, org_name, state
                      FROM filing_details
                     WHERE ein=?
                     ORDER BY tax_year DESC
                     LIMIT 1
              ) d ON d.ein=o.ein
             WHERE o.ein=?
            """,
            (ein, ein),
        ).fetchone()
        if row is None:
            row = self.conn.execute(
                """
                SELECT ein, org_name, state, NULL AS ntee_cd, NULL AS subsection
                  FROM filing_details
                 WHERE ein=?
                 ORDER BY tax_year DESC
                 LIMIT 1
                """,
                (ein,),
            ).fetchone()
        data = dict(row) if row else {
            "ein": ein,
            "org_name": ein,
            "state": None,
            "ntee_cd": None,
            "subsection": None,
        }
        self.org_cache[ein] = data
        return data

    def seed_people(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT p.name_norm, p.name, p.person_role, p.title, p.tax_year,
                   p.reportable_comp, p.hours_per_week, p.object_id
              FROM filing_persons p
             WHERE p.ein=?
               AND p.person_role <> 'preparer'
               AND p.name_norm IS NOT NULL
               AND p.name_norm <> ''
             ORDER BY p.name_norm, p.tax_year
            """,
            (self.seed_ein,),
        ).fetchall()
        return rows_to_dicts(rows)

    def person_matches(self, name_norm: str, exclude_ein: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [name_norm]
        extra = ""
        if exclude_ein:
            extra = "AND p.ein<>?"
            params.append(exclude_ein)
        rows = self.conn.execute(
            f"""
            SELECT p.name_norm, p.name, p.ein, p.tax_year, p.person_role, p.title,
                   p.reportable_comp, p.hours_per_week, p.object_id,
                   COALESCE(o.name, d.org_name) AS org_name,
                   COALESCE(o.state, d.state) AS state,
                   o.ntee_cd,
                   o.subsection
              FROM filing_persons p
              LEFT JOIN filing_details d USING(object_id)
              LEFT JOIN organizations o ON o.ein=p.ein
             WHERE p.name_norm=?
               {extra}
               AND p.person_role <> 'preparer'
             ORDER BY p.ein, p.tax_year
            """,
            params,
        ).fetchall()
        return rows_to_dicts(rows)

    def org_people(self, ein: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT p.name_norm, p.name, p.ein, p.tax_year, p.person_role, p.title,
                   p.reportable_comp, p.hours_per_week, p.object_id
              FROM filing_persons p
             WHERE p.ein=?
               AND p.person_role <> 'preparer'
               AND p.name_norm IS NOT NULL
               AND p.name_norm <> ''
             ORDER BY p.name_norm, p.tax_year
            """,
            (ein,),
        ).fetchall()
        return rows_to_dicts(rows)

    def build(self) -> dict[str, Any]:
        seed_org = self.fetch_org(self.seed_ein)
        seed_person_rows = self.seed_people()
        seed_names = sorted({r["name_norm"] for r in seed_person_rows if r["name_norm"]})

        nodes: dict[str, dict[str, Any]] = {}
        edge_map: dict[tuple[str, str, str], dict[str, Any]] = {}

        def node_priority(kind: str) -> int:
            return {
                "seed_org": 0,
                "seed_person": 1,
                "first_org": 2,
                "connected_person": 3,
                "second_org": 4,
            }[kind]

        def add_org(ein: str, kind: str) -> dict[str, Any]:
            oid = f"org:{ein}"
            meta = self.fetch_org(ein)
            existing = nodes.get(oid)
            if existing and node_priority(existing["kind"]) <= node_priority(kind):
                return existing
            node = {
                "id": oid,
                "type": "org",
                "kind": kind,
                "label": clean(meta.get("org_name")) or ein,
                "ein": ein,
                "state": clean(meta.get("state")),
                "ntee_cd": clean(meta.get("ntee_cd")),
                "subsection": clean(meta.get("subsection")),
                "years": set(),
                "roles": Counter(),
                "titles": set(),
                "shared_names": set(),
                "edge_rows": 0,
            }
            if existing:
                node["years"] = existing.get("years", set())
                node["roles"] = existing.get("roles", Counter())
                node["titles"] = existing.get("titles", set())
                node["shared_names"] = existing.get("shared_names", set())
                node["edge_rows"] = existing.get("edge_rows", 0)
            nodes[oid] = node
            return node

        def add_person(name_norm: str, name: str | None, kind: str) -> dict[str, Any]:
            pid = f"person:{name_norm}"
            existing = nodes.get(pid)
            if existing and node_priority(existing["kind"]) <= node_priority(kind):
                if name and name not in existing["aliases"]:
                    existing["aliases"].append(name)
                return existing
            node = {
                "id": pid,
                "type": "person",
                "kind": kind,
                "label": name_norm,
                "name_norm": name_norm,
                "aliases": [name] if name else [],
                "years": set(),
                "roles": Counter(),
                "titles": set(),
                "orgs": set(),
                "max_reportable_comp": None,
                "max_hours_per_week": None,
                "edge_rows": 0,
            }
            if existing:
                node["aliases"] = existing.get("aliases", [])
                node["years"] = existing.get("years", set())
                node["roles"] = existing.get("roles", Counter())
                node["titles"] = existing.get("titles", set())
                node["orgs"] = existing.get("orgs", set())
                node["max_reportable_comp"] = existing.get("max_reportable_comp")
                node["max_hours_per_week"] = existing.get("max_hours_per_week")
                node["edge_rows"] = existing.get("edge_rows", 0)
            nodes[pid] = node
            return node

        def add_edge(
            source: str,
            target: str,
            kind: str,
            rows: list[dict[str, Any]],
            layer: int,
        ) -> None:
            key = (source, target, kind)
            edge = edge_map.setdefault(
                key,
                {
                    "source": source,
                    "target": target,
                    "kind": kind,
                    "layer": layer,
                    "rows": 0,
                    "years": set(),
                    "roles": Counter(),
                    "titles": set(),
                    "objects": set(),
                },
            )
            edge["rows"] += len(rows)
            for row in rows:
                if row.get("tax_year") is not None:
                    edge["years"].add(int(row["tax_year"]))
                role = clean(row.get("person_role"))
                if role:
                    edge["roles"][role] += 1
                title = clean(row.get("title"))
                if title:
                    edge["titles"].add(title)
                obj = clean(row.get("object_id"))
                if obj and len(edge["objects"]) < 10:
                    edge["objects"].add(obj)

        add_org(self.seed_ein, "seed_org")

        seed_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in seed_person_rows:
            seed_by_name[row["name_norm"]].append(row)
            person = add_person(row["name_norm"], row.get("name"), "seed_person")
            person["edge_rows"] += 1
            if row.get("tax_year") is not None:
                person["years"].add(int(row["tax_year"]))
            if row.get("person_role"):
                person["roles"][row["person_role"]] += 1
            if row.get("title"):
                person["titles"].add(row["title"])
            person["orgs"].add(self.seed_ein)
            person["max_reportable_comp"] = max_optional(person["max_reportable_comp"], row.get("reportable_comp"))
            person["max_hours_per_week"] = max_optional(person["max_hours_per_week"], row.get("hours_per_week"))

        for name_norm, rows in seed_by_name.items():
            add_edge(f"org:{self.seed_ein}", f"person:{name_norm}", "seed_membership", rows, 0)

        first_edges_by_name_org: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for name_norm in seed_names:
            for row in self.person_matches(name_norm, exclude_ein=self.seed_ein):
                first_edges_by_name_org[(name_norm, row["ein"])].append(row)

        first_orgs = sorted({ein for _, ein in first_edges_by_name_org})
        for (name_norm, ein), rows in first_edges_by_name_org.items():
            org = add_org(ein, "first_org")
            org["shared_names"].add(name_norm)
            org["edge_rows"] += len(rows)
            for row in rows:
                if row.get("tax_year") is not None:
                    org["years"].add(int(row["tax_year"]))
                if row.get("person_role"):
                    org["roles"][row["person_role"]] += 1
                if row.get("title"):
                    org["titles"].add(row["title"])
            add_person(name_norm, rows[0].get("name"), "seed_person")
            add_edge(f"person:{name_norm}", f"org:{ein}", "first_degree", rows, 1)

        org_person_rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        second_names: set[str] = set()
        for ein in first_orgs:
            for row in self.org_people(ein):
                name_norm = row["name_norm"]
                second_names.add(name_norm)
                org_person_rows[(ein, name_norm)].append(row)
                person_kind = "seed_person" if name_norm in seed_names else "connected_person"
                person = add_person(name_norm, row.get("name"), person_kind)
                person["edge_rows"] += 1
                person["orgs"].add(ein)
                if row.get("tax_year") is not None:
                    person["years"].add(int(row["tax_year"]))
                if row.get("person_role"):
                    person["roles"][row["person_role"]] += 1
                if row.get("title"):
                    person["titles"].add(row["title"])
                person["max_reportable_comp"] = max_optional(person["max_reportable_comp"], row.get("reportable_comp"))
                person["max_hours_per_week"] = max_optional(person["max_hours_per_week"], row.get("hours_per_week"))

        for (ein, name_norm), rows in org_person_rows.items():
            add_edge(f"org:{ein}", f"person:{name_norm}", "connected_officer", rows, 2)

        first_org_set = set(first_orgs)
        second_edges_by_name_org: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for name_norm in sorted(second_names):
            for row in self.person_matches(name_norm, exclude_ein=self.seed_ein):
                ein = row["ein"]
                if ein in first_org_set:
                    continue
                second_edges_by_name_org[(name_norm, ein)].append(row)

        for (name_norm, ein), rows in second_edges_by_name_org.items():
            add_org(ein, "second_org")
            add_person(name_norm, rows[0].get("name"), "connected_person")
            add_edge(f"person:{name_norm}", f"org:{ein}", "second_degree", rows, 3)

        graph_nodes = [serialize_node(node) for node in nodes.values()]
        graph_edges = [serialize_edge(edge) for edge in edge_map.values()]
        graph_nodes.sort(key=lambda n: (node_priority(n["kind"]), n["label"]))
        graph_edges.sort(key=lambda e: (e["layer"], e["source"], e["target"]))

        metrics = {
            "seed_ein": self.seed_ein,
            "seed_name": clean(seed_org.get("org_name")) or SEED_LABEL,
            "seed_person_rows": len(seed_person_rows),
            "seed_people": len(seed_names),
            "first_degree_orgs": len(first_orgs),
            "connected_officers": len(second_names),
            "second_degree_orgs": sum(1 for n in graph_nodes if n["kind"] == "second_org"),
            "nodes": len(graph_nodes),
            "edges": len(graph_edges),
            "preparers_excluded": True,
        }

        return {
            "metadata": metrics,
            "nodes": graph_nodes,
            "edges": graph_edges,
        }


def max_optional(current: Any, value: Any) -> float | int | None:
    if value in (None, ""):
        return current
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return current
    if current is None or numeric > current:
        if isinstance(value, int):
            return int(numeric)
        return numeric
    return current


def serialize_node(node: dict[str, Any]) -> dict[str, Any]:
    result = dict(node)
    result["years"] = sorted(result.pop("years", set()))
    result["year_range"] = year_range(set(result["years"]))
    result["roles"] = dict(result.pop("roles", Counter()).most_common())
    result["role_summary"] = "; ".join(
        f"{ROLE_LABELS.get(k, k)}: {v}" for k, v in result["roles"].items()
    )
    result["titles"] = ordered_join(result.pop("titles", set()))
    if "shared_names" in result:
        names = result.pop("shared_names", set())
        result["shared_people"] = len(names)
        result["shared_names"] = ordered_join(names, limit=20)
    if "orgs" in result:
        orgs = result.pop("orgs", set())
        result["org_count"] = len(orgs)
        result["orgs_sample"] = ordered_join(orgs)
    aliases = result.get("aliases")
    if aliases:
        result["aliases"] = sorted(set(aliases))[:10]
    return result


def serialize_edge(edge: dict[str, Any]) -> dict[str, Any]:
    result = dict(edge)
    result["years"] = sorted(result.pop("years", set()))
    result["year_range"] = year_range(set(result["years"]))
    result["roles"] = dict(result.pop("roles", Counter()).most_common())
    result["role_summary"] = "; ".join(
        f"{ROLE_LABELS.get(k, k)}: {v}" for k, v in result["roles"].items()
    )
    result["titles"] = ordered_join(result.pop("titles", set()))
    result["objects"] = sorted(result.pop("objects", set()))
    return result


HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SPLC Persons Network Explorer</title>
<style>
:root {
  --bg: #f6f7f2;
  --ink: #1f2933;
  --muted: #5d6975;
  --line: #d7d9cf;
  --panel: #ffffff;
  --seed-org: #b42318;
  --seed-person: #0f766e;
  --first-org: #2563eb;
  --connected-person: #b7791f;
  --second-org: #6d5bd0;
}
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; }
body {
  background: var(--bg);
  color: var(--ink);
  font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
.app {
  display: grid;
  grid-template-columns: 320px minmax(0, 1fr) 360px;
  height: 100vh;
  min-height: 620px;
}
aside, .details {
  background: var(--panel);
  border-color: var(--line);
  overflow: auto;
}
aside { border-right: 1px solid var(--line); }
.details { border-left: 1px solid var(--line); }
.pad { padding: 16px; }
h1 {
  margin: 0 0 8px;
  font-size: 19px;
  line-height: 1.2;
  letter-spacing: 0;
}
h2 {
  margin: 18px 0 8px;
  font-size: 13px;
  text-transform: uppercase;
  letter-spacing: .08em;
  color: var(--muted);
}
.sub { color: var(--muted); margin: 0 0 12px; }
.metrics {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  margin: 14px 0;
}
.metric {
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 8px;
  background: #fbfbf8;
}
.metric b { display: block; font-size: 20px; line-height: 1.1; }
.metric span { color: var(--muted); font-size: 12px; }
label {
  display: flex;
  gap: 8px;
  align-items: center;
  margin: 10px 0;
  color: var(--ink);
}
input[type="checkbox"] { width: 16px; height: 16px; }
input[type="range"] { width: 100%; }
input[type="search"], select {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 8px 10px;
  font: inherit;
  background: white;
}
.field {
  display: grid;
  gap: 5px;
  margin: 10px 0;
}
.field span {
  color: var(--muted);
  font-size: 12px;
}
button {
  border: 1px solid var(--line);
  background: #ffffff;
  border-radius: 6px;
  color: var(--ink);
  padding: 8px 10px;
  font: inherit;
  cursor: pointer;
}
button:hover { background: #f1f3ed; }
.buttons { display: flex; gap: 8px; flex-wrap: wrap; }
.legend { display: grid; gap: 7px; margin-top: 8px; }
.legend-row { display: flex; align-items: center; gap: 8px; color: var(--muted); }
.dot { width: 11px; height: 11px; border-radius: 50%; flex: 0 0 auto; }
.stage { position: relative; min-width: 0; overflow: hidden; }
canvas { display: block; width: 100%; height: 100%; cursor: grab; }
canvas.dragging { cursor: grabbing; }
.hud {
  position: absolute;
  left: 12px;
  bottom: 12px;
  background: rgba(255,255,255,.92);
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 7px 9px;
  color: var(--muted);
  font-size: 12px;
}
.tip {
  position: absolute;
  display: none;
  max-width: 320px;
  pointer-events: none;
  background: #ffffff;
  border: 1px solid var(--line);
  border-radius: 6px;
  box-shadow: 0 10px 32px rgba(31, 41, 51, .14);
  padding: 9px 10px;
  color: var(--ink);
  font-size: 12px;
}
.detail-title { font-size: 18px; font-weight: 700; line-height: 1.2; margin-bottom: 6px; }
.kv {
  display: grid;
  grid-template-columns: 112px minmax(0, 1fr);
  gap: 5px 10px;
  padding: 8px 0;
  border-bottom: 1px solid #eceee6;
}
.k { color: var(--muted); }
.v { min-width: 0; overflow-wrap: anywhere; }
.list { margin: 8px 0 0; padding-left: 18px; }
.small { color: var(--muted); font-size: 12px; }
.warn {
  border-left: 3px solid #b7791f;
  padding: 8px 10px;
  background: #fff8e6;
  color: #5f4b16;
  margin-top: 12px;
}
@media (max-width: 980px) {
  .app { grid-template-columns: 1fr; grid-template-rows: auto 70vh auto; height: auto; }
  aside, .details { border: 0; border-bottom: 1px solid var(--line); }
}
</style>
</head>
<body>
<div class="app">
  <aside>
    <div class="pad">
      <h1>SPLC Persons Network Explorer</h1>
      <p class="sub">Name-normalized Form 990 person links. Preparers are excluded.</p>
      <div class="metrics" id="metrics"></div>

      <h2>Controls</h2>
      <div class="field">
        <span>Layout</span>
        <select id="layoutMode">
          <option value="ring">Ring by relationship</option>
          <option value="kk">Weighted KK-style force</option>
        </select>
      </div>
      <div class="small" id="layoutState">Ring layout active.</div>
      <label><input id="secondToggle" type="checkbox"> Show second-degree expansion</label>
      <label><input id="labelsToggle" type="checkbox"> Show labels</label>
      <label><input id="edgesToggle" type="checkbox" checked> Show edges</label>
      <label><input id="weightToggle" type="checkbox" checked> Weight people by connectedness</label>
      <div class="small">Minimum shared SPLC people for first-degree nonprofits: <b id="sharedValue">1</b></div>
      <input id="sharedRange" type="range" min="1" max="9" value="1">
      <div style="height:10px"></div>
      <input id="search" type="search" placeholder="Search name, EIN, organization, state, NTEE">
      <div style="height:10px"></div>
      <div class="buttons">
        <button id="fitBtn">Fit</button>
        <button id="centerBtn">Center SPLC</button>
        <button id="relayoutBtn">Re-layout</button>
        <button id="jitterBtn">Jitter</button>
        <button id="clearBtn">Clear</button>
      </div>

      <h2>Legend</h2>
      <div class="legend">
        <div class="legend-row"><span class="dot" style="background:var(--seed-org)"></span>SPLC</div>
        <div class="legend-row"><span class="dot" style="background:var(--seed-person)"></span>SPLC person</div>
        <div class="legend-row"><span class="dot" style="background:var(--first-org)"></span>First-degree nonprofit</div>
        <div class="legend-row"><span class="dot" style="background:var(--connected-person)"></span>Connected officer/person</div>
        <div class="legend-row"><span class="dot" style="background:var(--second-org)"></span>Second-degree nonprofit</div>
      </div>
      <div class="warn small">Identity matching is by normalized name only. Treat common-name edges as leads for review, not confirmed identities.</div>
    </div>
  </aside>

  <main class="stage" id="stage">
    <canvas id="graph"></canvas>
    <div class="hud" id="hud"></div>
    <div class="tip" id="tip"></div>
  </main>

  <section class="details">
    <div class="pad" id="details"></div>
  </section>
</div>

<script id="graph-data" type="application/json">__GRAPH_JSON__</script>
<script>
const graph = JSON.parse(document.getElementById('graph-data').textContent);
const nodes = graph.nodes.map((n) => ({...n}));
const edges = graph.edges.map((e) => ({...e}));
const byId = new Map(nodes.map((n) => [n.id, n]));
const edgesBySource = new Map();
const edgesByTarget = new Map();
for (const e of edges) {
  if (!edgesBySource.has(e.source)) edgesBySource.set(e.source, []);
  if (!edgesByTarget.has(e.target)) edgesByTarget.set(e.target, []);
  edgesBySource.get(e.source).push(e);
  edgesByTarget.get(e.target).push(e);
}
const stage = document.getElementById('stage');
const canvas = document.getElementById('graph');
const ctx = canvas.getContext('2d');
const tip = document.getElementById('tip');
const hud = document.getElementById('hud');
const details = document.getElementById('details');
const secondToggle = document.getElementById('secondToggle');
const labelsToggle = document.getElementById('labelsToggle');
const edgesToggle = document.getElementById('edgesToggle');
const weightToggle = document.getElementById('weightToggle');
const layoutMode = document.getElementById('layoutMode');
const layoutState = document.getElementById('layoutState');
const sharedRange = document.getElementById('sharedRange');
const sharedValue = document.getElementById('sharedValue');
const search = document.getElementById('search');
let dpr = window.devicePixelRatio || 1;
let transform = {x: 0, y: 0, k: 1};
let dragging = false;
let nodeDrag = null;
let dragStart = null;
let dragMoved = false;
let selected = null;
let hovered = null;
let visibleNodeIds = new Set();
let visibleEdges = [];
let labelSalt = 0;

const colors = {
  seed_org: '#b42318',
  seed_person: '#0f766e',
  first_org: '#2563eb',
  connected_person: '#b7791f',
  second_org: '#6d5bd0'
};

function metric(label, value) {
  return `<div class="metric"><b>${value.toLocaleString()}</b><span>${label}</span></div>`;
}

document.getElementById('metrics').innerHTML = [
  metric('SPLC people', graph.metadata.seed_people),
  metric('first-degree orgs', graph.metadata.first_degree_orgs),
  metric('connected people', graph.metadata.connected_officers),
  metric('second-degree orgs', graph.metadata.second_degree_orgs)
].join('');

function enrich() {
  for (const n of nodes) {
    n.degree = 0;
    n.searchText = [
      n.label, n.ein, n.state, n.ntee_cd, n.subsection, n.name_norm,
      n.shared_names, n.role_summary, n.titles, ...(n.aliases || [])
    ].filter(Boolean).join(' ').toLowerCase();
  }
  for (const e of edges) {
    const a = byId.get(e.source);
    const b = byId.get(e.target);
    if (a) a.degree += 1;
    if (b) b.degree += 1;
  }
}

function weightedAngle(parts) {
  if (!parts.length) return null;
  let x = 0, y = 0;
  for (const [angle, weight] of parts) {
    x += Math.cos(angle) * weight;
    y += Math.sin(angle) * weight;
  }
  return Math.atan2(y, x);
}

function layoutRing() {
  const firstOrgs = nodes.filter((n) => n.kind === 'first_org')
    .sort((a, b) => (b.shared_people || 0) - (a.shared_people || 0) || a.label.localeCompare(b.label));
  const seedPeople = nodes.filter((n) => n.kind === 'seed_person')
    .sort((a, b) => a.label.localeCompare(b.label));
  const connectedPeople = nodes.filter((n) => n.kind === 'connected_person')
    .sort((a, b) => b.degree - a.degree || a.label.localeCompare(b.label));
  const secondOrgs = nodes.filter((n) => n.kind === 'second_org')
    .sort((a, b) => b.degree - a.degree || a.label.localeCompare(b.label));

  const assignRing = (arr, radius, start=-Math.PI / 2) => {
    arr.forEach((n, i) => {
      const angle = start + (Math.PI * 2 * i / Math.max(1, arr.length));
      n.angle = angle;
      n.x = Math.cos(angle) * radius;
      n.y = Math.sin(angle) * radius;
    });
  };

  for (const n of nodes) {
    if (n.kind === 'seed_org') {
      n.x = 0; n.y = 0; n.angle = 0;
    }
  }
  assignRing(firstOrgs, 420);

  for (const p of seedPeople) {
    const parts = (edgesBySource.get(p.id) || [])
      .filter((e) => e.kind === 'first_degree')
      .map((e) => [byId.get(e.target)?.angle, Math.max(1, e.rows || 1)])
      .filter(([a]) => Number.isFinite(a));
    p.angle = weightedAngle(parts);
  }
  const missingSeed = seedPeople.filter((n) => n.angle === null || n.angle === undefined);
  assignRing(missingSeed, 220);
  for (const p of seedPeople) {
    p.x = Math.cos(p.angle) * 220;
    p.y = Math.sin(p.angle) * 220;
  }

  for (const p of connectedPeople) {
    const parts = (edgesByTarget.get(p.id) || [])
      .filter((e) => e.kind === 'connected_officer')
      .map((e) => [byId.get(e.source)?.angle, Math.max(1, e.rows || 1)])
      .filter(([a]) => Number.isFinite(a));
    p.angle = weightedAngle(parts) ?? 0;
    const jitter = ((hashCode(p.id) % 120) - 60) / 60;
    p.x = Math.cos(p.angle + jitter * 0.05) * (650 + (hashCode(p.id) % 80));
    p.y = Math.sin(p.angle + jitter * 0.05) * (650 + (hashCode(p.id) % 80));
  }

  for (const o of secondOrgs) {
    const parts = (edgesByTarget.get(o.id) || [])
      .filter((e) => e.kind === 'second_degree')
      .map((e) => [byId.get(e.source)?.angle, Math.max(1, e.rows || 1)])
      .filter(([a]) => Number.isFinite(a));
    o.angle = weightedAngle(parts) ?? 0;
    const jitter = ((hashCode(o.id) % 160) - 80) / 80;
    o.x = Math.cos(o.angle + jitter * 0.04) * (900 + (hashCode(o.id) % 110));
    o.y = Math.sin(o.angle + jitter * 0.04) * (900 + (hashCode(o.id) % 110));
  }
  layoutState.textContent = 'Ring layout active.';
}

function applyCurrentLayout(fitAfter=false) {
  if (layoutMode.value === 'kk') {
    layoutKk();
  } else {
    layoutRing();
  }
  if (fitAfter) fit();
  else draw();
}

function layoutKk() {
  const visible = nodes.filter((n) => visibleNodeIds.has(n.id));
  const maxForceNodes = secondToggle.checked ? 900 : 700;
  if (visible.length > maxForceNodes) {
    weightedScatterLayout();
    layoutState.textContent = `Weighted scatter fallback for ${visible.length.toLocaleString()} visible nodes. Raise filters for force layout.`;
    return;
  }

  const activeIds = new Set(visible.map((n) => n.id));
  const simEdges = visibleEdges.filter((e) => activeIds.has(e.source) && activeIds.has(e.target));
  for (const n of visible) {
    n.vx = 0;
    n.vy = 0;
    if (!Number.isFinite(n.x) || !Number.isFinite(n.y)) {
      const angle = (hashCode(n.id) % 360) * Math.PI / 180;
      n.x = Math.cos(angle) * 260;
      n.y = Math.sin(angle) * 260;
    }
  }

  const charge = secondToggle.checked ? 950 : 1450;
  const iterations = secondToggle.checked ? 130 : 180;
  for (let iter = 0; iter < iterations; iter++) {
    const alpha = 1 - iter / iterations;
    for (let i = 0; i < visible.length; i++) {
      const a = visible[i];
      for (let j = i + 1; j < visible.length; j++) {
        const b = visible[j];
        let dx = b.x - a.x;
        let dy = b.y - a.y;
        let d2 = dx * dx + dy * dy + 18;
        const d = Math.sqrt(d2);
        dx /= d;
        dy /= d;
        const strength = charge * alpha / d2;
        a.vx -= dx * strength;
        a.vy -= dy * strength;
        b.vx += dx * strength;
        b.vy += dy * strength;
      }
    }

    for (const e of simEdges) {
      const a = byId.get(e.source);
      const b = byId.get(e.target);
      if (!a || !b) continue;
      let dx = b.x - a.x;
      let dy = b.y - a.y;
      let d = Math.sqrt(dx * dx + dy * dy) || 1;
      dx /= d;
      dy /= d;
      const ideal = e.kind === 'seed_membership' ? 95 : e.kind === 'first_degree' ? 165 : e.kind === 'connected_officer' ? 125 : 145;
      const strength = (0.012 + Math.min(0.035, Math.sqrt(e.rows || 1) * 0.003)) * alpha;
      const pull = (d - ideal) * strength;
      a.vx += dx * pull;
      a.vy += dy * pull;
      b.vx -= dx * pull;
      b.vy -= dy * pull;
    }

    for (const n of visible) {
      if (n.kind === 'seed_org') {
        n.x *= 0.82;
        n.y *= 0.82;
        n.vx = 0;
        n.vy = 0;
        continue;
      }
      const hubPull = n.kind === 'seed_person' ? 0.018 : n.kind === 'first_org' ? 0.008 : 0.003;
      n.vx -= n.x * hubPull * alpha;
      n.vy -= n.y * hubPull * alpha;
      const damp = 0.78;
      n.x += n.vx * damp;
      n.y += n.vy * damp;
      n.vx *= 0.48;
      n.vy *= 0.48;
    }
  }
  layoutState.textContent = `KK-style force layout active on ${visible.length.toLocaleString()} nodes.`;
}

function weightedScatterLayout() {
  const visible = nodes.filter((n) => visibleNodeIds.has(n.id));
  for (const n of visible) {
    if (n.kind === 'seed_org') {
      n.x = 0;
      n.y = 0;
      continue;
    }
    const base = n.kind === 'seed_person' ? 230 : n.kind === 'first_org' ? 430 : n.kind === 'connected_person' ? 720 : 980;
    const weight = Math.max(1, n.degree || n.shared_people || 1);
    const inward = Math.min(180, Math.log1p(weight) * 42);
    const angle = n.angle ?? ((hashCode(n.id) % 360) * Math.PI / 180);
    const jitter = ((hashCode(n.id + ':' + labelSalt) % 160) - 80) / 80;
    const dist = Math.max(90, base - inward + (hashCode(n.id) % 120));
    n.x = Math.cos(angle + jitter * 0.08) * dist;
    n.y = Math.sin(angle + jitter * 0.08) * dist;
  }
}

function hashCode(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  return Math.abs(h);
}

function radius(n) {
  if (!weightToggle.checked) {
    if (n.kind === 'seed_org') return 14;
    if (n.type === 'person') return 6;
    return n.kind === 'second_org' ? 4 : 7;
  }
  if (n.kind === 'seed_org') return 15;
  if (n.kind === 'seed_person') return 5 + Math.min(12, Math.sqrt(n.degree || 1) * 1.35);
  if (n.kind === 'first_org') return 5 + Math.min(12, Math.sqrt((n.shared_people || 1) * 5));
  if (n.kind === 'connected_person') return 3.5 + Math.min(7, Math.sqrt(n.degree || 1));
  return 3.5 + Math.min(6, Math.sqrt(n.degree || 1));
}

function updateVisibility() {
  const showSecond = secondToggle.checked;
  const minShared = Number(sharedRange.value);
  const q = search.value.trim().toLowerCase();
  sharedValue.textContent = minShared;
  visibleNodeIds = new Set();

  for (const n of nodes) {
    if (n.kind === 'seed_org' || n.kind === 'seed_person') visibleNodeIds.add(n.id);
    if (n.kind === 'first_org' && (n.shared_people || 0) >= minShared) visibleNodeIds.add(n.id);
  }
  if (showSecond) {
    let changed = true;
    while (changed) {
      changed = false;
      for (const e of edges) {
        if (e.kind === 'connected_officer' && visibleNodeIds.has(e.source) && !visibleNodeIds.has(e.target)) {
          visibleNodeIds.add(e.target); changed = true;
        }
        if (e.kind === 'second_degree' && visibleNodeIds.has(e.source) && !visibleNodeIds.has(e.target)) {
          visibleNodeIds.add(e.target); changed = true;
        }
      }
    }
  }
  visibleEdges = edges.filter((e) => {
    if (!visibleNodeIds.has(e.source) || !visibleNodeIds.has(e.target)) return false;
    if (!showSecond && e.layer > 1) return false;
    return true;
  });
  if (q) {
    for (const n of nodes) n.hit = visibleNodeIds.has(n.id) && n.searchText.includes(q);
  } else {
    for (const n of nodes) n.hit = false;
  }
  hud.textContent = `${visibleNodeIds.size.toLocaleString()} nodes / ${visibleEdges.length.toLocaleString()} edges visible`;
  draw();
}

function resize() {
  const rect = stage.getBoundingClientRect();
  dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  fit();
}

function worldToScreen(x, y) {
  return {x: x * transform.k + transform.x, y: y * transform.k + transform.y};
}

function screenToWorld(x, y) {
  return {x: (x - transform.x) / transform.k, y: (y - transform.y) / transform.k};
}

function draw() {
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  ctx.save();
  ctx.translate(transform.x, transform.y);
  ctx.scale(transform.k, transform.k);

  if (edgesToggle.checked) drawEdges();
  drawNodes();
  ctx.restore();
}

function drawEdges() {
  ctx.lineCap = 'round';
  for (const e of visibleEdges) {
    const a = byId.get(e.source);
    const b = byId.get(e.target);
    if (!a || !b) continue;
    const dim = e.layer > 1 ? 0.16 : 0.28;
    ctx.strokeStyle = `rgba(73, 83, 94, ${dim})`;
    ctx.lineWidth = Math.max(0.6, Math.min(3, Math.sqrt(e.rows || 1))) / transform.k;
    ctx.beginPath();
    ctx.moveTo(a.x, a.y);
    ctx.lineTo(b.x, b.y);
    ctx.stroke();
  }
}

function drawNodes() {
  const showLabels = labelsToggle.checked || transform.k > 1.45 || Boolean(search.value.trim());
  for (const n of nodes) {
    if (!visibleNodeIds.has(n.id)) continue;
    const r = radius(n);
    const isSelected = selected && selected.id === n.id;
    const isHover = hovered && hovered.id === n.id;
    ctx.beginPath();
    ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
    if (weightToggle.checked && n.type === 'person' && (n.degree || 0) >= 20) {
      ctx.save();
      ctx.beginPath();
      ctx.arc(n.x, n.y, r + 4 / transform.k, 0, Math.PI * 2);
      ctx.strokeStyle = 'rgba(17, 24, 39, .35)';
      ctx.lineWidth = 1.5 / transform.k;
      ctx.stroke();
      ctx.restore();
      ctx.beginPath();
      ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
    }
    ctx.fillStyle = colors[n.kind] || '#64748b';
    ctx.globalAlpha = n.hit || !search.value.trim() ? 1 : 0.32;
    ctx.fill();
    ctx.globalAlpha = 1;
    if (isSelected || isHover || n.hit) {
      ctx.lineWidth = (isSelected ? 3 : 2) / transform.k;
      ctx.strokeStyle = '#111827';
      ctx.stroke();
    } else if (n.type === 'org') {
      ctx.lineWidth = 1 / transform.k;
      ctx.strokeStyle = 'rgba(255,255,255,.8)';
      ctx.stroke();
    }
    if (showLabels && (n.kind !== 'second_org' || n.hit || transform.k > 1.9)) {
      ctx.font = `${Math.max(9, 12 / transform.k)}px ui-sans-serif, system-ui, sans-serif`;
      ctx.fillStyle = '#1f2933';
      ctx.globalAlpha = n.hit || n.kind !== 'second_org' ? 0.95 : 0.7;
      const lo = labelOffset(n, r);
      if (labelsToggle.checked && transform.k > .45) {
        ctx.strokeStyle = 'rgba(73,83,94,.18)';
        ctx.lineWidth = 1 / transform.k;
        ctx.beginPath();
        ctx.moveTo(n.x, n.y);
        ctx.lineTo(n.x + lo.x - 2 / transform.k, n.y + lo.y - 3 / transform.k);
        ctx.stroke();
      }
      ctx.fillText(shortLabel(n.label), n.x + lo.x, n.y + lo.y);
      ctx.globalAlpha = 1;
    }
  }
}

function labelOffset(n, r) {
  const h = hashCode(`${n.id}:${labelSalt}`);
  const angle = (h % 360) * Math.PI / 180;
  const dist = r + (8 + (h % 18)) / transform.k;
  return {
    x: Math.cos(angle) * dist,
    y: Math.sin(angle) * dist + 4 / transform.k
  };
}

function shortLabel(s) {
  if (!s) return '';
  return s.length > 34 ? s.slice(0, 31) + '...' : s;
}

function nearestNode(clientX, clientY) {
  const rect = canvas.getBoundingClientRect();
  const p = screenToWorld(clientX - rect.left, clientY - rect.top);
  let best = null;
  let bestD = Infinity;
  for (const n of nodes) {
    if (!visibleNodeIds.has(n.id)) continue;
    const dx = n.x - p.x;
    const dy = n.y - p.y;
    const d = Math.sqrt(dx * dx + dy * dy);
    const hit = radius(n) + 7 / transform.k;
    if (d < hit && d < bestD) {
      best = n; bestD = d;
    }
  }
  return best;
}

function showTip(n, x, y) {
  if (!n) {
    tip.style.display = 'none';
    return;
  }
  tip.innerHTML = `<b>${escapeHtml(n.label)}</b><br>${escapeHtml(kindLabel(n.kind))}${n.ein ? ' - EIN ' + escapeHtml(n.ein) : ''}${n.year_range ? '<br>Years: ' + escapeHtml(n.year_range) : ''}`;
  tip.style.left = Math.min(x + 14, stage.clientWidth - 330) + 'px';
  tip.style.top = Math.min(y + 14, stage.clientHeight - 100) + 'px';
  tip.style.display = 'block';
}

function showDetails(n) {
  if (!n) {
    details.innerHTML = `<div class="detail-title">SPLC Network</div>
      <p class="sub">Click a node to inspect roles, years, titles, shared people, and connected nonprofits.</p>
      ${detailKv('Seed', graph.metadata.seed_name + ' / ' + graph.metadata.seed_ein)}
      ${detailKv('Visible graph', `${visibleNodeIds.size.toLocaleString()} nodes, ${visibleEdges.length.toLocaleString()} edges`)}
      ${detailKv('Full graph', `${graph.metadata.nodes.toLocaleString()} nodes, ${graph.metadata.edges.toLocaleString()} edges`)}
      <h2>Notes</h2>
      <p class="small">Second-degree expansion starts from people found at first-degree nonprofits and follows those people to other nonprofits.</p>`;
    return;
  }
  const connected = visibleEdges
    .filter((e) => e.source === n.id || e.target === n.id)
    .slice(0, 80)
    .map((e) => {
      const other = byId.get(e.source === n.id ? e.target : e.source);
      return `<li><b>${escapeHtml(kindLabel(e.kind))}</b>: ${escapeHtml(other?.label || '')}${e.year_range ? ` (${escapeHtml(e.year_range)})` : ''}${e.role_summary ? ` - ${escapeHtml(e.role_summary)}` : ''}</li>`;
    }).join('');
  details.innerHTML = `<div class="detail-title">${escapeHtml(n.label)}</div>
    <p class="sub">${escapeHtml(kindLabel(n.kind))}</p>
    ${detailKv('Type', n.type)}
    ${n.ein ? detailKv('EIN', n.ein) : ''}
    ${n.state ? detailKv('State', n.state) : ''}
    ${n.ntee_cd ? detailKv('NTEE', n.ntee_cd) : ''}
    ${n.subsection ? detailKv('Subsection', n.subsection) : ''}
    ${n.year_range ? detailKv('Years', n.year_range) : ''}
    ${n.shared_people ? detailKv('Shared SPLC people', String(n.shared_people)) : ''}
    ${n.shared_names ? detailKv('Shared names', n.shared_names) : ''}
    ${n.org_count ? detailKv('Org count', String(n.org_count)) : ''}
    ${n.role_summary ? detailKv('Roles', n.role_summary) : ''}
    ${n.titles ? detailKv('Titles', n.titles) : ''}
    ${n.max_reportable_comp != null ? detailKv('Max reportable comp', formatMoney(n.max_reportable_comp)) : ''}
    ${n.max_hours_per_week != null ? detailKv('Max hours/week', String(n.max_hours_per_week)) : ''}
    <h2>Visible Connections</h2>
    <ul class="list">${connected || '<li class="small">No visible connections under the current filters.</li>'}</ul>`;
}

function detailKv(k, v) {
  return `<div class="kv"><div class="k">${escapeHtml(k)}</div><div class="v">${escapeHtml(String(v))}</div></div>`;
}

function kindLabel(kind) {
  return {
    seed_org: 'SPLC',
    seed_person: 'SPLC person',
    first_org: 'first-degree nonprofit',
    connected_person: 'connected officer/person',
    second_org: 'second-degree nonprofit',
    seed_membership: 'SPLC filing person',
    first_degree: 'shared person at nonprofit',
    connected_officer: 'officer/person at first-degree nonprofit',
    second_degree: 'other nonprofit through connected person'
  }[kind] || kind;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (m) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
}

function formatMoney(v) {
  return Number(v).toLocaleString(undefined, {style: 'currency', currency: 'USD', maximumFractionDigits: 0});
}

function fit() {
  const visible = nodes.filter((n) => visibleNodeIds.has(n.id));
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  if (!visible.length || !w || !h) return;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const n of visible) {
    minX = Math.min(minX, n.x);
    minY = Math.min(minY, n.y);
    maxX = Math.max(maxX, n.x);
    maxY = Math.max(maxY, n.y);
  }
  const pad = secondToggle.checked ? 80 : 45;
  const graphW = Math.max(1, maxX - minX + pad * 2);
  const graphH = Math.max(1, maxY - minY + pad * 2);
  const k = Math.min(w / graphW, h / graphH, secondToggle.checked ? 1.2 : 1.8);
  transform.k = k;
  transform.x = w / 2 - ((minX + maxX) / 2) * k;
  transform.y = h / 2 - ((minY + maxY) / 2) * k;
  draw();
}

function centerSeed() {
  transform.k = secondToggle.checked ? .7 : 1.15;
  transform.x = canvas.clientWidth / 2;
  transform.y = canvas.clientHeight / 2;
  draw();
}

canvas.addEventListener('mousedown', (ev) => {
  const hit = nearestNode(ev.clientX, ev.clientY);
  const rect = canvas.getBoundingClientRect();
  const world = screenToWorld(ev.clientX - rect.left, ev.clientY - rect.top);
  dragMoved = false;
  if (hit) {
    nodeDrag = {
      node: hit,
      startX: world.x,
      startY: world.y,
      nodeX: hit.x,
      nodeY: hit.y
    };
    selected = hit;
    showDetails(hit);
  } else {
    dragging = true;
    dragStart = {x: ev.clientX, y: ev.clientY, tx: transform.x, ty: transform.y};
  }
  canvas.classList.add('dragging');
});
window.addEventListener('mousemove', (ev) => {
  const rect = canvas.getBoundingClientRect();
  const localX = ev.clientX - rect.left;
  const localY = ev.clientY - rect.top;
  if (nodeDrag) {
    const world = screenToWorld(localX, localY);
    const dx = world.x - nodeDrag.startX;
    const dy = world.y - nodeDrag.startY;
    nodeDrag.node.x = nodeDrag.nodeX + dx;
    nodeDrag.node.y = nodeDrag.nodeY + dy;
    nodeDrag.node.pinned = true;
    dragMoved = Math.abs(dx) + Math.abs(dy) > 3 / transform.k;
    hovered = nodeDrag.node;
    draw();
    return;
  }
  if (dragging) {
    transform.x = dragStart.tx + ev.clientX - dragStart.x;
    transform.y = dragStart.ty + ev.clientY - dragStart.y;
    dragMoved = Math.abs(ev.clientX - dragStart.x) + Math.abs(ev.clientY - dragStart.y) > 3;
    draw();
    return;
  }
  hovered = nearestNode(ev.clientX, ev.clientY);
  showTip(hovered, localX, localY);
  draw();
});
window.addEventListener('mouseup', () => {
  dragging = false;
  nodeDrag = null;
  canvas.classList.remove('dragging');
});
canvas.addEventListener('click', (ev) => {
  if (dragMoved) {
    dragMoved = false;
    return;
  }
  const n = nearestNode(ev.clientX, ev.clientY);
  if (n) {
    selected = n;
    showDetails(n);
    draw();
  }
});
canvas.addEventListener('wheel', (ev) => {
  ev.preventDefault();
  const rect = canvas.getBoundingClientRect();
  const sx = ev.clientX - rect.left;
  const sy = ev.clientY - rect.top;
  const before = screenToWorld(sx, sy);
  const scale = Math.exp(-ev.deltaY * 0.001);
  transform.k = Math.max(0.08, Math.min(4, transform.k * scale));
  transform.x = sx - before.x * transform.k;
  transform.y = sy - before.y * transform.k;
  draw();
}, {passive: false});

for (const el of [secondToggle, labelsToggle, edgesToggle, weightToggle, sharedRange]) {
  el.addEventListener('input', () => {
    updateVisibility();
    if (el === secondToggle || el === sharedRange) {
      if (layoutMode.value === 'kk') applyCurrentLayout(false);
      fit();
    }
    showDetails(selected);
  });
}
layoutMode.addEventListener('input', () => {
  applyCurrentLayout(true);
  showDetails(selected);
});
search.addEventListener('input', () => {
  updateVisibility();
  const q = search.value.trim().toLowerCase();
  if (q) {
    const hit = nodes.find((n) => visibleNodeIds.has(n.id) && n.searchText.includes(q));
    if (hit) {
      selected = hit;
      showDetails(hit);
    }
  }
});
document.getElementById('fitBtn').addEventListener('click', fit);
document.getElementById('centerBtn').addEventListener('click', centerSeed);
document.getElementById('relayoutBtn').addEventListener('click', () => {
  applyCurrentLayout(true);
  showDetails(selected);
});
document.getElementById('jitterBtn').addEventListener('click', () => {
  labelSalt += 1;
  jitterVisibleNodes();
  draw();
});
document.getElementById('clearBtn').addEventListener('click', () => {
  search.value = '';
  selected = null;
  updateVisibility();
  showDetails(null);
});
window.addEventListener('resize', resize);

function jitterVisibleNodes() {
  for (const n of nodes) {
    if (!visibleNodeIds.has(n.id) || n.kind === 'seed_org') continue;
    const h = hashCode(`${n.id}:node:${labelSalt}`);
    const angle = (h % 360) * Math.PI / 180;
    const amp = n.type === 'person' ? 15 : 11;
    n.x += Math.cos(angle) * amp;
    n.y += Math.sin(angle) * amp;
  }
}

enrich();
layoutRing();
updateVisibility();
resize();
showDetails(null);
</script>
</body>
</html>
"""


def write_outputs(graph: dict[str, Any], output: Path, json_output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(graph, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    embedded = json.dumps(graph, separators=(",", ":"), ensure_ascii=False).replace("</", "<\\/")
    output.write_text(HTML_TEMPLATE.replace("__GRAPH_JSON__", embedded), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a standalone SPLC persons network explorer HTML report."
    )
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--seed-ein", default=SEED_EIN)
    parser.add_argument(
        "--output",
        type=Path,
        default=OUT_DIR / "splc_network_explorer.html",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=OUT_DIR / "splc_network_explorer_data.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    builder = Builder(args.db, args.seed_ein)
    try:
        graph = builder.build()
    finally:
        builder.close()
    write_outputs(graph, args.output, args.json_output)
    print(
        "wrote "
        f"{args.output} with {graph['metadata']['nodes']:,} nodes, "
        f"{graph['metadata']['edges']:,} edges"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
