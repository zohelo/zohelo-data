"""Read-only proof for #3204 PR A: the changed-only plan with the unscoped rule (old) and block-scoped metrics (new).

Usage: python -m dev.plan_compare [--catalog]

Without --catalog the table states are simulated (every table built at T0 with its current fingerprint: a steady
state). With --catalog they are read from the production R2 Data Catalog (load_table only: no write, no data scan),
and each table's stored fingerprint is replaced by the current one, i.e. the state after the first build with the new
fingerprints; build times and snapshots are the real ones. Scenarios on top of that state:

  baseline           nothing simulated
  block:<model>      one Gold block's direct inputs changed after its build
  fact_edit          poc_fact_annual.sql edited (its manifest checksum changed)
  new_source         a new Gold block added to the project (table missing, facts' parents changed)
  coverage           gold_coverage.coverage changed (a non-block input of poc_dim_measure)

Each scenario prints the old and the new plan and checks the expectations of the #3204 spec; the exit code is 1 when
one fails. Also checks that no source_system maps to two blocks. Results: $DEV_OUTPUT_DIR/plan_compare.json.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import sys

from warehouse import changed

T0 = 1_700_000_000_000  # simulated build time
T1 = T0 + 3_600_000  # a change after it
BLOCKS_TRIED = ["poc_build_nbp", "poc_build_public_finance", "poc_build_eurostat", "poc_build_gtfs_krakow"]


def annotate(level: str, title: str, data: dict) -> None:
    try:
        from ingestion.common.report import annotate as report

        report(level, title, data)
    except Exception:  # local run without the report module's environment
        print(f"::{level} title={title}::{json.dumps(data, default=str)[:3000]}")


def plan_with(meta: dict, states: dict) -> dict:
    """Both plans for one table state: {(namespace, table): state or None}."""
    original = changed.table_state
    changed.table_state = lambda cat, namespace, table: copy.deepcopy(states.get((namespace, table)))
    try:
        return {"old": changed.plan(None, meta, scoped=False), "new": changed.plan(None, meta, scoped=True)}
    finally:
        changed.table_state = original


def relations(meta: dict) -> dict:
    """unique_id → (namespace, table) of every model, seed and source the plan reads."""
    graph = changed.models(meta)
    out = {uid: (m["namespace"], m["table"]) for uid, m in graph.items()}
    for uid in {p for m in graph.values() for p in m["parents"] if p.startswith("source.")}:
        out[uid] = changed.relation(meta["sources"][uid])
    return out


def steady(meta: dict, real: dict | None) -> dict:
    """Table states with current fingerprints: simulated at T0, or the real ones with the fingerprint replaced."""
    graph = changed.models(meta)
    out = {}
    for uid, rel in relations(meta).items():
        if real is not None:
            state = copy.deepcopy(real.get(rel))
            if state is not None and uid in graph and state["built_from_ms"] is not None:
                state["fingerprint"] = graph[uid]["fingerprint"]
        else:
            fp = graph[uid]["fingerprint"] if uid in graph else None
            state = {"snapshot_ms": T0 - 1, "built_from_ms": T0 if fp else None, "fingerprint": fp}
        out[rel] = state
    return out


def bump(states: dict, rel: tuple[str, str], when: int) -> None:
    state = states.get(rel)
    if state is None:
        return
    state["snapshot_ms"] = max(state["snapshot_ms"], when)
    if state["built_from_ms"] is not None:
        state["built_from_ms"] = max(state["built_from_ms"], when)


def later(states: dict) -> int:
    """A time after every build and snapshot in the state."""
    times = [v for s in states.values() if s for v in (s["snapshot_ms"], s["built_from_ms"] or 0)]
    return max([*times, T1]) + 1


def why(plan: dict) -> dict:
    """Rebuild reasons by kind, with the inputs named most often (the first three of each reason)."""
    kinds: dict[str, int] = {}
    named: dict[str, int] = {}
    for reason in plan["reasons"].values():
        kind, _, rest = reason.partition(": ")
        kinds[kind] = kinds.get(kind, 0) + 1
        for item in filter(None, rest.split(", ")):
            named[item] = named.get(item, 0) + 1
    return {"kinds": kinds, "inputs": dict(sorted(named.items(), key=lambda kv: -kv[1])[:8])}


def metrics(plan: dict) -> set[str]:
    return {name for name in plan["select"] if name.startswith(changed.METRICS_PREFIX)}


def summary(plans: dict) -> dict:
    old, new = plans["old"], plans["new"]
    return {
        "old_select": len(old["select"]),
        "new_select": len(new["select"]),
        "old_metrics": len(metrics(old)),
        "new_metrics": len(metrics(new)),
        "scoped_skipped": len(new["scoped_skipped"]),
        "new_metrics_selected": sorted(metrics(new))[:40],
        "non_metrics_differ": sorted((set(old["select"]) ^ set(new["select"])) - metrics(old) - metrics(new)),
    }


def main(argv: list[str]) -> int:
    use_catalog = "--catalog" in argv
    meta = changed.manifest()
    graph = changed.models(meta)
    by_name = {m["name"]: uid for uid, m in graph.items()}
    children: dict[str, set[str]] = {}
    for uid, model in graph.items():
        for p in model["parents"]:
            children.setdefault(p, set()).add(uid)
    scopes = {m["name"]: m["scope"] for m in graph.values() if m["name"].startswith(changed.METRICS_PREFIX)}
    all_metrics = set(scopes)
    unscoped = {n for n, s in scopes.items() if s is None}
    problems: list[str] = []
    report: dict = {"mode": "catalog" if use_catalog else "simulated", "metrics": len(all_metrics)}

    by_source = changed.block_sources(meta)
    twice = {s: sorted(b) for s, b in by_source.items() if len(b) > 1}
    if twice:
        problems.append(f"source_system under two blocks: {twice}")
    report["unscoped_metrics"] = sorted(unscoped)
    report["scope_inputs"] = sorted(
        {graph[i]["name"] if i in graph else i for s in scopes.values() if s for i in s["inputs"]}
    )
    report["scope_ancestors"] = sorted({graph[a]["name"] for s in scopes.values() if s for a in s["ancestors"]})

    real = None
    if use_catalog:
        from ingestion.common.r2 import catalog

        cat = catalog()
        real = {rel: changed.table_state(cat, *rel) for rel in set(relations(meta).values())}
        report["catalog_reads"] = len(real)
        report["tables_missing"] = sorted(".".join(r) for r, s in real.items() if s is None)[:20]
        # The plan as production would make it now, before any fingerprint is rewritten (both rules).
        today = plan_with(meta, real)
        report["production_now"] = {
            "old_select": len(today["old"]["select"]),
            "new_select": len(today["new"]["select"]),
            "stalled": today["new"]["stalled"],
            "new_why": why(today["new"]),
        }
    base = steady(meta, real)
    baseline = plan_with(meta, base)
    report["pending"] = {**summary(baseline), "new_why": why(baseline["new"])}
    if real is not None:
        # Settle the real state: the build that is pending now (both rules' selection) ran and recorded its
        # properties, so each scenario below shows only what its own change selects.
        when = later(base)
        rels = relations(meta)
        for name in set(baseline["old"]["select"]) | set(baseline["new"]["select"]):
            uid = by_name[name]
            base[rels[uid]] = {"snapshot_ms": when, "built_from_ms": when, "fingerprint": graph[uid]["fingerprint"]}
        baseline = plan_with(meta, base)
    report["baseline"] = summary(baseline)
    base_new = metrics(baseline["new"])
    if not metrics(baseline["new"]) <= metrics(baseline["old"]):
        problems.append("baseline: the new rule selects a metrics model the old one does not")

    scenarios: dict = {}
    for block in BLOCKS_TRIED:
        uid = by_name.get(block)
        if uid is None:
            problems.append(f"{block}: not in the project")
            continue
        states = copy.deepcopy(base)
        when = later(states)
        rels = relations(meta)
        # Only the inputs this block alone reads (its Silver tables): a shared one such as poc_map_geography also
        # feeds poc_dim_geography, a direct input of every metrics model, and rightly rebuilds them all.
        exclusive = [p for p in graph[uid]["parents"] if children.get(p, set()) == {uid}]
        for p in exclusive:
            bump(states, rels[p], when)
        plans = plan_with(meta, states)
        expected = {n for n, s in scopes.items() if s is None or uid in s["blocks"]}
        old_m, new_m = metrics(plans["old"]), metrics(plans["new"])
        name = f"block:{block}"
        scenarios[name] = {
            **summary(plans),
            "expected_metrics": len(expected),
            "inputs_changed": sorted(".".join(rels[p]) for p in exclusive),
        }
        if not exclusive:
            problems.append(f"{name}: no input read by this block alone to simulate a change")
        if block not in plans["new"]["select"]:
            problems.append(f"{name}: the block itself is not selected")
        if old_m != all_metrics - set(plans["old"]["waiting"]):
            problems.append(f"{name}: the old rule does not select every metrics model ({len(old_m)})")
        if not new_m <= old_m:
            problems.append(f"{name}: new rule selects metrics the old one does not")
        if (new_m - base_new) - expected:
            problems.append(f"{name}: metrics outside the block's scope: {sorted((new_m - base_new) - expected)[:10]}")
        if (expected & old_m) - new_m:
            problems.append(f"{name}: metrics of the block missing: {sorted((expected & old_m) - new_m)[:10]}")
        if set(plans["old"]["select"]) - old_m != set(plans["new"]["select"]) - new_m:
            problems.append(f"{name}: the non-metrics models differ between the rules")

    # An edit to poc_fact_annual.sql: every metrics model is rebuilt under both rules.
    edited = copy.deepcopy(meta)
    edited["nodes"][by_name["poc_fact_annual"]]["checksum"]["checksum"] = "edited-for-plan-compare"
    plans = plan_with(edited, base)
    scenarios["fact_edit"] = summary(plans)
    waits = set(plans["new"]["waiting"])
    if metrics(plans["new"]) != all_metrics - waits:
        problems.append(
            f"fact_edit: new rule selects {len(metrics(plans['new']))} of {len(all_metrics - waits)} metrics"
        )

    # A new source: a copy of an existing block as a new model, read by every model that reads the blocks.
    grown = copy.deepcopy(meta)
    template_uid = by_name["poc_build_nbp"]
    new_uid = template_uid.replace("poc_build_nbp", "poc_build_zz_new_source")
    node = copy.deepcopy(grown["nodes"][template_uid])
    node.update(unique_id=new_uid, name="poc_build_zz_new_source", alias="build_zz_new_source")
    node["checksum"] = {"name": "sha256", "checksum": "new-source"}
    grown["nodes"][new_uid] = node
    for other in grown["nodes"].values():
        deps = other.get("depends_on", {}).get("nodes", [])
        if template_uid in deps and other["name"] != "poc_build_nbp":
            deps.append(new_uid)
    for macro in grown["macros"].values():
        if macro["name"] == changed.LEGACY_BLOCKS:
            macro["macro_sql"] = macro["macro_sql"].replace(
                "poc_build_nbp:nbp", "poc_build_nbp:nbp\n    poc_build_zz_new_source:zz_new_source"
            )
    plans = plan_with(grown, base)  # the new block has no table: it waits; the facts' parents changed
    scenarios["new_source"] = summary(plans)
    scenarios["new_source"]["facts_reason"] = plans["new"]["reasons"].get("poc_fact_annual")
    if metrics(plans["new"]) - base_new:
        problems.append(f"new_source: new rule rebuilds metrics: {sorted(metrics(plans['new']) - base_new)[:10]}")
    if "poc_fact_annual" not in plans["new"]["select"]:
        problems.append("new_source: poc_fact_annual is not rebuilt")

    # gold_coverage.coverage (read by poc_dim_measure, not a block): every scoped metrics model is rebuilt.
    coverage = next((u for u in relations(meta) if u.endswith("gold_coverage.coverage")), None)
    if coverage:
        states = copy.deepcopy(base)
        bump(states, relations(meta)[coverage], later(states))
        plans = plan_with(meta, states)
        scenarios["coverage"] = summary(plans)
        waits = set(plans["new"]["waiting"])
        if metrics(plans["new"]) != all_metrics - waits:
            problems.append("coverage: a change of gold_coverage.coverage does not rebuild every metrics model")

    report["scenarios"] = scenarios
    report["problems"] = problems
    out = Path(os.environ.get("DEV_OUTPUT_DIR", "."))
    out.mkdir(parents=True, exist_ok=True)
    (out / "plan_compare.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    compact = {
        k: v
        for k, v in report.items()
        if k not in ("scenarios", "unscoped_metrics", "scope_inputs", "scope_ancestors", "tables_missing")
    }
    for part in ("pending", "baseline"):
        compact[part] = {k: v for k, v in compact[part].items() if k != "new_metrics_selected"}
    annotate("notice" if not problems else "error", "plan_compare: setup and baseline", compact)
    for name, data in scenarios.items():  # at most 7 more annotations
        annotate("notice", f"plan_compare: {name}", {k: v for k, v in data.items() if k != "new_metrics_selected"})
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
