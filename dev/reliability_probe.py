"""Read-only probe (test branch only, never merged): why v2 KPI eurpln is not built by gold.publish, and the
measured rows of every full-rebuild model against its ceiling. No writes. At most 10 annotations."""

from __future__ import annotations

from collections import Counter, defaultdict
import json
import subprocess
import traceback

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build, changed, publish, publish_v2


def eurpln(cat) -> dict:
    out: dict = {}
    measures = publish.seed("kpi_measure")
    licensed = publish.licensed_sources(publish.seed("source"), publish.seed("licence"))
    entries = {e["kpi_key"]: e for e in publish_v2.v2_kpis(publish.seed("kpi"), publish.seed("kpi_v2_map"))}
    entry = entries["eurpln"]
    out["entry"] = {k: entry[k] for k in ("concept", "sources", "unplaced", "frequency")}
    raw = publish.gold(cat, "gold", f"metrics_{entry['concept']}")
    out["raw_rows"] = raw.num_rows
    out["raw_by_source_frequency"] = dict(
        Counter(f"{s}|{f}" for s, f in zip(raw["source_system"].to_pylist(), raw["frequency"].to_pylist(), strict=True))
    )
    gated = publish.licence_gated(raw, licensed, entry["concept"])
    out["gated_rows"] = gated.num_rows
    geo = publish_v2.geography_map(publish.seed("geography_v2"))
    rows = publish_v2.gold_rows(gated, entry, geo)
    out["gold_rows"] = len(rows)
    out["gold_rows_by"] = dict(Counter(f"{r['source']}|{r['measure']}|{r['frequency']}|{r['geography']}" for r in rows))
    out["breakdown_sample"] = sorted({str(r["breakdown"])[:80] for r in rows})[:6]
    aliases = {(a["source_system"], a["source_dimension"]): a["dimension_key"] for a in publish.seed("dimension_alias")}
    curated: dict[str, set[str]] = defaultdict(set)
    for m in publish.seed("kpi_member"):
        if m["v2_key"] == "eurpln":
            curated[m["dimension_key"]].add(m["member_key"])
    out["curated"] = {k: sorted(v) for k, v in curated.items()}
    cells, dims = publish_v2.kpi_cells(rows, curated, aliases)
    out["cells"] = len(cells)
    out["dims"] = dims
    out["cells_by"] = dict(Counter(f"{c['source']}|{c['measure']}|{c['frequency']}|{c['members']}" for c in cells))
    geo_order = {g["geography_key"]: int(g["sort"]) for g in publish.seed("geography_v2")}
    doc, problems = publish_v2.series_json(entry, cells, measures, geo_order)
    out["periods"] = len(doc["periods"])
    out["series"] = [{k: s.get(k) for k in ("source_key", "geography", "members", "frequency")} for s in doc["series"]][:6]
    out["problems"] = problems[:5]
    return out


def v2_tables(cat) -> dict:
    from pyiceberg.exceptions import NoSuchTableError

    missing = []
    entries = publish_v2.v2_kpis(publish.seed("kpi"), publish.seed("kpi_v2_map"))
    for concept in sorted({e["concept"] for e in entries}):
        try:
            cat.load_table(("gold", f"metrics_{concept}"))
        except NoSuchTableError:
            missing.append(concept)
    return {"candidates": len(entries), "missing_tables": missing}


def ceilings(cat) -> list[dict]:
    env_cmd = ["dbt", "parse", "--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse"]
    subprocess.run(env_cmd, check=True, capture_output=True)
    meta = json.loads((build.PROJECT / "target" / "manifest.json").read_text())
    rows_out = []
    for uid, node in sorted(meta["nodes"].items()):
        if not uid.startswith("model.") or node.get("config", {}).get("materialized") == "incremental":
            continue
        name = node["name"]
        ceiling = int(build.FULL_REBUILD[name]["rows"]) if name in build.FULL_REBUILD else None
        try:
            rows = build.table_rows(cat.load_table(changed.relation(node)))
        except Exception as exc:
            rows_out.append({"m": name, "c": ceiling, "err": type(exc).__name__})
            continue
        rows_out.append({"m": name, "r": rows, "c": ceiling})
    return rows_out


def main() -> int:
    cat = catalog()
    for title, fn in (("eurpln", eurpln), ("v2 tables", v2_tables)):
        try:
            annotate("notice", f"probe: {title}", fn(cat))
        except Exception:
            annotate("warning", f"probe: {title} failed", {"traceback": traceback.format_exc()[-2500:]})
    try:
        rows = ceilings(cat)
        near = [r for r in rows if "r" in r and (r["c"] is None or r["r"] > 0.67 * r["c"])]
        annotate("notice", "probe: ceilings over or near (rows > 0.67 x ceiling) or unlisted", near)
        annotate("notice", "probe: ceilings errors", [r for r in rows if "err" in r][:60])
        compact = [f"{r['m']}={r['r']}/{r['c']}" for r in rows if "r" in r]
        for i in range(0, min(len(compact), 500), 100):
            annotate("notice", f"probe: rows/ceiling {i}", compact[i : i + 100])
    except Exception:
        annotate("warning", "probe: ceilings failed", {"traceback": traceback.format_exc()[-2500:]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
