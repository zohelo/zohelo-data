"""Data catalog: one machine-readable JSON of every table, column, metric and dimension.

Built after each ``silver_gold.build`` from dbt's own artifacts (target/manifest.json and
target/semantic_manifest.json, written by the build) and the dimension registry
(warehouse/dimensions/*.yaml); no data is queried. Published to the lakehouse bucket at
``_portal_catalog/v1/catalog.json`` for the portal and for AI agents (stable keys, plain
descriptions, lineage as upstream lists).
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import yaml

PROJECT = Path(__file__).resolve().parent
CATALOG_KEY = "_portal_catalog/v1/catalog.json"


def build() -> dict[str, Any]:
    manifest = json.loads((PROJECT / "target" / "manifest.json").read_text())
    semantic_path = PROJECT / "target" / "semantic_manifest.json"
    semantic = json.loads(semantic_path.read_text()) if semantic_path.exists() else {}
    nodes = manifest.get("nodes", {})
    tests_by_node: dict[str, list[str]] = {}
    for node in nodes.values():
        if node.get("resource_type") == "test":
            for parent in node.get("depends_on", {}).get("nodes", []):
                name = (node.get("test_metadata") or {}).get("name") or node["name"]
                column = node.get("column_name")
                tests_by_node.setdefault(parent, []).append(f"{name}({column})" if column else name)

    def table_name(node: dict) -> str:
        return f"{node.get('schema')}.{node.get('alias') or node.get('name')}"

    tables = []
    for uid, node in sorted(nodes.items()):
        if node.get("resource_type") != "model":
            continue
        tables.append(
            {
                "id": uid,
                "table": table_name(node),
                "layer": node.get("schema"),
                "description": (node.get("description") or "").strip(),
                "columns": [
                    {"name": c, "description": (v.get("description") or "").strip()}
                    for c, v in node.get("columns", {}).items()
                ],
                "upstream": node.get("depends_on", {}).get("nodes", []),
                "tests": sorted(tests_by_node.get(uid, [])),
                "path": node.get("original_file_path"),
            }
        )
    sources = []
    for uid, src in sorted(manifest.get("sources", {}).items()):
        sources.append(
            {
                "id": uid,
                "table": f"{src.get('schema')}.{src.get('name')}",
                "source_system": src.get("source_name"),
                "description": (src.get("description") or "").strip(),
                "meta": src.get("source_meta") or src.get("meta") or {},
            }
        )
    downstream: dict[str, list[str]] = {}
    for t in tables:
        for up in t["upstream"]:
            downstream.setdefault(up, []).append(t["id"])
    for item in tables + sources:
        item["downstream"] = sorted(downstream.get(item["id"], []))
    metrics = [
        {
            "name": m["name"],
            "label": m.get("label"),
            "description": m.get("description"),
            "type": m.get("type"),
            "type_params": m.get("type_params"),
            "filter": m.get("filter"),
        }
        for m in semantic.get("metrics", [])
    ]
    semantic_models = [
        {
            "name": s["name"],
            "description": s.get("description"),
            "model": (s.get("node_relation") or {}).get("relation_name") or s.get("node_relation"),
            "entities": [e["name"] for e in s.get("entities", [])],
            "dimensions": [d["name"] for d in s.get("dimensions", [])],
            "measures": [m["name"] for m in s.get("measures", [])],
        }
        for s in semantic.get("semantic_models", [])
    ]
    dimensions = []
    for path in sorted((PROJECT / "dimensions").glob("*.yaml")):
        spec = yaml.safe_load(path.read_text()) or {}
        dimensions.append(
            {
                "name": spec.get("name", path.stem),
                "kind": spec.get("kind"),
                "status": spec.get("status"),
                "description": (spec.get("description") or "").strip(),
                "grain": spec.get("grain"),
                "identifiers": spec.get("identifiers"),
                "used_by": sorted((spec.get("used_by") or {}).get("platform", {}) or {}),
                "registry": f"warehouse/dimensions/{path.name}",
            }
        )
    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "about": "zohelo-data catalog: Bronze sources, Silver/Gold tables with columns, tests and lineage; "
        "MetricFlow metrics; conformed dimension registry.",
        "counts": {
            "sources": len(sources),
            "tables": len(tables),
            "metrics": len(metrics),
            "dimensions": len(dimensions),
        },
        "sources": sources,
        "tables": tables,
        "semantic_models": semantic_models,
        "metrics": metrics,
        "dimensions": dimensions,
    }


def publish() -> dict[str, Any]:
    from ingestion.common.r2 import req, s3

    catalog = build()
    body = json.dumps(catalog, ensure_ascii=False, indent=1).encode()
    s3().put_object(
        Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"),
        Key=CATALOG_KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="no-cache",
    )
    return {"key": CATALOG_KEY, "bytes": len(body), **catalog["counts"]}


def manifest_object(semantic: dict, kpis: list[dict], generated_at: str) -> dict:
    """manifest.json object (docs/published.md): one metric per published KPI with a semantic model."""
    models = {}
    for model in semantic.get("semantic_models", []):
        alias = (model.get("node_relation") or {}).get("alias") or ""
        models[alias.removeprefix("poc_")] = model
    metrics = []
    for kpi in kpis:
        model = models.get(f"metrics_{kpi['kpi_key']}")
        if kpi.get("status") != "published" or model is None:
            continue
        agg_time = (model.get("defaults") or {}).get("agg_time_dimension")
        time_column = next(
            (d.get("expr") or d["name"] for d in model.get("dimensions", []) if d["name"] == agg_time), None
        )
        dimensions = [
            {"dimension_key": "geography", "column": e.get("expr") or e["name"]}
            for e in model.get("entities", [])
            if e["name"] == "poc_geography"
        ]
        metrics.append(
            {
                "kpi_key": kpi["kpi_key"],
                "table": f"gold.metrics_{kpi['kpi_key']}",
                "unit_key": kpi["unit_key"],
                "frequency": kpi["frequency_default"],
                "time_column": time_column,
                "value_column": "value",
                "source_column": "source_system",
                "dimensions": dimensions,
            }
        )
    return {"version": 1, "generated_at": generated_at, "metrics": metrics}


def manifest() -> dict:
    """Build the manifest object from dbt's semantic manifest and kpi.csv; writes nothing."""
    semantic = json.loads((PROJECT / "target" / "semantic_manifest.json").read_text())
    with (PROJECT / "seeds" / "kpi.csv").open(encoding="utf-8", newline="") as f:
        kpis = list(csv.DictReader(f))
    return manifest_object(semantic, kpis, datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))
