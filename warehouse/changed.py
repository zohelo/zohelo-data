"""Which models the scheduled build must rebuild: only those whose inputs or code changed.

Owner policy #822: scheduled runs are incremental only. The daily build used to rewrite
every model in full (silver.bdl_observations alone ~229 M rows) although most Bronze
inputs change weekly at most. This plan is stateless: it reads Iceberg metadata only
(no data scan) and dbt's manifest from `dbt parse`.

After a successful build every model it built carries two table properties:
``zohelo.built_from_ms`` (when that build started) and ``zohelo.build_fingerprint``
(its SQL and config, the project's macros/dimensions/settings and requirements). A model
is rebuilt when

- its table is missing or has no properties (never built by this rule, or the last build failed),
- its fingerprint differs (the code changed),
- an upstream model is rebuilt in this run, or
- an input changed after its build started: a Bronze source's current snapshot, or an
  upstream table's ``built_from_ms`` (its snapshot time if it has none, e.g. the
  Eurostat and BDL observations, written per entity by their loaders, #875).

An incremental model (materialized='incremental') is rebuilt
incrementally, except when its own code changed or it has no build properties: then it is
listed in ``full_refresh`` and the build passes it as the dbt var ``full_refresh_models``
(the model's config turns that into a full refresh of that model only).

Table compaction also makes a new snapshot, so a compacted input causes a rebuild:
conservative, never a missed change.
"""

from __future__ import annotations

from collections.abc import Iterable
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any

PROJECT = Path(__file__).resolve().parent
BUILT_FROM = "zohelo.built_from_ms"
FINGERPRINT = "zohelo.build_fingerprint"
# Code outside a model's own SQL that can change what it writes.
SHARED_CODE = ("macros", "dimensions", "dbt_project.yml", "../requirements.txt")
EXTERNAL_MODELS: frozenset[str] = frozenset()  # models built outside the scheduled build (none since #875)
# A Bronze commit stamped just before a build started may become visible only after dbt
# read that table; the recorded start is moved back by this margin (an extra rebuild at worst).
START_MARGIN_MS = 10 * 60 * 1000
# Rebuild reasons after which an incremental model's rows cannot be trusted to follow its code.
FULL_REFRESH_REASONS = ("no build properties", "code changed")


def manifest(target: str = "prod") -> dict[str, Any]:
    """dbt's manifest of the project, freshly parsed (no database access)."""
    command = [
        "dbt",
        "parse",
        "--target",
        target,
        "--project-dir",
        str(PROJECT),
        "--profiles-dir",
        str(PROJECT),
        "--no-partial-parse",
    ]
    completed = subprocess.run(command, cwd=PROJECT, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(f"dbt parse failed: {completed.stdout[-1500:]} {completed.stderr[-500:]}")
    return json.loads((PROJECT / "target" / "manifest.json").read_text())


def shared_digest() -> str:
    digest = hashlib.sha256()
    for name in SHARED_CODE:
        root = (PROJECT / name).resolve()
        for path in sorted(root.rglob("*") if root.is_dir() else [root]):
            if path.is_file():
                digest.update(str(path.relative_to(root.parent)).encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def models(meta: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """unique_id → name, namespace, table, parents (unique ids of models and sources), fingerprint."""
    shared = shared_digest()
    nodes = {**meta["nodes"], **meta["sources"]}
    out = {}
    for uid, node in meta["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        # Parents' resolved relations: repointing a source or ref to another table is a change.
        parents = sorted(".".join(relation(nodes[p])) for p in node["depends_on"]["nodes"] if p in nodes)
        code = json.dumps(
            [shared, node["checksum"]["checksum"], node.get("config", {}), parents], sort_keys=True, default=str
        )
        out[uid] = {
            "name": node["name"],
            "namespace": relation(node)[0],
            "table": relation(node)[1],
            "parents": [p for p in node["depends_on"]["nodes"] if p.startswith(("model.", "source."))],
            "fingerprint": hashlib.sha256(code.encode()).hexdigest()[:32],
            "incremental": node.get("config", {}).get("materialized") == "incremental",
        }
    return out


def relation(node: dict[str, Any]) -> tuple[str, str]:
    """(namespace, table) of a model or source node."""
    if node.get("resource_type") == "source":
        return node["schema"], node.get("identifier") or node["name"]
    return node["schema"], node.get("alias") or node["name"]


def table_state(cat, namespace: str, table: str) -> dict[str, Any] | None:
    """Current snapshot time and build properties of a table; None when it does not exist."""
    from pyiceberg.exceptions import NoSuchTableError

    try:
        loaded = cat.load_table((namespace, table))
    except NoSuchTableError:
        return None
    snapshot = loaded.current_snapshot()
    props = loaded.properties
    return {
        "snapshot_ms": snapshot.timestamp_ms if snapshot else 0,
        "built_from_ms": int(props[BUILT_FROM]) if BUILT_FROM in props else None,
        "fingerprint": props.get(FINGERPRINT),
    }


def plan(cat, meta: dict[str, Any]) -> dict[str, Any]:
    """{"select": model names to build, "reasons": name → why, "unchanged": count,
    "full_refresh": incremental models among them to rebuild in full}."""
    graph = models(meta)
    relations = {uid: (m["namespace"], m["table"]) for uid, m in graph.items()}
    for uid in {p for m in graph.values() for p in m["parents"] if p.startswith("source.")}:
        relations[uid] = relation(meta["sources"][uid])
    state = {uid: table_state(cat, *rel) for uid, rel in relations.items()}

    def updated(uid: str) -> int:
        s = state[uid]
        if s is None:
            return 0  # a missing source cannot have changed since; dbt will fail on it if read
        return s["built_from_ms"] if s["built_from_ms"] is not None else s["snapshot_ms"]

    reasons: dict[str, str] = {}
    done: dict[str, bool] = {}

    def stale(uid: str) -> bool:
        if uid in done:
            return done[uid]
        model, own = graph[uid], state[uid]
        upstream = [p for p in model["parents"] if p in graph and stale(p)]
        reason = None
        if model["name"] in EXTERNAL_MODELS:
            reason = None
        elif own is None:
            reason = "table missing"
        elif own["built_from_ms"] is None or own["fingerprint"] is None:
            reason = "no build properties"
        elif own["fingerprint"] != model["fingerprint"]:
            reason = "code changed"
        elif upstream:
            reason = "upstream rebuilt: " + ", ".join(graph[p]["name"] for p in upstream[:3])
        else:
            newer = [p for p in model["parents"] if updated(p) > own["built_from_ms"]]
            if newer:
                reason = "input changed: " + ", ".join(".".join(relations[p]) for p in newer[:3])
        if reason:
            reasons[model["name"]] = reason
        done[uid] = reason is not None
        return done[uid]

    for uid in graph:
        stale(uid)
    full = sorted(
        m["name"] for m in graph.values() if m["incremental"] and reasons.get(m["name"]) in FULL_REFRESH_REASONS
    )
    return {
        "select": sorted(reasons),
        "reasons": reasons,
        "unchanged": len(graph) - len(reasons),
        "full_refresh": full,
    }


TEST_OK = ("pass", "warn")  # dbt test statuses that leave a model trusted


def record(cat, meta: dict[str, Any], results: Iterable[dict[str, Any]], started_ms: int) -> dict[str, Any]:
    """Set the build properties on each model the build wrote whose own tests all passed (dbt
    run results). A model with a failing test, or whose property write fails, keeps no
    properties and is rebuilt (and tested) next time; write failures are reported, not raised.
    A `warn` result is a pass at the severity the project declared (e.g. bdl_observations.year
    not_null warns on every build), so only `fail`, `error`, `skipped` and unknown statuses
    make a model untrusted."""
    results = list(results)
    graph = {m["name"]: m for m in models(meta).values()}
    failed_tests = {
        r["unique_id"] for r in results if r["unique_id"].startswith("test.") and r["status"] not in TEST_OK
    }
    untrusted = {
        p.split(".")[-1] for t in failed_tests for p in meta["nodes"].get(t, {}).get("depends_on", {}).get("nodes", [])
    }
    built = [
        r["unique_id"].split(".")[-1]
        for r in results
        if r["unique_id"].startswith("model.") and r["status"] == "success"
    ]
    recorded, failed = 0, []
    for name in built:
        model = graph.get(name)
        if model is None or name in EXTERNAL_MODELS or name in untrusted:
            continue
        try:
            table = cat.load_table((model["namespace"], model["table"]))
            with table.transaction() as tx:
                tx.set_properties({BUILT_FROM: str(started_ms - START_MARGIN_MS), FINGERPRINT: model["fingerprint"]})
            recorded += 1
        except Exception as exc:  # any failure only means a rebuild next run
            failed.append(f"{name}: {type(exc).__name__}: {str(exc)[:200]}")
    return {"recorded": recorded, "failed": failed[:5]}
