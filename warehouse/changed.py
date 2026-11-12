"""Which models the scheduled build must rebuild: only those whose inputs or code changed.

Owner policy #822: scheduled runs are incremental only. The daily build used to rewrite
every model in full (silver.bdl_observations alone ~229 M rows) although most Bronze
inputs change weekly at most. This plan is stateless: it reads Iceberg metadata only
(no data scan) and dbt's manifest from `dbt parse`.

After a successful build every model it built carries two table properties:
``zohelo.built_from_ms`` (when that build started) and ``zohelo.build_fingerprint``
(its SQL and config, the project's macros/dimensions/settings and requirements). The per-source
macro files (``macros/sources/``, docs/source-registration.md) count only for the models that use
them, so a new source's file rebuilds the Gold models that read it and what is built on them, not
every model of every source. A model is rebuilt when

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

A model whose source table does not exist yet (a new source before its first Bronze load), directly or
through other models, is left out and listed under ``waiting``. A Gold block (a model tagged ``gold_block``,
listed by ``poc_block_models()``, macros/source_registration.sql) stops that: it waits itself, but the models that
union the blocks (facts, dimensions, metrics) are planned as usual and read only the blocks whose table
exists (``poc_observations()``), so a new source's block contributes zero rows until its first run instead
of holding back the whole Gold layer. Those blocks are listed again under ``empty_blocks``, but only when
their own table does not exist either. A model also does not wait for a parent it declares optional (config
meta ``optional_parents``, read through ``poc_optional_ref()``: the Silver tables the shared geography map reads
only for a source's own codes), so one source without Bronze cannot hold back the map and every block that
reads it (#2903).

A waiting model that already has a table is not a new source: an input it was built from is gone, or a new
input was added to it before that input's first load. It is listed under ``stalled`` with the missing source
tables, and the build stops before dbt runs (warehouse/build.py): such a model would otherwise stay frozen
without a word, and a stalled Gold block must never be passed on as an empty one.

Table compaction also makes a new snapshot, so a compacted input causes a rebuild:
conservative, never a missed change.
"""

from __future__ import annotations

from collections.abc import Iterable
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any

PROJECT = Path(__file__).resolve().parent
BLOCK_TAG = "gold_block"  # a Gold block: its children read it only when its table exists
OPTIONAL_PARENTS = "optional_parents"  # config meta: parents (model names) a model reads only when they exist
BUILT_FROM = "zohelo.built_from_ms"
FINGERPRINT = "zohelo.build_fingerprint"
# Code outside a model's own SQL that can change what it writes.
SHARED_CODE = ("macros", "dimensions", "dbt_project.yml", "../requirements.txt")
# Per-source macro files (D34): left out of the shared digest, fingerprinted with the models that use them.
SOURCE_MACROS = "macros/sources"
# The macros that find the per-source macros by name; dbt records no dependency for a macro called that way.
REGISTRATION = "macros/source_registration.sql"
SOURCE_MACRO_PREFIX = "poc_"  # a macro of another name there could replace a dbt macro for every model
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
    """One digest of the code every model shares; the files under SOURCE_MACROS are not in it."""
    digest = hashlib.sha256()
    apart = (PROJECT / SOURCE_MACROS).resolve()
    for name in SHARED_CODE:
        root = (PROJECT / name).resolve()
        for path in sorted(root.rglob("*") if root.is_dir() else [root]):
            if path.is_file() and apart not in path.parents:
                digest.update(str(path.relative_to(root.parent)).encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def source_macro_files(meta: dict[str, Any]) -> dict[str, list[str]]:
    """unique_id of a model or seed → the files under SOURCE_MACROS its SQL depends on (project-relative, sorted);
    pure (reads the manifest only).

    A model depends on such a file when it calls a macro defined there, directly or through other project
    macros, and on every such file when it reaches a macro of REGISTRATION, which calls the per-source macros by
    a name built at run time. Calls are taken from dbt (``depends_on.macros`` of the node and of each macro)
    and, because dbt does not record a call inside ``{% if execute %}`` or through ``context[...]``, also from the
    text: a per-source or registration macro whose name occurs as a word in the model's code or in a reached
    macro's code counts as called. A file there that defines a macro without SOURCE_MACRO_PREFIX belongs to
    every model."""
    project = meta["metadata"]["project_name"]
    macros = {uid: m for uid, m in meta["macros"].items() if m["package_name"] == project}
    files = {uid: Path(m["original_file_path"]).as_posix() for uid, m in macros.items()}
    per_source = {uid for uid, f in files.items() if f.startswith(SOURCE_MACROS + "/")}
    registration = {uid for uid, f in files.items() if f == REGISTRATION}
    every = sorted({files[uid] for uid in per_source})
    loose = {files[uid] for uid in per_source if not macros[uid]["name"].startswith(SOURCE_MACRO_PREFIX)}
    named = {macros[uid]["name"]: uid for uid in per_source | registration}
    word = re.compile(r"\b(" + "|".join(map(re.escape, sorted(named))) + r")\b") if named else None

    def called(recorded: list[str], code: str) -> set[str]:
        found = {uid for uid in recorded if uid in macros}
        return found | ({named[name] for name in word.findall(code or "")} if word else set())

    calls = {
        uid: called(m.get("depends_on", {}).get("macros", []), m.get("macro_sql", "")) for uid, m in macros.items()
    }

    def reached(start: set[str]) -> set[str]:
        """The macros and every project macro they call, at any depth."""
        seen, queue = set(start), list(start)
        while queue:
            for callee in calls[queue.pop()] - seen:
                seen.add(callee)
                queue.append(callee)
        return seen

    out = {}
    for uid, node in meta["nodes"].items():
        if node.get("resource_type") not in ("model", "seed"):
            continue
        used = reached(called(node.get("depends_on", {}).get("macros", []), node.get("raw_code", "")))
        if used & registration:
            out[uid] = every
        else:
            out[uid] = sorted({files[m] for m in used & per_source} | loose)
    return out


def files_digest(files: list[str]) -> str:
    """Digest of project files by path and content; "" for none."""
    if not files:
        return ""
    digest = hashlib.sha256()
    for name in files:
        digest.update(name.encode() + b"\0" + (PROJECT / name).read_bytes() + b"\0")
    return digest.hexdigest()


def models(meta: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """unique_id → name, namespace, table, parents (unique ids of models, seeds and sources), fingerprint.
    Seeds are planned like models: their checksum is the CSV's, so an edited seed rebuilds itself
    and the models that ref it."""
    shared = shared_digest()
    per_source = source_macro_files(meta)
    nodes = {**meta["nodes"], **meta["sources"]}
    out = {}
    for uid, node in meta["nodes"].items():
        if node.get("resource_type") not in ("model", "seed"):
            continue
        # Parents' resolved relations: repointing a source or ref to another table is a change.
        upstream = node.get("depends_on", {}).get("nodes", [])  # a seed lists none
        parents = sorted(".".join(relation(nodes[p])) for p in upstream if p in nodes)
        code = json.dumps(
            [shared, node["checksum"]["checksum"], node.get("config", {}), parents, files_digest(per_source[uid])],
            sort_keys=True,
            default=str,
        )
        out[uid] = {
            "name": node["name"],
            "namespace": relation(node)[0],
            "table": relation(node)[1],
            "parents": [p for p in upstream if p.startswith(("model.", "source.", "seed."))],
            "fingerprint": hashlib.sha256(code.encode()).hexdigest()[:32],
            "incremental": node.get("config", {}).get("materialized") == "incremental",
            "block": BLOCK_TAG in node.get("tags", []),
            "optional": set(node.get("config", {}).get("meta", {}).get(OPTIONAL_PARENTS) or []),
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
    "full_refresh": incremental models among them to rebuild in full, "waiting": models left out because a
    source table they read does not exist yet, "empty_blocks": the Gold blocks among them that have no table
    either, read as zero rows, "stalled": waiting model → the missing source tables, for every waiting model
    that has a table (the build must stop on any)}."""
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
    blocked: dict[str, bool] = {}

    def waiting(uid: str) -> bool:
        """True when a source table this model reads (directly or through other models) does not exist yet:
        a newly onboarded source whose first Bronze load has not run. Building it would fail every pipeline's
        build, so it waits and is listed under "waiting". A waiting Gold block does not make its children
        wait: they skip a block whose table does not exist (poc_observations). Nor does a parent the model
        declares optional (poc_optional_ref)."""
        if uid not in blocked:
            blocked[uid] = bool(missing_sources(uid))
        return blocked[uid]

    missing: dict[str, set[str]] = {}

    def missing_sources(uid: str) -> set[str]:
        """The source tables that do not exist and make this model wait (through other models too)."""
        if uid not in missing:
            model = graph[uid]
            found = {p for p in model["parents"] if p.startswith("source.") and state.get(p) is None}
            for p in model["parents"]:
                if p in graph and not graph[p]["block"] and graph[p]["name"] not in model["optional"]:
                    found |= missing_sources(p)
            missing[uid] = found
        return missing[uid]

    def stale(uid: str) -> bool:
        if uid in done:
            return done[uid]
        model, own = graph[uid], state[uid]
        if waiting(uid):
            done[uid] = False
            return False
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
        "unchanged": len(graph) - len(reasons) - sum(blocked.values()),
        "full_refresh": full,
        "waiting": sorted(graph[u]["name"] for u, b in blocked.items() if b),
        "empty_blocks": sorted(
            graph[u]["name"] for u, b in blocked.items() if b and graph[u]["block"] and state[u] is None
        ),
        "stalled": {
            graph[u]["name"]: sorted(".".join(relations[p]) for p in missing[u])
            for u, b in sorted(blocked.items(), key=lambda item: graph[item[0]]["name"])
            if b and state[u] is not None
        },
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
        if r["unique_id"].startswith(("model.", "seed.")) and r["status"] == "success"
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
