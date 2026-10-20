#!/usr/bin/env python3
"""GUS BDL Landing → Bronze → Silver, one subgroup at a time (design #875).

The entity is a subgroup; its version is every part of the newest complete download
(batch.candidates). Bronze and Silver each hold exactly one file per subgroup:

  02_bronze/gus_bdl/observations/<subgroup>/<recipe>.parquet           all parts, as published (decode.py)
  03_silver/gus_bdl/observations/<subgroup>/<recipe>.<model>.parquet   warehouse/entity/bdl_observations.sql

``<recipe>`` is the SHA-256 of the sorted SHA-256s of the parts; ``<model>`` digests the Silver
SQL and the subgroup's variable mapping (silver.bdl_variables), so a changed mapping redoes only
that subgroup. A changed SQL model redoes every subgroup: over ROW_LIMIT rows that needs
``--approved <issue>`` (owner rule R6, #875). A subgroup is rejected (recorded in
bronze.bdl_rejected_files, listed in the result) when a part fails the decoder or the Silver
checks: grain unit × dimensions × year, no two different published values for one key.
State comes from the tables' file lists only. Prints one JSON result line.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, get_with_backoff, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gus_bdl.decode import BRONZE_COLUMNS, BdlDecodeError, decode_web_export

ENTITY = "subgroup_id"
BRONZE = ("bronze", "bdl_observations")
SILVER = ("silver", "bdl_observations")
REJECTED = ("bronze", "bdl_rejected_files")
REJECTED_COLUMN = "dataset_id"  # rejected-files table predates the shared "entity" name
VARIABLES = ("silver", "bdl_variables")
BRONZE_PREFIX = "02_bronze/gus_bdl/observations/"
SILVER_PREFIX = "03_silver/gus_bdl/observations/"
SILVER_MODEL = "bdl_observations"
STATS = [ENTITY]
ROW_LIMIT = 10_000_000
COMMIT_EVERY = 200
COMMIT_SECONDS = 600
SILVER_COLUMNS = [
    ("subgroup_id", "VARCHAR"),
    ("variable_id", "BIGINT"),
    ("unit_code", "VARCHAR"),
    ("unit_code_scheme", "VARCHAR"),
    ("unit_name", "VARCHAR"),
    ("year", "INTEGER"),
    ("value", "DOUBLE"),
    ("value_text", "VARCHAR"),
    ("measure_unit", "VARCHAR"),
    ("attribute", "VARCHAR"),
    ("dimension_values", "VARCHAR"),
    ("channel", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("value_conflict", "BOOLEAN"),
]


DATA = "01_landing/gus_bdl/snapshot/data/"


def candidates(client, bucket: str) -> list[dict[str, Any]]:
    """Every part of the newest complete version per subgroup."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=DATA):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict:
        body = get_with_backoff(client, bucket, key)
        return {**json.loads(body), "_receipt_key": key, "_receipt_bytes": body}

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(read, keys))
    versions: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in receipts:
        if r.get("kind") == "gus_bdl_landing" and r.get("area") == "data":
            versions[(r["subgroup_id"], r["version_token"])].append(r)
    newest: dict[str, tuple[str, list[dict]]] = {}
    for (subgroup, version), parts in versions.items():
        total = int((parts[0].get("part") or {}).get("total", 1))
        if len({(p.get("part") or {}).get("index", 1) for p in parts}) < total:
            continue  # incomplete version: never loaded
        if subgroup not in newest or version > newest[subgroup][0]:
            newest[subgroup] = (version, parts)
    return [p for _, parts in newest.values() for p in parts]


def recipe(parts: list[dict[str, Any]]) -> str:
    return sha256("\n".join(sorted(p["sha256"] for p in parts)).encode()).hexdigest()


def variables(cat) -> dict[str, pa.Table]:
    """silver.bdl_variables per subgroup: (dimension_key, variable_id)."""
    table = bf.retry(lambda: cat.load_table(VARIABLES), "variables")
    data = table.scan(selected_fields=("subgroup_id", "dimension_key", "variable_id")).to_arrow()
    out: dict[str, list[dict]] = defaultdict(list)
    for row in data.to_pylist():
        out[row["subgroup_id"]].append({"dimension_key": row["dimension_key"], "variable_id": row["variable_id"]})
    schema = pa.schema([("dimension_key", pa.string()), ("variable_id", pa.int64())])
    return {
        k: pa.Table.from_pylist(
            sorted(v, key=lambda r: (r["dimension_key"] or "", r["variable_id"] or 0)), schema=schema
        )
        for k, v in out.items()
    }


def silver_tag(model: str, mapping: pa.Table | None) -> str:
    rows = json.dumps(mapping.to_pylist() if mapping is not None else [], sort_keys=True)
    return sha256((model + "\n" + rows).encode()).hexdigest()[:12]


def work(task: dict[str, Any], paths: dict[str, str], memory: str, tmp_root: str) -> dict[str, Any]:
    """One subgroup: Bronze (decode and combine every part) when needed, then Silver. Runs in a worker."""
    from warehouse import entity

    client = s3()
    bucket, landing = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    sg, rcp = task[ENTITY], task["recipe"]
    out: dict[str, Any] = {ENTITY: sg, "recipe": rcp, "status": "ok"}
    work_dir = Path(tempfile.mkdtemp(prefix="bdl-", dir=tmp_root))
    started = time.monotonic()
    try:
        bronze_key = f"{paths['bronze']}{sg}/{rcp}.parquet"
        out["bronze"] = task.get("bronze_uri") or f"s3://{bucket}/{bronze_key}"
        local_bronze = None
        if task["need_bronze"]:
            files = []
            for i, part in enumerate(sorted(task["parts"], key=lambda p: int((p.get("part") or {}).get("index", 1)))):
                raw, part_out = work_dir / f"{i:04d}.zip", work_dir / f"{i:04d}.parquet"
                bf.download(
                    client, landing, "gus_bdl", part["object"]["key"], part["sha256"], part.get("size_bytes"), raw
                )
                try:
                    decode_web_export(raw, part, part_out)
                except BdlDecodeError as exc:
                    out.update(
                        status="rejected",
                        error=f"part {i + 1}: {exc}"[:1000],
                        raw_sha256=part["sha256"],
                        landing_key=part["object"]["key"],
                    )
                    return out
                raw.unlink()
                files.append(str(part_out))
            local_bronze = work_dir / "bronze.parquet"
            with duckdb.connect() as con:
                listing = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
                con.execute(
                    f"COPY (SELECT * FROM read_parquet([{listing}])) TO '{local_bronze}' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)"
                )
            out["bronze"] = bf.upload(client, bucket, bronze_key, local_bronze, rcp)
            out["bronze_action"] = "decoded"
        lookup = work_dir / "variables.parquet"
        pq.write_table(task["mapping"], lookup)
        silver = work_dir / "silver.parquet"
        try:
            report = entity.apply(
                SILVER_MODEL,
                str(local_bronze) if local_bronze else out["bronze"],
                silver,
                lookups={"variables": str(lookup)},
                same_rows=False,
                not_null=("subgroup_id", "unit_code", "year"),
                unique_key=("unit_code", "dimension_values", "year"),
                reject_if="value_conflict",
                memory_limit=memory,
                temp_directory=str(work_dir),
            )
        except entity.EntityModelError as exc:
            out.update(status="rejected", error=str(exc)[:1000], raw_sha256=rcp, landing_key=f"subgroup {sg}")
            return out
        out["silver"] = bf.upload(client, bucket, f"{paths['silver']}{sg}/{rcp}.{task['tag']}.parquet", silver, rcp)
        out.update(rows=report["rows"], bronze_rows=report["bronze_rows"])
        return out
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        out["seconds"] = round(time.monotonic() - started, 1)


def plan(cat, client, ids: dict[str, tuple[str, str]], only: set[str] | None) -> dict[str, Any]:
    bronze = bf.retry(lambda: bf.ensure_table(cat, ids["bronze"], list(BRONZE_COLUMNS), ENTITY), "bronze")
    silver = bf.retry(lambda: bf.ensure_table(cat, ids["silver"], SILVER_COLUMNS, ENTITY), "silver")
    rejected = bf.rejected_table(cat, ids["rejected"], REJECTED_COLUMN)
    refused = set()
    if rejected.current_snapshot() is not None:
        refused = set(rejected.scan(selected_fields=("raw_sha256",)).to_arrow().column("raw_sha256").to_pylist())
    b_state, s_state = et.state(bronze, ENTITY), et.state(silver, ENTITY)
    groups: dict[str, list[dict]] = defaultdict(list)
    for part in candidates(client, req("R2_LANDING_BUCKET")):
        groups[part["subgroup_id"]].append(part)
    if only:
        groups = {k: v for k, v in groups.items() if k in only}
    mappings = variables(cat)
    model = (Path(__file__).resolve().parents[3] / "warehouse" / "entity" / f"{SILVER_MODEL}.sql").read_text()
    empty = pa.table({"dimension_key": pa.array([], pa.string()), "variable_id": pa.array([], pa.int64())})
    todo, model_only_rows, skipped_rejected, expected = [], 0, [], {}
    for sg, parts in groups.items():
        rcp = recipe(parts)
        if rcp in refused or any(p["sha256"] in refused for p in parts):
            skipped_rejected.append(sg)
            continue
        expected[sg] = rcp
        mapping = mappings.get(sg, empty)
        tag = silver_tag(model, mapping)
        need_bronze = b_state.get(sg, {}).get("recipes") != {rcp}
        held = s_state.get(sg, {}).get("recipes", set())
        need_silver = need_bronze or held != {f"{rcp}.{tag}"}
        if need_silver and not need_bronze and held and all(r.startswith(rcp + ".") for r in held):
            model_only_rows += s_state[sg]["rows"]
        if need_bronze or need_silver:
            current = b_state.get(sg, {}).get("paths", [])
            todo.append(
                {
                    ENTITY: sg,
                    "recipe": rcp,
                    "parts": parts,
                    "need_bronze": need_bronze,
                    "bronze_uri": None if need_bronze else current[0],
                    "mapping": mapping,
                    "tag": tag,
                    "size": sum(int(p.get("size_bytes") or 0) for p in parts),
                }
            )
    return {
        "todo": todo,
        "expected": expected,
        "subgroups": len(groups),
        "skipped_rejected": skipped_rejected,
        "model_only_rows": model_only_rows,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=50)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--approved", default="")
    parser.add_argument("--memory", default="4GB")
    parser.add_argument("--silver-table", default=".".join(SILVER))
    parser.add_argument("--trial", default="", help="scratch namespace and prefix (Dev trials): writes only there")
    args = parser.parse_args(argv)
    started = time.monotonic()
    deadline = started + args.minutes * 60
    ids = {"bronze": BRONZE, "silver": tuple(args.silver_table.split(".", 1)), "rejected": REJECTED}
    paths = {"bronze": BRONZE_PREFIX, "silver": SILVER_PREFIX}
    if args.trial:
        ids = {k: (args.trial, f"{k}_{v[1]}") for k, v in ids.items()}
        paths = {"bronze": f"{args.trial}/bronze/", "silver": f"{args.trial}/silver/"}
    cat, client = bf.retry(catalog, "catalog"), s3()
    p = plan(cat, client, ids, set(args.only) if args.only else None)
    for key in ("bronze", "silver"):
        et.configure(ids[key])
    if p["model_only_rows"] > ROW_LIMIT and not args.approved:
        annotate("error", "bdl entities: approval needed", {"rule": "R6 #875", "rows_to_redo": p["model_only_rows"]})
        return 1
    tmp_root = os.environ.get("DUCKDB_TEMP_DIRECTORY") or tempfile.gettempdir()
    Path(tmp_root).mkdir(parents=True, exist_ok=True)
    totals: dict[str, Any] = {"bronze_decoded": 0, "silver_written": 0, "rows": 0, "bronze_rows": 0, "rejected": []}
    pending: list[dict[str, Any]] = []
    last = time.monotonic()

    def commit() -> None:
        nonlocal pending, last
        ok = [r for r in pending if r["status"] == "ok"]
        if ok:
            et.replace(cat, ids["bronze"], ENTITY, {r[ENTITY]: [r["bronze"]] for r in ok}, STATS, "bdl-entities")
            et.replace(cat, ids["silver"], ENTITY, {r[ENTITY]: [r["silver"]] for r in ok}, STATS, "bdl-entities")
        bad = [r for r in pending if r["status"] == "rejected"]
        bf.record_rejections(
            cat,
            ids["rejected"],
            [
                {
                    "entity": r[ENTITY],
                    "raw_sha256": r["raw_sha256"],
                    "landing_key": r["landing_key"],
                    "error": r["error"],
                    "recorded_at": datetime.now(UTC),
                    "code_sha": os.environ.get("GITHUB_SHA", "unknown"),
                }
                for r in bad
            ],
            REJECTED_COLUMN,
        )
        pending, last = [], time.monotonic()

    queue = sorted(p["todo"], key=lambda t: t["size"])
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        running: dict = {}
        while queue or running:
            while queue and len(running) < args.workers and time.monotonic() < deadline:
                task = queue.pop(0)
                running[pool.submit(work, task, paths, args.memory, tmp_root)] = task
            if not running:
                break
            finished, _ = wait(running, timeout=60, return_when=FIRST_COMPLETED)
            for f in finished:
                running.pop(f)
                result = f.result()
                pending.append(result)
                if result["status"] == "ok":
                    totals["silver_written"] += 1
                    totals["rows"] += result["rows"]
                    totals["bronze_rows"] += result["bronze_rows"]
                    totals["bronze_decoded"] += result.get("bronze_action") == "decoded"
                else:
                    totals["rejected"].append({"subgroup": result[ENTITY], "error": result["error"][:200]})
            if len(pending) >= COMMIT_EVERY or (pending and time.monotonic() - last > COMMIT_SECONDS):
                commit()
    commit()
    left = len(p["todo"]) - totals["silver_written"] - len(totals["rejected"])
    summary = {
        "operation": "bdl-entities",
        "result": "complete" if left == 0 else "incomplete_time_budget",
        "subgroups": p["subgroups"],
        "todo": len(p["todo"]),
        "remaining": left,
        "skipped_rejected_earlier": len(p["skipped_rejected"]),
        **{k: v for k, v in totals.items() if k != "rejected"},
        "rejected": len(totals["rejected"]),
        "rejected_subgroups": totals["rejected"][:15],
        "minutes": round((time.monotonic() - started) / 60, 1),
    }
    print(json.dumps(summary, sort_keys=True, default=str), flush=True)
    annotate("notice" if left == 0 and not totals["rejected"] else "warning", "bdl entities", summary)
    return 0 if left == 0 else 2


def verify(min_pct: float = 99.0) -> int:
    """bronze.verify: Landing → Bronze → Silver completeness per subgroup, from metadata (R5/R6, #875)."""
    started = time.monotonic()
    cat, client = bf.retry(catalog, "catalog"), s3()
    ids = {"bronze": BRONZE, "silver": SILVER, "rejected": REJECTED}
    p = plan(cat, client, ids, None)
    report = et.completeness(
        p["expected"],
        et.state(bf.retry(lambda: cat.load_table(BRONZE), "bronze"), ENTITY),
        et.state(bf.retry(lambda: cat.load_table(SILVER), "silver"), ENTITY),
    )
    report["rejected_subgroups"] = sorted(p["skipped_rejected"])[:20]
    report["rejected_count"] = len(p["skipped_rejected"])
    report["minutes"] = round((time.monotonic() - started) / 60, 1)
    failed = report["bronze_pct"] < min_pct or report["silver_pct"] < min_pct or report["entities_with_several_files"]
    annotate("error" if failed else "notice", "gus_bdl completeness (Landing → Bronze → Silver)", report)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())


def proven() -> set[str]:
    """Landing SHA-256 of every part whose subgroup's Bronze file holds exactly its download (archive proof)."""
    cat, client = bf.retry(catalog, "catalog"), s3()
    held = et.state(bf.retry(lambda: cat.load_table(BRONZE), "bronze"), ENTITY)
    groups: dict[str, list[dict]] = defaultdict(list)
    for part in candidates(client, req("R2_LANDING_BUCKET")):
        groups[part["subgroup_id"]].append(part)
    return {
        p["sha256"] for sg, parts in groups.items() if held.get(sg, {}).get("recipes") == {recipe(parts)} for p in parts
    }
