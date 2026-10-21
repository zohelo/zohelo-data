#!/usr/bin/env python3
"""GUS BDL Landing → Bronze → Silver, one subgroup at a time (design #875).

The entity is a subgroup; its version is every part of the newest complete download
(batch.candidates). Bronze and Silver each hold exactly one file per subgroup:

  02_bronze/gus_bdl/observations/<subgroup>/<recipe>.parquet           all parts, as published (decode.py)
  03_silver/gus_bdl/observations/<subgroup>/<recipe>.<model>.parquet   warehouse/entity/bdl_observations.sql

``<recipe>`` is the SHA-256 of the sorted SHA-256s of the parts; ``<model>`` digests the Silver
SQL and the subgroup's variable mapping (silver.bdl_variables), so a changed mapping redoes only
that subgroup. A changed SQL model recomputes every subgroup's Silver from unchanged Bronze
(reported as ``redo_rows``). A subgroup is rejected (recorded in
bronze.bdl_rejected_files, listed in the result) when a part fails the decoder or the Silver
checks: grain unit (code + name) × dimensions × year, no two different published values for one key.
State comes from the tables' file lists only. Prints one JSON result line.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
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
from ingestion.to_bronze.gus_bdl.decode import BRONZE_COLUMNS, BdlDecodeError, BdlEmptyExport, decode_web_export

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
COMMIT_EVERY = 200
COMMIT_SECONDS = 600
EMPTY = "empty: every part of the download has no rows (published without data)"
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
    by_part: dict[tuple[str, str], dict[int, dict]] = defaultdict(dict)
    for r in receipts:
        if r.get("kind") == "gus_bdl_landing" and r.get("area") == "data":
            index = int((r.get("part") or {}).get("index", 1))
            held = by_part[(r["subgroup_id"], r["version_token"])].get(index)
            # A part downloaded again under the same version replaces the earlier download of it.
            if held is None or (r.get("ingested_at") or "") > (held.get("ingested_at") or ""):
                by_part[(r["subgroup_id"], r["version_token"])][index] = r
    versions = {key: list(parts.values()) for key, parts in by_part.items()}
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
    """<model digest 6><mapping digest 6>: a changed SQL model redoes every subgroup; a
    changed variable mapping redoes only that subgroup and is a normal refresh, not a rebuild."""
    rows = json.dumps(mapping.to_pylist() if mapping is not None else [], sort_keys=True)
    return sha256(model.encode()).hexdigest()[:6] + sha256(rows.encode()).hexdigest()[:6]


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
                except BdlEmptyExport:  # its territories have no data: an empty part, not an error
                    raw.unlink()
                    continue
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
            if not files:  # every part empty: published without data (a table cannot hold an empty file)
                out.update(status="rejected", error=EMPTY, raw_sha256=rcp, landing_key=f"subgroup {sg}")
                return out
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
                unique_key=("unit_code", "unit_name", "dimension_values", "year"),
                reject_if="value_conflict",
                memory_limit=memory,
                temp_directory=str(work_dir),
            )
        except entity.EntityModelError as exc:  # verdict of this model and mapping: keyed by <recipe>.<tag>
            out.update(
                status="rejected",
                error=f"silver: {exc}"[:1000],
                raw_sha256=f"{rcp}.{task['tag']}",
                landing_key=f"subgroup {sg}",
            )
            return out
        out["silver"] = bf.upload(client, bucket, f"{paths['silver']}{sg}/{rcp}.{task['tag']}.parquet", silver, rcp)
        out.update(rows=report["rows"], bronze_rows=report["bronze_rows"])
        return out
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        out["seconds"] = round(time.monotonic() - started, 1)


def refused_keys(cat, rejected_id: tuple[str, str], create: bool) -> set[str]:
    """Keys recorded as rejected: a part's Landing SHA-256 (decoder) or <recipe>.<tag> (Silver)."""
    from pyiceberg.exceptions import NoSuchTableError

    try:
        table = bf.rejected_table(cat, rejected_id, REJECTED_COLUMN) if create else cat.load_table(rejected_id)
    except NoSuchTableError:
        return set()
    if table.current_snapshot() is None:
        return set()
    rows = table.scan(selected_fields=("raw_sha256", "error")).to_arrow().to_pylist()
    # Empty exports were rejected by the earlier loader; an empty part is valid, not a refusal.
    return {r["raw_sha256"] for r in rows if not str(r.get("error") or "").startswith("BDL export has no rows")}


def empty_keys(cat, rejected_id: tuple[str, str]) -> set[str]:
    """Recipes of downloads whose every part is empty (recorded with EMPTY): no entity, not a refusal."""
    from pyiceberg.exceptions import NoSuchTableError

    try:
        table = cat.load_table(rejected_id)
    except NoSuchTableError:
        return set()
    if table.current_snapshot() is None:
        return set()
    rows = table.scan(selected_fields=("raw_sha256", "error")).to_arrow().to_pylist()
    return {r["raw_sha256"] for r in rows if r.get("error") == EMPTY}


def plan(cat, client, ids: dict[str, tuple[str, str]], only: set[str] | None, create: bool = True) -> dict[str, Any]:
    """What each subgroup needs. ``create=False`` (checks): reads only, never creates a table."""
    if create:
        bronze = bf.retry(lambda: bf.ensure_table(cat, ids["bronze"], list(BRONZE_COLUMNS), ENTITY), "bronze")
        silver = bf.retry(lambda: bf.ensure_table(cat, ids["silver"], SILVER_COLUMNS, ENTITY), "silver")
    else:
        bronze = bf.retry(lambda: cat.load_table(ids["bronze"]), "bronze")
        silver = bf.retry(lambda: cat.load_table(ids["silver"]), "silver")
    refused = refused_keys(cat, ids["rejected"], create)
    without_data = empty_keys(cat, ids["rejected"])
    b_state, s_state = et.state(bronze, ENTITY), et.state(silver, ENTITY)
    groups: dict[str, list[dict]] = defaultdict(list)
    for part in candidates(client, req("R2_LANDING_BUCKET")):
        groups[part["subgroup_id"]].append(part)
    if only:
        groups = {k: v for k, v in groups.items() if k in only}
    mappings = variables(cat)
    model = (Path(__file__).resolve().parents[3] / "warehouse" / "entity" / f"{SILVER_MODEL}.sql").read_text()
    empty = pa.table({"dimension_key": pa.array([], pa.string()), "variable_id": pa.array([], pa.int64())})
    todo, rejected, expected, expected_silver, redo_rows, no_data, stale_remove = [], [], {}, {}, 0, [], []
    for sg, parts in groups.items():
        rcp = recipe(parts)
        if rcp in without_data:
            no_data.append(sg)
            if sg in b_state or sg in s_state:  # its newest download has no data: older data leaves too
                stale_remove.append(sg)
            continue
        mapping = mappings.get(sg, empty)
        tag = silver_tag(model, mapping)
        if f"{rcp}.{tag}" in refused or any(p["sha256"] in refused for p in parts):
            rejected.append(sg)
            continue
        expected[sg], expected_silver[sg] = rcp, f"{rcp}.{tag}"
        need_bronze = b_state.get(sg, {}).get("recipes") != {rcp}
        need_silver = need_bronze or s_state.get(sg, {}).get("recipes") != {f"{rcp}.{tag}"}
        held = s_state.get(sg, {}).get("recipes", set())
        # reported: Silver recomputed from unchanged Bronze because the SQL model changed
        redo = need_silver and not need_bronze and not any(r.startswith(f"{rcp}.{tag[:6]}") for r in held)
        if redo:
            redo_rows += b_state[sg]["rows"]
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
                    "redo": redo,
                    "size": sum(int(p.get("size_bytes") or 0) for p in parts),
                }
            )
    return {
        "todo": todo,
        "expected": expected,
        "expected_silver": expected_silver,
        "subgroups": len(groups),
        "rejected": rejected,
        "no_data": no_data,
        "remove": stale_remove,
        "redo_rows": redo_rows,
        "bronze": b_state,
        "silver": s_state,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=50)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--only", nargs="*")
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
    if p["remove"]:  # subgroups whose newest download is published without data
        for key in ("bronze", "silver"):
            et.replace(cat, ids[key], ENTITY, {sg: [] for sg in p["remove"]}, STATS, "bdl-entities")
        annotate("notice", "bdl entities: removed, newest download has no data", {"subgroups": p["remove"]})
    tmp_root = os.environ.get("DUCKDB_TEMP_DIRECTORY") or tempfile.gettempdir()
    Path(tmp_root).mkdir(parents=True, exist_ok=True)
    totals: dict[str, Any] = {"bronze_decoded": 0, "silver_written": 0, "rows": 0, "bronze_rows": 0}
    rejected_now: list[dict[str, str]] = []
    failed: list[dict[str, str]] = []
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

    def account(result: dict[str, Any]) -> None:
        if result["status"] == "failed":
            failed.append({"subgroup": result[ENTITY], "error": result["error"][:200]})
            return
        pending.append(result)
        if result["status"] == "ok":
            totals["silver_written"] += 1
            totals["rows"] += result["rows"]
            totals["bronze_rows"] += result["bronze_rows"]
            totals["bronze_decoded"] += result.get("bronze_action") == "decoded"
        elif result["error"] == EMPTY:
            totals["published_without_data"] = totals.get("published_without_data", 0) + 1
        else:
            rejected_now.append({"subgroup": result[ENTITY], "error": result["error"][:300]})

    def failure(task: dict[str, Any], exc: BaseException) -> dict[str, Any]:
        return {ENTITY: task[ENTITY], "status": "failed", "error": f"{type(exc).__name__}: {exc}"}

    queue = sorted(p["todo"], key=lambda t: t["size"])
    serial: list[dict[str, Any]] = []
    while queue and time.monotonic() < deadline - (900 if serial else 0):
        broken = False
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            running: dict = {}
            while (queue or running) and not broken:
                stop = deadline - (900 if serial else 0)  # killed subgroups keep time to run alone
                while queue and len(running) < args.workers and time.monotonic() < stop:
                    task = queue.pop(0)
                    running[pool.submit(work, task, paths, args.memory, tmp_root)] = task
                if not running:
                    break
                finished, _ = wait(running, timeout=60, return_when=FIRST_COMPLETED)
                for f in finished:
                    task = running.pop(f)
                    try:
                        account(f.result())
                    except BrokenProcessPool:  # a worker was killed (memory): unknown which; retry alone
                        serial.append(task)
                        broken = True
                    except Exception as exc:  # non-data failure of one subgroup: the next run retries it
                        account(failure(task, exc))
                if len(pending) >= COMMIT_EVERY or (pending and time.monotonic() - last > COMMIT_SECONDS):
                    commit()
            if broken:
                serial += list(running.values())
                pool.shutdown(wait=False, cancel_futures=True)
        commit()
    for task in serial:  # each in its own one-worker pool with the full memory: a kill fails that subgroup only
        if time.monotonic() + 600 > deadline:
            continue
        with ProcessPoolExecutor(max_workers=1) as alone:
            try:
                account(alone.submit(work, task, paths, "10GB", tmp_root).result())
            except Exception as exc:
                account(failure(task, exc))
        commit()
    left = len(p["todo"]) - totals["silver_written"] - len(rejected_now) - totals.get("published_without_data", 0)
    summary = {
        "operation": "bdl-entities",
        "result": "complete" if left == 0 else ("failures" if failed else "incomplete_time_budget"),
        "subgroups": p["subgroups"],
        "todo": len(p["todo"]),
        "redo_rows": p["redo_rows"],
        "remaining": left,
        "rejected_earlier": len(p["rejected"]),
        **totals,
        "rejected": len(rejected_now),
        "rejected_subgroups": rejected_now[:12],
        "failed": len(failed),
        "failed_subgroups": failed[:10],
        "minutes": round((time.monotonic() - started) / 60, 1),
    }
    print(json.dumps(summary, sort_keys=True, default=str), flush=True)
    annotate("notice" if left == 0 and not rejected_now else "warning", "bdl entities", summary)
    return 0 if left == 0 else 2


def completeness_report() -> dict[str, Any]:
    """Landing → Bronze → Silver completeness per subgroup, from table metadata only (R5, #875)."""
    started = time.monotonic()
    cat, client = bf.retry(catalog, "catalog"), s3()
    p = plan(cat, client, {"bronze": BRONZE, "silver": SILVER, "rejected": REJECTED}, None, create=False)
    report = et.completeness(p["expected"], p["bronze"], p["silver"], p["expected_silver"])
    report["rejected_count"] = len(p["rejected"])
    report["rejected_subgroups"] = sorted(p["rejected"])[:20]
    report["published_without_data"] = sorted(p["no_data"])
    report["minutes"] = round((time.monotonic() - started) / 60, 1)
    return report


def verify(min_pct: float = 99.0) -> int:
    """bronze.verify: fails when a layer is below ``min_pct`` complete or a subgroup holds several files (#875)."""
    report = completeness_report()
    failed = report["bronze_pct"] < min_pct or report["silver_pct"] < min_pct or report["entities_with_several_files"]
    annotate("error" if failed else "notice", "gus_bdl completeness (Landing → Bronze → Silver)", report)
    return 1 if failed else 0


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


if __name__ == "__main__":
    raise SystemExit(main())
