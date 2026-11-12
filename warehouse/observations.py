#!/usr/bin/env python3
"""Gold observations of a big source, one block at a time (docs/gold-coverage.md, owner direction O36).

Every loaded Silver block of the source (Eurostat: a dataset) is put into Gold by rule, without an
activation list: the block's Silver files go through the source's build stage
(warehouse/entity/gold_<source>_*.sql, the common observation shape) and three projections shared
by all sources (gold_observations.sql, gold_series.sql, gold_measures.sql). Gold holds exactly one
file per block in each table, as Silver does (design #875):

  04_gold/<source>/<table>/<block>/<recipe>.parquet     gold.<source>_observations | _series | _measures

``<recipe>`` names what the file was made from: the block's Silver files (their names carry the
Landing SHA-256 and the Silver model), the Gold SQL and the source's rows of gold.map_geography. A
block is rebuilt only when its recipe changed: a new source version rebuilds that block, a changed
rule or geography map rebuilds every block, and a block that left Silver leaves Gold. What Gold holds
is read from the tables' file lists; nothing is scanned.

The grain is checked on every block before it is accepted: one row per series_key × date_key.
measure_key begins with the source and the block, so a duplicate can only occur inside one block
and the per-block check covers the whole table. A block that breaks the grain or yields no rows is
rejected (gold.observations_rejected, keyed by its recipe) and removed from Gold, never loaded in
part. A block that fails for another reason (storage, memory) is tried once more alone with the
full memory limit, then reported and retried by the next run. Work runs in parallel processes within
a time budget; finished blocks are committed in chunks,
so a run that stops resumes where it stopped. Prints one JSON result line.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime
from hashlib import sha256
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

MODELS = Path(__file__).parent / "entity"
ENTITY = "block_id"
REJECTED = ("gold", "observations_rejected")
COMMIT_EVERY = 200  # blocks
COMMIT_SECONDS = 600
BIG_ROWS = 50_000_000  # blocks with more Silver rows are built alone, with the full memory limit
# The Parquet writer buffers a row group per thread; with names and JSON on every row, 1 M-row groups
# took more than 10 GB on one dataset (NAIO_10_FCP_U3, Dev trial 43202733905).
ROW_GROUP = 100_000

# Columns of the three Gold tables, in the order their models select them.
TABLES: dict[str, dict[str, Any]] = {
    "observations": {
        "model": "gold_observations",
        "unique_key": ("series_key", "date_key"),
        "not_null": ("series_key", "measure_key", "geography_key", "date_key", "frequency", "value"),
        "columns": [
            ("series_key", "VARCHAR"),
            ("measure_key", "VARCHAR"),
            ("geography_key", "VARCHAR"),
            ("date_key", "DATE"),
            ("frequency", "VARCHAR"),
            ("value", "DOUBLE"),
            ("observation_status_key", "VARCHAR"),
            ("source_system", "VARCHAR"),
            ("block_id", "VARCHAR"),
            ("period_label", "VARCHAR"),
            ("source_version", "VARCHAR"),
            ("raw_sha256", "VARCHAR"),
        ],
    },
    "series": {
        "model": "gold_series",
        "unique_key": ("series_key",),
        "not_null": ("series_key", "measure_key", "geography_key", "sex_key", "is_total"),
        "columns": [
            ("series_key", "VARCHAR"),
            ("measure_key", "VARCHAR"),
            ("geography_key", "VARCHAR"),
            ("source_system", "VARCHAR"),
            ("block_id", "VARCHAR"),
            ("frequency", "VARCHAR"),
            ("unit_key", "VARCHAR"),
            ("breakdown_json", "VARCHAR"),
            ("breakdown_label_json", "VARCHAR"),
            ("sex_key", "VARCHAR"),
            ("is_total", "BOOLEAN"),
            ("first_date", "DATE"),
            ("last_date", "DATE"),
            ("points", "BIGINT"),
            ("geography_source_code", "VARCHAR"),
            ("source_version", "VARCHAR"),
            ("raw_sha256", "VARCHAR"),
        ],
    },
    "measures": {
        "model": "gold_measures",
        "unique_key": ("measure_key",),
        "not_null": ("measure_key", "unit_key", "frequency"),
        "columns": [
            ("measure_key", "VARCHAR"),
            ("dataset_key", "VARCHAR"),
            ("source_system", "VARCHAR"),
            ("block_id", "VARCHAR"),
            ("block_name", "VARCHAR"),
            ("measure_name", "VARCHAR"),
            ("measure_codes", "VARCHAR"),
            ("unit_key", "VARCHAR"),
            ("unit_name", "VARCHAR"),
            ("frequency", "VARCHAR"),
            ("first_date", "DATE"),
            ("last_date", "DATE"),
            ("series", "BIGINT"),
            ("points", "BIGINT"),
            ("areas", "BIGINT"),
            ("points_geography_unmatched", "BIGINT"),
            ("points_total_series", "BIGINT"),
            ("source_version", "VARCHAR"),
            ("raw_sha256", "VARCHAR"),
        ],
    },
}

# Per source: the Silver entity table and how its entities group into blocks, the build stage, and the
# small tables the build stage reads (relation -> (namespace, table, columns, DuckDB filter)).
SOURCES: dict[str, dict[str, Any]] = {
    "eurostat": {
        "silver": ("silver", "eurostat_observations"),
        "block_of": str.upper,  # Eurostat spells a dataset id in either case
        "build": "gold_eurostat_build",
        # steps before the build, each kept as a table it reads (table -> model)
        "prepare": {"dimension_sets": "gold_eurostat_dimension_sets", "measure_names": "gold_eurostat_measure_names"},
        "lookups": {
            "geo_map": ("gold", "map_geography", ("source_system", "source_code", "geography_key"), "eurostat"),
            "flows": ("silver", "eurostat_dataflows", ("dataset_id", "name_en"), None),
            "dims": ("silver", "eurostat_dimensions", ("dataset_id", "dimension_id", "codelist_id"), None),
            "codes": ("silver", "eurostat_codes", ("codelist_id", "code", "name_en"), None),
        },
        "keyed_by": ("geo_map",),  # lookups that decide keys: their content is part of the recipe
    },
}


def table_ids(source: str, namespace: str = "gold") -> dict[str, tuple[str, str]]:
    return {name: (namespace, f"{source}_{name}") for name in TABLES}


def export_lookups(cat, source: str, directory: Path) -> tuple[dict[str, str], str]:
    """The source's lookup tables as local Parquet files (read once per run), and the digest of those
    that decide keys."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    spec = SOURCES[source]
    paths, digest = {}, sha256()
    for name, (namespace, table, columns, only_source) in spec["lookups"].items():
        data = bf.retry(lambda n=namespace, t=table, c=columns: cat.load_table((n, t)).scan(selected_fields=c), name)
        data = data.to_arrow()
        if only_source:
            data = data.filter(pc.equal(data.column("source_system"), only_source)).drop_columns(["source_system"])
        if name in spec["keyed_by"]:
            ordered = data.sort_by([(c, "ascending") for c in data.column_names])
            digest.update(json.dumps(ordered.to_pylist(), sort_keys=True, default=str).encode())
        paths[name] = str(directory / f"{name}.parquet")
        pq.write_table(data, paths[name])
    return paths, digest.hexdigest()


def model_digest(source: str, keys_digest: str) -> str:
    """Digest of everything but the Silver files that decides a block's Gold files."""
    digest = sha256(keys_digest.encode())
    spec = SOURCES[source]
    for model in (*spec.get("prepare", {}).values(), spec["build"], *(t["model"] for t in TABLES.values())):
        digest.update((MODELS / f"{model}.sql").read_bytes())
    return digest.hexdigest()[:12]


def blocks_of(source: str, silver_state: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Block -> its Silver files, their size, and the digest of their names."""
    block_of = SOURCES[source]["block_of"]
    out: dict[str, dict[str, Any]] = {}
    for entity, held in silver_state.items():
        if entity is None:
            continue
        item = out.setdefault(block_of(str(entity)), {"paths": [], "bytes": 0, "rows": 0})
        item["paths"] += held["paths"]
        item["bytes"] += held["bytes"]
        item["rows"] += held["rows"]
    for item in out.values():
        item["paths"].sort()
        names = "\n".join(et.recipe_of(p) for p in item["paths"])
        item["silver_recipe"] = sha256(names.encode()).hexdigest()[:16]
    return out


def plan(cat, source: str, digest: str, namespace: str, rejected_id: tuple[str, str], only: set[str] | None) -> dict:
    """What each block needs, from the tables' file lists: build (new Silver files, new rule), or removal."""
    silver = bf.retry(lambda: cat.load_table(SOURCES[source]["silver"]), "silver_table")
    blocks = blocks_of(source, et.state(silver, silver.spec().fields[0].name))
    if only:
        blocks = {b: v for b, v in blocks.items() if b in only}
    ids = table_ids(source, namespace)
    states = {}
    for name, table_id in ids.items():
        table = bf.retry(lambda t=table_id, n=name: bf.ensure_table(cat, t, TABLES[n]["columns"], ENTITY), name)
        states[name] = et.state(table, ENTITY)
    refused = bf.retry(lambda: bf.rejected_hashes(cat, rejected_id), "rejected")
    todo, rejected, expected = [], [], {}
    for block, item in sorted(blocks.items()):
        recipe = f"{item['silver_recipe']}.{digest}"
        if f"{source}:{block}:{recipe}" in refused:
            rejected.append(block)
            continue
        expected[block] = recipe
        if any(states[name].get(block, {}).get("recipes") != {recipe} for name in ids):
            todo.append({"block": block, "recipe": recipe, **item})
    held = set().union(*(set(state) for state in states.values()))
    gone = sorted(b for b in held if b not in expected and (only is None or b in only))
    return {"blocks": len(blocks), "todo": todo, "rejected": rejected, "expected": expected, "gone": gone, "ids": ids}


def work(source: str, item: dict[str, Any], lookups: dict[str, str], memory: str, tmp_root: str, prefix: str) -> dict:
    """One block: build stage, the three projections with their checks, upload. Runs in a worker process."""
    from warehouse import entity

    block, recipe = item["block"], item["recipe"]
    out: dict[str, Any] = {"block": block, "recipe": recipe, "silver_rows": item["rows"], "uris": {}}
    work_dir = Path(tempfile.mkdtemp(prefix=f"gold-{source}-", dir=tmp_root))
    started = time.monotonic()
    try:
        build = work_dir / "build.parquet"
        try:
            built = entity.apply(
                SOURCES[source]["build"],
                item["paths"],
                build,
                not_null=("measure_key", "geography_key", "date_key", "frequency", "breakdown_json", "is_total"),
                same_rows=False,
                lookups=lookups,
                memory_limit=memory,
                temp_directory=str(work_dir),
                source_view="silver",
                row_group_size=ROW_GROUP,
                prepare=SOURCES[source].get("prepare"),
            )
            if built["rows"] == 0:
                raise entity.EntityModelError("no row with a value and a period the calendar can place")
            reports = {}
            for name, table in TABLES.items():
                reports[name] = entity.apply(
                    table["model"],
                    str(build),
                    work_dir / f"{name}.parquet",
                    not_null=table["not_null"],
                    same_rows=name == "observations",
                    unique_key=table["unique_key"],
                    memory_limit=memory,
                    temp_directory=str(work_dir),
                    source_view="build",
                    row_group_size=ROW_GROUP,
                )
        except entity.EntityModelError as exc:  # verdict on the block under this recipe
            out.update(status="rejected", error=str(exc)[:1000])
            return out
        client, bucket = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
        for name in TABLES:
            key = f"{prefix}{source}/{name}/{block}/{recipe}.parquet"
            out["uris"][name] = bf.upload(client, bucket, key, work_dir / f"{name}.parquet", recipe)
        out.update(
            status="ok",
            rows=reports["observations"]["rows"],
            series=reports["series"]["rows"],
            measures=reports["measures"]["rows"],
            bytes=sum(r["bytes"] for r in reports.values()),
        )
        return out
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        out["seconds"] = round(time.monotonic() - started, 1)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, choices=sorted(SOURCES))
    parser.add_argument(
        "--minutes", type=float, default=50, help="time budget; unfinished blocks wait for the next run"
    )
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--only", nargs="*", help="block ids (trials)")
    parser.add_argument("--memory", default="4GB", help="DuckDB memory per worker for small blocks")
    parser.add_argument("--trial", default="", help="scratch namespace and prefix (Dev trials): writes only there")
    args = parser.parse_args(argv)
    source = args.source
    namespace, prefix, rejected_id = "gold", "04_gold/", REJECTED
    if args.trial:
        namespace, prefix, rejected_id = args.trial, f"{args.trial}/gold/", (args.trial, REJECTED[1])
    started = time.monotonic()
    deadline = started + args.minutes * 60
    cat = bf.retry(catalog, "catalog")
    tmp_root = os.environ.get("DUCKDB_TEMP_DIRECTORY") or tempfile.gettempdir()
    Path(tmp_root).mkdir(parents=True, exist_ok=True)
    lookup_dir = Path(tempfile.mkdtemp(prefix=f"gold-{source}-lookups-", dir=tmp_root))
    try:
        lookups, keys_digest = export_lookups(cat, source, lookup_dir)
        digest = model_digest(source, keys_digest)
        p = plan(cat, source, digest, namespace, rejected_id, {b.upper() for b in args.only} if args.only else None)
        ids = p["ids"]
        for table_id in ids.values():
            et.configure(table_id)
        todo = sorted(p["todo"], key=lambda c: c["rows"])
        totals = {"built": 0, "rows": 0, "series": 0, "measures": 0, "bytes": 0, "silver_rows": 0}
        rejected_now: list[dict[str, str]] = []
        failed: list[dict[str, str]] = []
        pending: list[dict[str, Any]] = []
        slowest: list[tuple[float, str, int]] = []
        last_commit = time.monotonic()

        def commit(removed: list[str] | None = None) -> None:
            nonlocal pending, last_commit
            bad = [r for r in pending if r["status"] == "rejected"]
            # observations last: a block counts as built only when all three tables hold its recipe
            for name in ("measures", "series", "observations"):
                wanted = {r["block"]: [r["uris"][name]] for r in pending if r["status"] == "ok"}
                wanted.update({r["block"]: [] for r in bad})  # a rejected block does not keep its old version
                wanted.update({block: [] for block in removed or []})
                if wanted:
                    et.replace(cat, ids[name], ENTITY, wanted, [ENTITY], f"{source}-gold-observations")
            bf.record_rejections(
                cat,
                rejected_id,
                [
                    {
                        "entity": f"{source}:{r['block']}",
                        "raw_sha256": f"{source}:{r['block']}:{r['recipe']}",
                        "landing_key": "",
                        "error": r["error"],
                        "recorded_at": datetime.now(UTC),
                        "code_sha": os.environ.get("GITHUB_SHA", "unknown"),
                    }
                    for r in bad
                ],
            )
            pending, last_commit = [], time.monotonic()

        def account(result: dict[str, Any]) -> None:
            if result["status"] == "failed":
                failed.append({"block": result["block"], "error": result["error"][:200]})
                return
            pending.append(result)
            if result["status"] == "ok":
                totals["built"] += 1
                for key in ("rows", "series", "measures", "bytes", "silver_rows"):
                    totals[key] += result[key]
                slowest.append((result["seconds"], result["block"], result["rows"]))
            else:
                rejected_now.append({"block": result["block"], "error": result["error"][:160]})

        def failure(c: dict[str, Any], exc: BaseException, cause: str = "") -> dict[str, Any]:
            prefix = f"{cause}; " if cause else ""
            return {"block": c["block"], "status": "failed", "error": f"{prefix}{type(exc).__name__}: {exc}"}

        # Workers start clean (spawn): the parent has read Iceberg metadata through Arrow's S3 client by now,
        # and a forked copy of that state is not safe to use.
        clean = multiprocessing.get_context("spawn")

        if p["gone"]:
            commit(removed=p["gone"])
        queue = [c for c in todo if c["rows"] <= BIG_ROWS]
        serial = [c for c in todo if c["rows"] > BIG_ROWS]
        while queue and time.monotonic() < deadline:
            broken = False
            with ProcessPoolExecutor(max_workers=args.workers, mp_context=clean) as pool:
                running: dict = {}
                while (queue or running) and not broken:
                    while queue and len(running) < args.workers and time.monotonic() < deadline:
                        c = queue.pop(0)
                        running[pool.submit(work, source, c, lookups, args.memory, tmp_root, prefix)] = c
                    if not running:
                        break
                    finished, _ = wait(running, timeout=60, return_when=FIRST_COMPLETED)
                    for f in finished:
                        c = running.pop(f)
                        try:
                            account(f.result())
                        except BrokenProcessPool:  # a worker was killed (memory): unknown which; retry alone
                            serial.append(c)
                            broken = True
                        except Exception:  # not a verdict on the block (memory, storage): once more, alone
                            serial.append(c)
                    if len(pending) >= COMMIT_EVERY or (pending and time.monotonic() - last_commit > COMMIT_SECONDS):
                        commit()
                if broken:
                    serial += list(running.values())
                    pool.shutdown(wait=False, cancel_futures=True)
            commit()
        for c in sorted(serial, key=lambda c: c["rows"]):
            if time.monotonic() > deadline:
                break
            with ProcessPoolExecutor(max_workers=1, mp_context=clean) as alone:  # a kill fails that block only
                future = alone.submit(work, source, c, lookups, "10GB", tmp_root, prefix)
                workers = list((alone._processes or {}).values())  # kept to read the exit code if it dies
                try:
                    account(future.result())
                except BrokenProcessPool as exc:
                    from ingestion.to_bronze.eurostat.entities import worker_exit

                    account(failure(c, exc, worker_exit(workers)))
                except Exception as exc:
                    account(failure(c, exc))
            commit()
    finally:
        shutil.rmtree(lookup_dir, ignore_errors=True)
    left = len(todo) - totals["built"] - len(rejected_now)
    minutes = (time.monotonic() - started) / 60
    summary = {
        "operation": f"{source}-gold-observations",
        "result": "complete" if left == 0 else ("failures" if failed else "incomplete_time_budget"),
        "blocks_in_silver": p["blocks"],
        "todo": len(todo),
        "remaining": left,
        **totals,
        "removed": len(p["gone"]),
        "rejected": len(rejected_now),
        "rejected_blocks": rejected_now[:8],
        "rejected_earlier": len(p["rejected"]),
        "failed": len(failed),
        "failed_blocks": failed[:5],
        "slowest": [list(s) for s in sorted(slowest, reverse=True)[:5]],
        "minutes": round(minutes, 1),
        "rows_per_minute": round(totals["rows"] / minutes) if minutes else 0,
    }
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if left == 0 and not rejected_now else "warning", f"{source} gold observations", summary)
    return 0 if left == 0 else 2


def completeness(source: str) -> dict[str, Any]:
    """Gold against Silver for the source, from file lists only: blocks holding the expected recipe."""
    cat = bf.retry(catalog, "catalog")
    with tempfile.TemporaryDirectory() as directory:
        _, keys_digest = export_lookups(cat, source, Path(directory))
    from pyiceberg.exceptions import NoSuchTableError

    try:
        p = plan(cat, source, model_digest(source, keys_digest), "gold", REJECTED, None)
    except NoSuchTableError as exc:
        return {"source": source, "error": f"table missing: {exc}"}
    return {
        "source": source,
        "blocks_in_silver": p["blocks"],
        "gold_complete": len(p["expected"]) - len(p["todo"]),
        "to_build": len(p["todo"]),
        "rejected": len(p["rejected"]),
        "rejected_sample": p["rejected"][:10],
        "to_remove": len(p["gone"]),
    }


if __name__ == "__main__":
    raise SystemExit(main())
