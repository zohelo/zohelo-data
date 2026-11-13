#!/usr/bin/env python3
"""Gold observations of a big source, one block at a time (docs/gold-coverage.md, owner direction O36).

Every loaded Silver block of the source (Eurostat: a dataset) is put into Gold by rule, without an
activation list: the block's Silver files go through the source's build stage
(warehouse/entity/gold_<source>_*.sql, the common observation shape) and three projections shared
by all sources (gold_observations.sql, gold_series.sql, gold_measures.sql). Gold holds exactly one
file per block in each table, as Silver does (design #875):

  04_gold/<source>/<table>/<block>/<recipe>.<content>.parquet   gold.<source>_observations | _series |
                                                                  _measures | _geography

``<content>`` is a digest of the file's own bytes, so an upload never replaces an object: a key a table
holds is never rewritten (a rebuild after an interrupted run writes new keys), and an existing key
with other bytes stops the run. ``<recipe>`` names what the file was made from: the block's Silver
files (their names carry the Landing SHA-256 and the Silver model), the Gold SQL, and the mapping of
the geography codes the block holds. A block is rebuilt only when its recipe changed: a new source
version rebuilds that block, a changed rule rebuilds every block, a changed row of gold.map_geography
rebuilds the blocks that hold that code (gold.<source>_geography lists each block's codes), and a
block that left Silver leaves Gold. What Gold holds is read from the tables' file lists; data is read
only to find the blocks a changed geography map touches. The four tables are committed one after
the other (observations last); a block counts as built only when all four hold the same recipe, so
a run that stops between the commits rebuilds the block next time.

Limits of a run (decision D35; these replace the #822 row ceilings, which are for dbt models): the
time budget; a block estimated to need more than the time left waits for a run with enough; at most
MAX_REMOVE_SHARE of the blocks held may be removed in one run, so a partial Silver listing cannot empty
Gold (above it nothing is removed and the run fails).

The grain is checked on every block before it is accepted: one row per series_key × date_key.
measure_key begins with the source and the block, so a duplicate can only occur inside one block
and the per-block check covers the whole table. A block that breaks the grain or yields no rows is
rejected (gold.observations_rejected, keyed by its recipe and the geography map) and never loaded in
part; the block keeps the version Gold already holds, if any, and stays listed as rejected. A block
that fails for another reason (storage, memory) is tried once more alone with the full memory limit,
then reported and retried by the next run. Work runs in parallel processes; finished blocks are committed in chunks,
so a run that stops resumes where it stopped. Prints one JSON result line.

Load order (owner decision O43): blocks that hold Polish regions, then blocks that hold Poland, another
country or an aggregate such as the EU, then the rest; smaller blocks first within a group. The group of a
waiting block comes from the geography codes of its Silver files (one column, read once per run for the
blocks to build): the structure messages Eurostat publishes name one GEO code list for all datasets, not
the codes of each.

The Pipeline activity ``gold.observations`` (run.py) calls ``activity``: a run that reaches its time budget
ends green with a warning and the next run continues; a run without a filter only continues a load (Gold
already holds a block of the source), so the first load of a source is started by hand, with a filter.
Every run ends with a coverage annotation: series in Gold over loaded series.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime
from fnmatch import fnmatchcase
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
MAX_REMOVE_SHARE = 0.05  # of the blocks Gold holds; at least MIN_REMOVABLE may always go
MIN_REMOVABLE = 10
# A block built alone is started only when its estimate fits the time left. Silver bytes, not rows: rows
# count empty cells, which cost little (AVIA_TF_ACA: 3,633 M Silver rows, 241 MB, built in 26 min;
# AVIA_TF_ACC: 92 MB in 13 min; Dev runs 43268232691, 43202733905). 6 MB a minute is below both.
SERIAL_BYTES_PER_MINUTE = 6_000_000
MAP_DIGEST = "zohelo.geography_map_digest"  # on gold.<source>_geography: the map its rows were last checked against
# Load order (O43). Areas of dim_geography below the country level carry a prefix; the Polish ones are
# TERC: (voivodeship and below), BDL: (podregion) and NUTS:PL (makroregion, region).
GROUPS = ("Polish regions", "countries and aggregates", "other")
POLISH_REGION_KEYS = ("TERC:", "BDL:", "NUTS:PL")
REGION_KEYS = ("TERC:", "BDL:", "NUTS:")
PROBE_FILES = 200  # Silver files per query when the geography codes of waiting blocks are read
PROBE_SHARE = 0.1  # of the run's time budget; blocks not read in that time are ordered as group 2

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

TABLES["geography"] = {
    "model": "gold_geography",
    "unique_key": ("geography_source_code",),
    "not_null": ("block_id", "geography_key"),
    "reject_if": "geography_keys > 1",  # one source code mapped to two areas
    "columns": [
        ("block_id", "VARCHAR"),
        ("geography_source_code", "VARCHAR"),
        ("geography_key", "VARCHAR"),
        ("geography_keys", "BIGINT"),
        ("points", "BIGINT"),
    ],
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
        "geography_column": "geo_code",  # of the Silver table: decides the load order (O43)
        # lookup, block column, dimension column, the geography dimension: a block the structure gives no
        # geography dimension holds no area, and its Silver files are not read for the load order
        "geography_dimension": ("dims", "dataset_id", "dimension_id", "geo"),
        "polish_code": "PL",  # an unmatched code that starts with it and is longer is a Polish region
    },
}


def table_ids(source: str, namespace: str = "gold") -> dict[str, tuple[str, str]]:
    return {name: (namespace, f"{source}_{name}") for name in TABLES}


def export_lookups(cat, source: str, directory: Path) -> tuple[dict[str, str], dict[str, str]]:
    """The source's lookup tables as local Parquet files (read once per run), and its geography map
    (source code -> geography_key)."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    spec = SOURCES[source]
    paths, geo_map = {}, {}
    for name, (namespace, table, columns, only_source) in spec["lookups"].items():
        data = bf.retry(lambda n=namespace, t=table, c=columns: cat.load_table((n, t)).scan(selected_fields=c), name)
        data = data.to_arrow()
        if only_source:
            data = data.filter(pc.equal(data.column("source_system"), only_source)).drop_columns(["source_system"])
        if name in spec["keyed_by"]:
            codes, keys = data.column("source_code").to_pylist(), data.column("geography_key").to_pylist()
            geo_map = dict(zip(codes, keys, strict=True))
        paths[name] = str(directory / f"{name}.parquet")
        pq.write_table(data, paths[name])
    return paths, geo_map


def expected_area(geo_map: dict[str, str], code: str | None) -> str:
    """The geography_key the build stage gives a source code: no code `_Z`, unmatched `_X`."""
    return "_Z" if code is None else geo_map.get(code, "_X")


def recipe_of_file(path: str) -> str:
    """The recipe in a Gold file name: ``<recipe>.<content>.parquet`` -> ``<recipe>``."""
    return et.recipe_of(path).rsplit(".", 1)[0]


def upload_new(client, bucket: str, key: str, path: Path, recipe: str) -> str:
    """Upload a Gold file under a key that is never rewritten. The key ends in a digest of the bytes; an
    object already under it with the same digest is the same file (a repeated attempt) and is kept; one
    with other bytes is never deleted or replaced: the run stops, because a table may point to it."""
    import botocore.exceptions

    digest = sha256(path.read_bytes()).hexdigest()
    try:
        held = (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256")
    except botocore.exceptions.ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code", "")) not in {"404", "NoSuchKey", "NotFound"}:
            raise
        held = None
    if held == digest:
        return f"s3://{bucket}/{key}"
    if held is not None:
        raise RuntimeError(f"{key}: another file is stored under this key; it is not replaced")
    metadata = {"sha256": digest, "raw-sha256": recipe}
    client.upload_file(
        str(path), bucket, key, ExtraArgs={"ContentType": "application/vnd.apache.parquet", "Metadata": metadata}
    )
    if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Gold upload verification failed: {key}")
    return f"s3://{bucket}/{key}"


def geography_digest(pairs: list[tuple[str | None, str]]) -> str:
    """Digest of a block's geography codes and the areas they map to."""
    return sha256(json.dumps(sorted(pairs, key=lambda p: (p[0] or "", p[1]))).encode()).hexdigest()[:8]


def model_digest(source: str) -> str:
    """Digest of the Gold SQL of the source: with the Silver files and the block's geography mapping it
    decides a block's Gold files."""
    digest = sha256()
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


def remapped_blocks(table, geo_map: dict[str, str]) -> tuple[set[str], str]:
    """Blocks holding a geography code whose mapping differs from the map now, and the map's digest.
    The table's rows are read only when the map changed since they were last checked (table property)."""
    digest = sha256(json.dumps(sorted(geo_map.items())).encode()).hexdigest()[:16]
    if table.properties.get(MAP_DIGEST) == digest or table.current_snapshot() is None:
        return set(), digest
    held = table.scan(selected_fields=("block_id", "geography_source_code", "geography_key")).to_arrow()
    rows = zip(  # noqa: B905 - columns of one arrow table
        held.column("block_id").to_pylist(),
        held.column("geography_source_code").to_pylist(),
        held.column("geography_key").to_pylist(),
    )
    return {block for block, code, key in rows if expected_area(geo_map, code) != key}, digest


def area_group(source: str, geo_map: dict[str, str], codes: set[str]) -> int:
    """The load-order group (index of GROUPS) of a block that holds these geography codes."""
    polish = SOURCES[source].get("polish_code", "")
    group = 2
    for code in codes:
        key = geo_map.get(code, "_X")
        if key.startswith(POLISH_REGION_KEYS) or (
            key == "_X" and polish and code.startswith(polish) and code != polish
        ):
            return 0
        if key not in ("_X", "_Z") and not key.startswith(REGION_KEYS):
            group = 1
    return group


def without_geography(source: str, lookups: dict[str, str]) -> set[str] | None:
    """Blocks whose structure has no geography dimension (upper case), from the source's structure lookup;
    None when the source declares no such lookup. A block the lookup does not list is not in the set."""
    import pyarrow.parquet as pq

    spec = SOURCES[source].get("geography_dimension")
    if not spec:
        return None
    lookup, block_column, dimension_column, dimension = spec
    table = pq.read_table(lookups[lookup], columns=[block_column, dimension_column])
    listed: dict[str, bool] = {}
    for block, held in zip(table.column(0).to_pylist(), table.column(1).to_pylist(), strict=True):
        if block is not None:
            listed[block.upper()] = listed.get(block.upper(), False) or str(held).lower() == dimension
    return {block for block, has in listed.items() if not has}


def load_order(
    source: str, todo: list[dict[str, Any]], geo_map: dict[str, str], seconds: float, no_area: set[str] | None = None
) -> dict[str, int]:
    """Block -> load-order group. A block without a geography dimension (``no_area``, from the structure)
    is group 2 and nothing is read for it. For the others the group comes from the geography column of the
    Silver files of the blocks to build (one column; nothing is read for a block that needs no build). A block
    whose files could not be read within ``seconds`` is ordered with the countries (group 1)."""
    import duckdb

    from warehouse import entity

    column = SOURCES[source].get("geography_column")
    groups = {c["block"]: 2 if c["block"] in (no_area or ()) else 1 for c in todo}
    todo = [c for c in todo if c["block"] not in (no_area or ())]
    if not column or not todo:
        return groups
    # DuckDB names a file as it was given; the scheme is dropped in case a version spells it another way
    block_of_path = {path.split("://", 1)[-1]: c["block"] for c in todo for path in c["paths"]}
    codes: dict[str, set[str]] = {c["block"]: set() for c in todo}
    read: set[str] = set()
    paths = [path for c in sorted(todo, key=lambda c: c["rows"]) for path in c["paths"]]
    deadline = time.monotonic() + seconds
    con = duckdb.connect()
    try:
        if any(path.startswith("s3://") for path in paths):
            entity._remote(con)
        for start in range(0, len(paths), PROBE_FILES):
            if time.monotonic() > deadline:
                break
            batch = paths[start : start + PROBE_FILES]
            files = ", ".join("'" + path.replace("'", "''") + "'" for path in batch)
            try:
                rows = con.execute(
                    f"SELECT DISTINCT filename, {column} FROM read_parquet([{files}], filename = true)"
                ).fetchall()
            except duckdb.Error:  # the order is a preference: a batch that cannot be read keeps group 1
                continue
            named = [(path.split("://", 1)[-1], code) for path, code in rows]
            if any(path not in block_of_path for path, _ in named):  # not one of the files asked for
                continue
            for path, code in named:
                if code is not None:
                    codes[block_of_path[path]].add(code)
            read.update(block_of_path[path.split("://", 1)[-1]] for path in batch)
    finally:
        con.close()
    for block in read:
        groups[block] = area_group(source, geo_map, codes[block])
    return groups


def matches(only: set[str] | None, block: str) -> bool:
    """Whether a block is in the run's filter: block ids or globs, compared in upper case; no filter = all."""
    return only is None or any(fnmatchcase(block.upper(), pattern) for pattern in only)


def blocks_in_gold(cat, source: str, namespace: str = "gold") -> int:
    """Blocks the source's observations table holds; 0 when the table does not exist. Reads metadata only."""
    from pyiceberg.exceptions import NoSuchTableError

    try:
        table = bf.retry(lambda: cat.load_table(table_ids(source, namespace)["observations"]), "observations")
    except NoSuchTableError:
        return 0
    return len(et.state(table, ENTITY))


def coverage(source: str, p: dict, built: list[dict[str, Any]], removed: set[str]) -> dict[str, Any]:
    """Series in Gold over loaded series, after a run (docs/gold-coverage.md, decision 10), from the tables'
    file lists as the plan read them plus the run's own results; no data is read.

    A block in Gold holds every loaded series that no stated rule excludes (it is accepted whole), so its
    loaded series are its Gold series: the row count of its file in gold.<source>_series. The loaded series
    of a block that waits or is rejected are not known before its first build; they are estimated from its
    Silver rows at the series-per-Silver-row ratio of the blocks in Gold, and the result says so."""
    series = dict(p["series_held"])
    for block in removed:
        series.pop(block, None)
    for result in built:
        series[result["block"]] = result["series"]
    silver_rows = p["silver_rows"]
    in_gold = sum(series.values())
    rows_in_gold = sum(silver_rows.get(block, 0) for block in series)
    rows_waiting = sum(rows for block, rows in silver_rows.items() if block not in series)
    waiting = sum(1 for block in silver_rows if block not in series)
    estimate = round(in_gold / rows_in_gold * rows_waiting) if rows_in_gold and rows_waiting else 0
    out = {
        "blocks_in_gold": len(series),
        "blocks_in_silver": len(silver_rows),
        "blocks_not_in_gold": waiting,
        "series_in_gold": in_gold,
        "loaded_series": in_gold + estimate,
        "loaded_series_estimated": estimate,  # of the blocks not in Gold; 0 = the number is exact
        "coverage_pct": round(100 * in_gold / (in_gold + estimate), 2) if in_gold + estimate else 0.0,
        "silver_rows_in_gold_blocks_pct": round(100 * rows_in_gold / (rows_in_gold + rows_waiting), 2)
        if rows_in_gold + rows_waiting
        else 0.0,
    }
    if waiting and not rows_in_gold:
        out["coverage_pct"] = 0.0
    return out


def plan(
    cat,
    source: str,
    digest: str,
    geo_map: dict[str, str],
    namespace: str,
    rejected_id: tuple[str, str],
    only: set[str] | None,
    create: bool = True,
) -> dict:
    """What each block needs, from the tables' file lists: build (new Silver files, new rule, a changed
    mapping of one of its geography codes), or removal (the block left Silver).
    ``create=False`` (checks): reads only, never creates a table."""
    silver = bf.retry(lambda: cat.load_table(SOURCES[source]["silver"]), "silver_table")
    listed = blocks_of(source, et.state(silver, silver.spec().fields[0].name))
    if not listed:
        raise RuntimeError(f"{source}: the Silver table lists no blocks; nothing is built or removed")
    blocks = {b: v for b, v in listed.items() if matches(only, b)}
    ids = table_ids(source, namespace)
    states, tables = {}, {}
    for name, table_id in ids.items():
        if create:
            load = lambda t=table_id, n=name: bf.ensure_table(cat, t, TABLES[n]["columns"], ENTITY)  # noqa: E731
        else:
            load = lambda t=table_id: cat.load_table(t)  # noqa: E731
        tables[name] = bf.retry(load, name)
        states[name] = et.state(tables[name], ENTITY)
    remapped, map_digest = remapped_blocks(tables["geography"], geo_map)
    refused = bf.retry(lambda: bf.rejected_hashes(cat, rejected_id), "rejected")
    todo, rejected, complete = [], [], []
    for block, item in sorted(blocks.items()):
        recipe = f"{item['silver_recipe']}.{digest}"  # the worker adds the block's geography digest
        if f"{source}:{block}:{recipe}.{map_digest}" in refused:  # a fixed map retries the block
            rejected.append(block)
            continue
        held = [{recipe_of_file(p) for p in states[name].get(block, {}).get("paths", [])} for name in ids]
        current = len(held[0]) == 1 and all(h == held[0] for h in held) and next(iter(held[0])).startswith(recipe + ".")
        if current and block not in remapped:
            complete.append(block)
        else:
            todo.append({"block": block, "recipe": recipe, **item})
    held_blocks = set().union(*(set(state) for state in states.values()))
    # a block leaves Gold only when it left Silver; a rejected new version keeps the version Gold holds
    gone = sorted(b for b in held_blocks if b not in listed and matches(only, b))
    # for the coverage annotation: a block is in Gold when the observations table holds it
    series_held = {b: states["series"].get(b, {}).get("rows", 0) for b in states["observations"]}
    return {
        "blocks": len(blocks),
        "series_held": series_held,
        "silver_rows": {b: v["rows"] for b, v in listed.items()},
        "todo": todo,
        "rejected": rejected,
        "complete": complete,
        "gone": gone,
        "held": len(held_blocks),
        "remapped": len(remapped & set(blocks)),
        "map_digest": map_digest,
        "ids": ids,
        "tables": tables,
    }


def work(source: str, item: dict[str, Any], lookups: dict[str, str], memory: str, tmp_root: str, prefix: str) -> dict:
    """One block: build stage, the projections with their checks, upload. Runs in a worker process."""
    import duckdb

    from warehouse import entity

    block = item["block"]
    out: dict[str, Any] = {"block": block, "recipe": item["recipe"], "silver_rows": item["rows"], "uris": {}}
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
                    reject_if=table.get("reject_if", ""),
                    memory_limit=memory,
                    temp_directory=str(work_dir),
                    source_view="build",
                    row_group_size=ROW_GROUP,
                )
        except entity.EntityModelError as exc:  # verdict on the block under this recipe
            out.update(status="rejected", error=str(exc)[:1000])
            return out
        geography = str(work_dir / "geography.parquet").replace("'", "''")
        pairs = duckdb.sql(f"select geography_source_code, geography_key from read_parquet('{geography}')").fetchall()
        recipe = f"{item['recipe']}.{geography_digest(pairs)}"
        client, bucket = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
        for name in TABLES:
            path = work_dir / f"{name}.parquet"
            content = sha256(path.read_bytes()).hexdigest()[:16]
            key = f"{prefix}{source}/{name}/{block}/{recipe}.{content}.parquet"
            out["uris"][name] = upload_new(client, bucket, key, path, recipe)
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


def run(argv=None) -> dict[str, Any]:
    """One run of the builder; returns its summary (``result`` says how it ended)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, choices=sorted(SOURCES))
    parser.add_argument(
        "--minutes", type=float, default=50, help="time budget; unfinished blocks wait for the next run"
    )
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--only", nargs="*", help="block ids or globs: the run builds and removes only these")
    parser.add_argument("--memory", default="4GB", help="DuckDB memory per worker for small blocks")
    parser.add_argument("--trial", default="", help="scratch namespace and prefix (Dev trials): writes only there")
    parser.add_argument(
        "--continue-only",
        action="store_true",
        help="build only when Gold already holds a block of the source (runs without a filter: the first load "
        "of a source is started by hand)",
    )
    args = parser.parse_args(argv)
    source = args.source
    namespace, prefix, rejected_id = "gold", "04_gold/", REJECTED
    if args.trial:
        namespace, prefix, rejected_id = args.trial, f"{args.trial}/gold/", (args.trial, REJECTED[1])
    started = time.monotonic()
    deadline = started + args.minutes * 60
    cat = bf.retry(catalog, "catalog")
    if args.continue_only and not blocks_in_gold(cat, source, namespace):
        summary = {
            "operation": f"{source}-gold-observations",
            "result": "not_started",
            "reason": "Gold holds no block of this source and the run has no filter: the first load is started "
            "by hand, Pipeline run `gold: observations` with filter `*` (every block) or block ids",
        }
        print(json.dumps(summary, sort_keys=True), flush=True)
        annotate("notice", f"{source} gold observations", summary)
        return summary
    tmp_root = os.environ.get("DUCKDB_TEMP_DIRECTORY") or tempfile.gettempdir()
    Path(tmp_root).mkdir(parents=True, exist_ok=True)
    lookup_dir = Path(tempfile.mkdtemp(prefix=f"gold-{source}-lookups-", dir=tmp_root))
    try:
        lookups, geo_map = export_lookups(cat, source, lookup_dir)
        only = {b.upper() for b in args.only} if args.only else None
        p = plan(cat, source, model_digest(source), geo_map, namespace, rejected_id, only)
        ids = p["ids"]
        for table_id in ids.values():
            et.configure(table_id)
        # O43: Polish regions, then countries and aggregates, then the rest; smaller blocks first in a group
        groups = load_order(
            source, p["todo"], geo_map, args.minutes * 60 * PROBE_SHARE, without_geography(source, lookups)
        )
        todo = sorted(p["todo"], key=lambda c: (groups[c["block"]], c["rows"]))
        totals = {"built": 0, "rows": 0, "series": 0, "measures": 0, "bytes": 0, "silver_rows": 0}
        built: list[dict[str, Any]] = []
        built_by_group = [0, 0, 0]
        rejected_now: list[dict[str, str]] = []
        failed: list[dict[str, str]] = []
        waiting: list[str] = []
        pending: list[dict[str, Any]] = []
        slowest: list[tuple[float, str, int]] = []
        last_commit = time.monotonic()

        def commit(removed: list[str] | None = None) -> None:
            nonlocal pending, last_commit
            bad = [r for r in pending if r["status"] == "rejected"]
            # observations last: a block counts as built only when every table holds its recipe
            for name in (*(n for n in TABLES if n != "observations"), "observations"):
                wanted = {r["block"]: [r["uris"][name]] for r in pending if r["status"] == "ok"}
                wanted.update({block: [] for block in removed or []})
                if wanted:
                    et.replace(cat, ids[name], ENTITY, wanted, [ENTITY], f"{source}-gold-observations")
            bf.record_rejections(
                cat,
                rejected_id,
                [
                    {
                        "entity": f"{source}:{r['block']}",
                        "raw_sha256": f"{source}:{r['block']}:{r['recipe']}.{p['map_digest']}",
                        "landing_key": "",
                        "error": r["error"],
                        "recorded_at": datetime.now(UTC),
                        "code_sha": os.environ.get("GITHUB_SHA", "unknown"),
                    }
                    for r in bad
                ],
            )
            built.extend(r for r in pending if r["status"] == "ok")  # in Gold only once committed
            pending, last_commit = [], time.monotonic()

        def account(result: dict[str, Any]) -> None:
            if result["status"] == "failed":
                failed.append({"block": result["block"], "error": result["error"][:200]})
                return
            pending.append(result)
            if result["status"] == "ok":
                totals["built"] += 1
                built_by_group[groups[result["block"]]] += 1
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
        removable = max(MIN_REMOVABLE, int(p["held"] * MAX_REMOVE_SHARE))
        removal_refused = len(p["gone"]) if len(p["gone"]) > removable else 0
        if p["gone"] and not removal_refused:
            commit(removed=p["gone"])
        for group in range(len(GROUPS)):  # a group is finished, its large blocks included, before the next starts
            queue = [c for c in todo if groups[c["block"]] == group and c["rows"] <= BIG_ROWS]
            serial = [c for c in todo if groups[c["block"]] == group and c["rows"] > BIG_ROWS]
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
                        if len(pending) >= COMMIT_EVERY or (
                            pending and time.monotonic() - last_commit > COMMIT_SECONDS
                        ):
                            commit()
                    if broken:
                        serial += list(running.values())
                        pool.shutdown(wait=False, cancel_futures=True)
                commit()
            for c in sorted(serial, key=lambda c: c["rows"]):
                # the budget is checked before a block starts: one that cannot finish in the time left waits
                if c["bytes"] / SERIAL_BYTES_PER_MINUTE > (deadline - time.monotonic()) / 60:
                    waiting.append(c["block"])
                    continue
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
        left = len(todo) - totals["built"] - len(rejected_now)
        if left == 0 and not removal_refused and only is None:  # every block is checked against this map
            with p["tables"]["geography"].refresh().transaction() as tx:
                tx.set_properties({MAP_DIGEST: p["map_digest"]})
    finally:
        shutil.rmtree(lookup_dir, ignore_errors=True)
    minutes = (time.monotonic() - started) / 60
    result = "complete" if left == 0 else ("failures" if failed else "incomplete_time_budget")
    summary = {
        "operation": f"{source}-gold-observations",
        "result": "removal_refused" if removal_refused else result,
        "blocks_in_silver": p["blocks"],
        "todo": len(todo),
        "todo_by_group": dict(
            zip(GROUPS, [sum(1 for c in todo if groups[c["block"]] == g) for g in range(3)], strict=True)
        ),
        "built_by_group": dict(zip(GROUPS, built_by_group, strict=True)),
        "remapped_geography": p["remapped"],
        "remaining": left,
        **totals,
        "removed": 0 if removal_refused else len(p["gone"]),
        "removal_refused": removal_refused,
        "rejected": len(rejected_now),
        "rejected_blocks": rejected_now[:8],
        "rejected_earlier": len(p["rejected"]),
        "failed": len(failed),
        "failed_blocks": failed[:5],
        "waiting_for_a_longer_run": waiting[:8],
        "slowest": [list(s) for s in sorted(slowest, reverse=True)[:5]],
        "minutes": round(minutes, 1),
        "rows_per_minute": round(totals["rows"] / minutes) if minutes else 0,
    }
    print(json.dumps(summary, sort_keys=True), flush=True)
    clean_run = left == 0 and not rejected_now and not removal_refused
    annotate("notice" if clean_run else "warning", f"{source} gold observations", summary)
    covered = coverage(source, p, built, set() if removal_refused else set(p["gone"]))
    if only is not None:
        covered["filter"] = sorted(only)[:10]  # the numbers are the source's, whatever the filter
    annotate("notice", f"{source} gold coverage", covered)
    return {**summary, "coverage": covered}


def main(argv=None) -> int:
    """Command line: 0 when every block is built, 2 when blocks are left or a removal was refused."""
    return 0 if run(argv)["result"] in ("complete", "not_started") else 2


def activity(source: str, minutes: float, block_filter: str = "") -> int:
    """The Pipeline activity ``gold.observations``. ``block_filter``: block ids or globs, comma separated;
    `*` is every block; empty continues a load that was started and never starts the first one. A run that
    reaches its time budget is not a failure: its blocks are committed and the next run continues (0, with a
    warning annotation). Failed blocks or a refused removal fail the run (2)."""
    argv = ["--source", source, "--minutes", f"{max(minutes, 5):.0f}"]
    patterns = [part.strip() for part in block_filter.replace(" ", ",").split(",") if part.strip()]
    if not patterns:
        argv.append("--continue-only")
    elif "*" not in patterns:
        argv += ["--only", *patterns]
    return 0 if run(argv)["result"] in ("complete", "not_started", "incomplete_time_budget") else 2


def completeness(source: str) -> dict[str, Any]:
    """Gold against Silver for the source, from file lists (and the geography rows when the map changed):
    blocks holding the expected recipe. Reads only."""
    from pyiceberg.exceptions import NoSuchTableError

    cat = bf.retry(catalog, "catalog")
    with tempfile.TemporaryDirectory() as directory:
        _, geo_map = export_lookups(cat, source, Path(directory))
    try:
        p = plan(cat, source, model_digest(source), geo_map, "gold", REJECTED, None, create=False)
    except NoSuchTableError as exc:
        return {"source": source, "error": f"table missing: {exc}"}
    return {
        "source": source,
        "blocks_in_silver": p["blocks"],
        "gold_complete": len(p["complete"]),
        "to_build": len(p["todo"]),
        "remapped_geography": p["remapped"],
        "rejected": len(p["rejected"]),
        "rejected_sample": p["rejected"][:10],
        "to_remove": len(p["gone"]),
    }


if __name__ == "__main__":
    raise SystemExit(main())
