"""Dev trial (PL-ENV-007, #3106): GUGiK PRG with the address package, Landing → Gold on the final code.

Landing extract of both declared packages (boundaries, address points) into the Landing prefix zz_dev_prg_addresses/
twice (the second must write nothing: same SHA-256), inspect and verify; Bronze load (load.load: boundaries, then the
address entity loader with its Silver model) into the Iceberg namespaces zz_dev_prg_addresses (Bronze) and
zz_dev_prg_addresses_silver (Silver) and the same lake prefixes, twice (the second writes nothing); profiles of the
Bronze and Silver rows; the dbt Silver model prg_units, the geography map, the Gold count model, the build stage and
the source's tests rendered with Jinja from warehouse/ and run in local DuckDB, each twice; the grain check by
series_key × date_key and spot values for Poland; peak memory and time of every step. dim_geography is read
(read-only) from production. Everything written is removed in `finally`. At most 10 annotations.
"""

from __future__ import annotations

from pathlib import Path
import re
import resource
import time

import duckdb
from jinja2 import Environment

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gugik_prg import entities
from ingestion.to_bronze.gugik_prg import load as prg_load
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gugik_prg"
SCRATCH = "zz_dev_prg_addresses"
SILVER = "zz_dev_prg_addresses_silver"
NOTES = {"left": 9}


def note(title: str, data, level: str = "notice") -> None:
    if NOTES["left"] > 0:
        NOTES["left"] -= 1
        annotate(level, title, data)
    print(f"[{title}] {data}", flush=True)


def rss_mb() -> dict:
    return {
        "self": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024),
        "children": round(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024),
    }


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def render(path: str, refs: dict[str, str], sources: dict[str, str], prefix: str = "") -> str:
    return (
        Environment()
        .from_string(prefix + Path(path).read_text())
        .render(
            config=lambda **_k: "",
            ref=lambda n: refs.get(n, n),
            source=lambda _s, t: sources[t],
            poc_optional_ref=lambda m, _c: refs[m],
        )
    )


def files_of(cat, table_id):
    table = cat.load_table(table_id)
    f = table.inspect.files()
    return {
        "files": f.num_rows,
        "rows": sum(f.column("record_count").to_pylist()),
        "bytes": sum(f.column("file_size_in_bytes").to_pylist()),
        "snapshot": table.current_snapshot().snapshot_id if table.current_snapshot() else None,
        "paths": f.column("file_path").to_pylist(),
    }


def trial(cat) -> None:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    specs = bulk_file.declaration(SOURCE)
    runs = []
    for attempt in (1, 2):
        for spec in specs:
            started = time.monotonic()
            result = bulk_file.load_file(client, bucket, SOURCE, spec, SCRATCH)
            runs.append(
                {
                    "run": attempt,
                    "entity": spec["entity"],
                    "outcome": result.get("outcome"),
                    "size_mb": result.get("size_mb"),
                    "version": result.get("version"),
                    "seconds": round(time.monotonic() - started),
                }
            )
    held = {
        s["entity"]: bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, s["entity"], SCRATCH)) for s in specs
    }
    inspect = bulk_file.main(SOURCE, "inspect", landing_root=SCRATCH, specs=specs)
    verify = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=specs)
    note(
        "prg landing (scratch)",
        {
            "extract_two_runs": runs,
            "inspect_exit": inspect,
            "verify_exit": verify,
            "receipts": {
                e: [
                    {
                        "size": r["size_bytes"],
                        "version": r["version_token"],
                        "basis": r.get("version_basis"),
                        "sha": r["sha256"][:12],
                        "etag": r["detail"].get("http", {}).get("etag"),
                    }
                    for r in h
                ]
                for e, h in held.items()
            },
        },
    )

    loads = []
    tables = {
        "boundaries": (SCRATCH, "prg_boundaries"),
        "bronze_addresses": (SCRATCH, "prg_address_points"),
        "silver_addresses": (SILVER, "prg_address_points"),
    }
    for _ in (1, 2):
        started = time.monotonic()
        code = prg_load.load(300, landing_root=SCRATCH, namespace=SCRATCH, silver_namespace=SILVER)
        state = {k: files_of(cat, t) for k, t in tables.items()}
        loads.append(
            {
                "exit": code,
                "seconds": round(time.monotonic() - started),
                "rss_mb": rss_mb(),
                **{k: {x: v[x] for x in ("files", "rows", "bytes", "snapshot")} for k, v in state.items()},
            }
        )
    try:
        rejected = cat.load_table((SCRATCH, "prg_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    unchanged = all(loads[0][k]["snapshot"] == loads[1][k]["snapshot"] for k in tables)
    note(
        "prg bronze and silver loads (scratch, two runs)",
        {
            "loads": loads,
            "second_run_wrote_nothing": unchanged,
            "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3],
        },
    )

    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'")
    from warehouse import entity

    entity._remote(con)
    bronze_paths = state["bronze_addresses"]["paths"]
    silver_paths = state["silver_addresses"]["paths"]
    con.execute(f"create view bronze_a as select * from read_parquet({bronze_paths!r})")
    con.execute(f"create table silver_a as select * from read_parquet({silver_paths!r})")
    attrs = [a.lower() for a in entities.ATTRIBUTES]
    note(
        "prg address bronze profile",
        {
            "rows_ids_versions": q(
                con,
                "select count(*), count(distinct id_iip), min(source_version), max(source_version), "
                "count(distinct raw_sha256), any_value(geometry_type), count(distinct crs_wkt) from bronze_a",
            ),
            "null_by_column": dict(
                zip(
                    attrs,
                    con.execute(
                        "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in attrs) + " from bronze_a"
                    ).fetchone(),
                    strict=True,
                )
            ),
            "postal_not_NN_NNN": q(
                con,
                "select kod_poczt, count(*) from bronze_a where not regexp_full_match(kod_poczt, '[0-9]{2}-[0-9]{3}') "
                "group by 1 order by 2 desc limit 8",
            ),
            "ulic_not_5_digits": q(
                con,
                "select id_ulic, count(*) from bronze_a where id_ulic is not null and not regexp_full_match(id_ulic, "
                "'[0-9]{5}') group by 1 order by 2 desc limit 6",
            ),
            "simc_null_or_odd": one(
                con, "select count(*) from bronze_a where not regexp_full_match(coalesce(id_simc, ''), '[0-9]{7}')"
            ),
            "assigned_range": q(con, "select min(data_nad), max(data_nad), count(data_nad) from bronze_a"),
            "literal_null_text": dict(
                zip(
                    attrs,
                    con.execute(
                        "select "
                        + ", ".join(
                            f"count(*) filter (where lower(trim({c})) in ('null', 'none', 'brak', '-'))" for c in attrs
                        )
                        + " from bronze_a"
                    ).fetchone(),
                    strict=True,
                )
            ),
            "assigned_outside_1900_today": q(
                con,
                "select count(*), min(data_nad), max(data_nad) from bronze_a where data_nad < '1900' or "
                "data_nad > cast(current_date as varchar)",
            ),
            "house_number_shapes": q(
                con,
                "select regexp_replace(regexp_replace(numer_porz, '[0-9]', '9', 'g'), '[^9/ -]', 'a', 'g'), count(*) "
                "from bronze_a group by 1 order by 2 desc limit 8",
            ),
            "gmina_type_digit": q(
                con,
                "select right(teryt_gmi, 1), count(distinct teryt_gmi), count(*) from bronze_a group by 1 order by 1",
            ),
        },
    )

    silver_cols = ("entity", "address_id", "gmina_code", "house_number", "x_epsg2180", "y_epsg2180", "raw_sha256")
    note(
        "prg address silver",
        {
            "rows": one(con, "select count(*) from silver_a"),
            "duplicate_address_id": one(
                con, "select count(*) from (select address_id from silver_a group by 1 having count(*) > 1)"
            ),
            "repeated_local_id (keys, rows)": q(
                con,
                "select count(*), sum(n) from (select regexp_replace(address_id, '_[0-9]{4}-[0-9]{2}-[0-9]{2}T.*$', "
                "'') l, count(*) n from silver_a group by 1 having count(*) > 1)",
            ),
            "null_keys": dict(
                zip(
                    silver_cols,
                    con.execute(
                        "select "
                        + ", ".join(f"count(*) filter (where {c} is null)" for c in silver_cols)
                        + " from silver_a"
                    ).fetchone(),
                    strict=True,
                )
            ),
            "nulls_other": q(
                con,
                "select count(*) filter (where address_version_at is null), count(*) filter (where postal_code is null), count(*) filter (where ulic_code is null), "
                "count(*) filter (where simc_code is null), count(*) filter (where assigned_on is null) from silver_a",
            ),
            "xy": q(con, "select min(x_epsg2180), max(x_epsg2180), min(y_epsg2180), max(y_epsg2180) from silver_a"),
            "versions": q(con, "select min(address_version_at), max(address_version_at) from silver_a"),
            "complete_test_rows": q(
                con,
                render(
                    "warehouse/tests/gugik_prg_address_points_complete.sql",
                    {},
                    {"prg_address_points": "silver_a"},
                ),
            ),
            "types": q(con, "select column_name, column_type from (describe silver_a)"),
        },
    )

    con.execute(f"create table bronze_b as select * from read_parquet({state['boundaries']['paths']!r})")
    units_sql = render("warehouse/models/silver/gugik_prg/prg_units.sql", {}, {"prg_boundaries": "bronze_b"})
    con.execute(f"create table units as {units_sql}")
    geo_cols = ("geography_key", "level_name", "name_pl", "terc_code")
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan(selected_fields=geo_cols).to_arrow())
    con.execute("create table geo as select * from geo_a")
    macro = Path("warehouse/macros/sources/gugik_prg.sql").read_text()
    cte = Environment().from_string(macro + "{{ poc_geography__gugik_prg() }}").render(ref=lambda n: "units")
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from gugik_prg")
    counts_sql = render("warehouse/models/gold/poc_prg_address_counts.sql", {}, {"prg_address_points": "silver_a"})
    build_sql = render(
        "warehouse/models/gold/poc_build_prg.sql",
        {"poc_prg_address_counts": "counts_t", "poc_map_geography": "map_t", "prg_units": "units"},
        {},
    )
    rows = []
    for _ in (1, 2):
        started = time.monotonic()
        con.execute(f"create or replace table counts_t as {counts_sql}")
        con.execute(f"create or replace table gold_t as {build_sql}")
        rows.append(
            [
                one(con, "select count(*) from counts_t"),
                one(con, "select count(*) from gold_t"),
                round(time.monotonic() - started),
            ]
        )
    series = (
        "measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
        "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json"
    )
    note(
        "prg gold prg_address_counts and build_prg",
        {
            "rows_counts_build_seconds_two_builds": rows,
            "counts_by_level": q(
                con, "select unit_level, count(*), sum(address_points) from counts_t group by 1 order by 1"
            ),
            "dup_series_x_date": one(
                con, f"select count(*) from (select {series}, date_key from gold_t group by all having count(*) > 1)"
            ),
            "by_block": q(
                con,
                "select block_id, date_key, frequency, count(*), count(distinct measure_key), "
                "count(*) filter (where geography_key = '_X'), min(value), max(value) from gold_t group by all "
                "order by 1",
            ),
            "unmatched": q(
                con,
                "select geography_source_code, value from gold_t where block_id = 'punkty_adresowe' and "
                "geography_key = '_X' order by 2 desc limit 12",
            ),
            "levels": q(
                con,
                "select b.block_id, coalesce(g.level_name, b.geography_key), count(*) from gold_t as b left join geo "
                "as g using (geography_key) group by all order by 1, 2",
            ),
            "not_null_fails": q(
                con,
                "select count(*) filter (where measure_key is null or geography_key is null or "
                "date_key is null or value is null) from gold_t",
            ),
            "tests": {
                "counts_complete": q(
                    con,
                    render(
                        "warehouse/tests/gugik_prg_address_counts_complete.sql",
                        {"poc_prg_address_counts": "counts_t"},
                        {},
                    ),
                ),
                "value_range": q(
                    con,
                    render("warehouse/tests/gugik_prg_address_points_value_range.sql", {"poc_build_prg": "gold_t"}, {}),
                ),
                "unit_level_accepted": q(
                    con,
                    "select unit_level from counts_t where unit_level not in ('kraj', "
                    "'wojewodztwo', 'powiat', 'gmina')",
                ),
                "packages_accepted": q(con, "select packages, count(*) from counts_t where packages <> 1 group by 1"),
            },
        },
    )
    note(
        "prg gold spot values (address points)",
        {
            "poland_vs_silver": q(
                con,
                "select (select value from gold_t where block_id = 'punkty_adresowe' and geography_key = 'PL'), "
                "(select count(*) from silver_a)",
            ),
            "voivodeships": q(
                con,
                "select g.name_pl, b.value from gold_t as b join geo as g using (geography_key) where g.level_name = "
                "'wojewodztwo' and b.block_id = 'punkty_adresowe' order by 2 desc",
            ),
            "spot_gminas": q(
                con,
                "select geography_key, g.name_pl, value from gold_t as b left join geo as g using (geography_key) "
                "where block_id = 'punkty_adresowe' and geography_key in ('TERC:1465011', 'TERC:1261011', "
                "'TERC:0264011', 'TERC:1062011') order by 1",
            ),
            "largest_gminas": q(
                con,
                "select geography_key, g.name_pl, value from gold_t as b left join geo as g using (geography_key) "
                "where block_id = 'punkty_adresowe' and g.level_name = 'gmina' order by value desc limit 5",
            ),
            "smallest_gminas": q(
                con,
                "select geography_key, g.name_pl, value from gold_t as b left join geo as g using (geography_key) "
                "where block_id = 'punkty_adresowe' and g.level_name = 'gmina' order by value limit 3",
            ),
            "boundaries_block_rows": one(
                con, "select count(*) from gold_t where block_id = 'jednostki_administracyjne'"
            ),
            "rss_mb": rss_mb(),
        },
    )

    from warehouse import seed_rows

    source_row = [r for r in seed_rows.rows("source") if r["source_key"] == SOURCE]
    lic = {r["licence_key"] for r in source_row}
    note(
        "prg licence and code lists (catalogue rows)",
        {
            "source": source_row,
            "licence": [r for r in seed_rows.rows("licence") if r["licence_key"] in lic],
            "unit_map": [r for r in seed_rows.rows("unit_map") if r["source_system"] == SOURCE],
            "kpi_measure": [r for r in seed_rows.rows("kpi_measure") if r["source_system"] == SOURCE],
        },
    )


def cleanup(cat) -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    for namespace in (SCRATCH, SILVER):
        try:
            for table in cat.list_tables(namespace):
                try:
                    cat.purge_table(table)
                except Exception:
                    cat.drop_table(table)
                removed["tables"].append(".".join(table))
            cat.drop_namespace(namespace)
        except Exception as exc:
            removed[f"catalog_error_{namespace}"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    targets = [("STORAGE_LANDING_BUCKET_NAME", SCRATCH, "landing_objects")]
    targets += [("STORAGE_LAKEHOUSE_BUCKET_NAME", p, "lake_objects") for p in (SCRATCH, SILVER)]
    for var, prefix, label in targets:
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for var, prefix, _ in targets
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(var), Prefix=f"{prefix}/")
    )
    NOTES["left"] += 1
    note("scratch removed", removed)


def main() -> int:
    bulk_file.annotate = lambda *_a, **_k: None  # their summaries go to the log only (cap 10)
    prg_load.annotate = lambda *_a, **_k: None
    entities.annotate = lambda *_a, **_k: None
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            message = re.sub(r"\s+", " ", str(exc))[:1500]
            note("prg trial failed", {"error": f"{type(exc).__name__}: {message}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
