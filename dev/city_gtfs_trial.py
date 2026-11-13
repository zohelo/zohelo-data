"""Dev trial of the city GTFS family (gtfs_gdansk, gtfs_poznan, gtfs_krakow, gtfs_wroclaw; args: source systems).

Landing (extract twice, inspect, verify) and Bronze (load twice) into the scratch prefix and namespace zz_dev_gtfs,
Bronze profiled and counted against the raw ZIPs, then the branch's Silver and Gold SQL (Jinja-rendered with the
source macros) and every test of the sources' YAML files applied in memory (DuckDB). Reads silver.teryt_terc
(read-only) for the geography check. Everything written is removed at the end (finally). Deleted before merge.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
import re
import sys
import tempfile
import zipfile

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze, gtfs
from ingestion.common import report as report_module
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_gtfs"
LAND = f"{SCRATCH}/01_landing"
SOURCES = ["gtfs_gdansk", "gtfs_poznan", "gtfs_krakow", "gtfs_wroclaw"]
TERC = {"gtfs_gdansk": "2261011", "gtfs_poznan": "3064011", "gtfs_krakow": "1261011", "gtfs_wroclaw": "0264011"}

annotate = report_module.annotate


def _quiet(*args, **kwargs) -> None:
    """Library annotations are printed only (the step keeps to a few compact annotations of its own)."""
    print("ANNOTATION", json.dumps([str(a)[:300] for a in args]), flush=True)


report_module.annotate = _quiet
bulk_bronze.annotate = _quiet
bulk_file.annotate = _quiet

MACROS = "".join(p.read_text() for p in sorted((ROOT / "warehouse/macros/sources").glob("gtfs_*.sql")))
TESTS = (ROOT / "warehouse/tests/generic/gtfs.sql").read_text()
TESTS = re.sub(r"\{%\s*test\s+(\w+)\(", r"{% macro test_\1(", TESTS).replace("{% endtest %}", "{% endmacro %}")
ENV = jinja2.Environment()


def render(text: str, tables: dict[str, str]) -> str:
    text = re.sub(r"\{\{ config\(.*?\) \}\}", "", text, flags=re.S)
    return ENV.from_string(MACROS + TESTS + text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def one(con, sql):
    return con.sql(sql).fetchall()[0][0]


def show(con, title, sql, limit=60):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:100] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def run_tests(con, yml: Path, tables: dict[str, str]) -> dict[str, int]:
    """Every test of a schema YAML on the in-memory tables; returns {test: failing rows}."""
    out: dict[str, int] = {}
    for model in yaml.safe_load(yml.read_text())["models"]:
        table = tables[model["name"]]
        specs = [(None, t) for t in model.get("tests", [])]
        specs += [(c["name"], t) for c in model.get("columns", []) for t in c.get("tests", [])]
        for column, test in specs:
            name, args = (test, {}) if isinstance(test, str) else next(iter(test.items()))
            args = dict(args or {})
            args = {**args.pop("arguments", {}), **{k: v for k, v in args.items() if k != "config"}}
            if column:
                args.setdefault("column_name", column)
            if name == "not_null":
                sql = f"select * from {table} where {args['column_name']} is null"
            elif name == "unique":
                c = args["column_name"]
                sql = f"select {c} from {table} group by all having count(*) > 1"
            elif name == "accepted_values":
                quote = args.get("quote", True)
                vals = ", ".join(f"'{v}'" if quote else str(v) for v in args["values"])
                sql = f"select distinct {args['column_name']} from {table} where {args['column_name']} not in ({vals})"
            else:
                for k, v in list(args.items()):
                    if isinstance(v, str) and v.startswith(("source(", "ref(")):
                        args[k] = ENV.from_string("{{ " + v + " }}").render(
                            source=lambda s, t: tables[t], ref=lambda m: tables[m]
                        )
                macro = getattr(ENV.from_string(MACROS + TESTS).module, f"test_{name}")
                sql = str(macro(model=table, **args))
            failing = one(con, f"select count(*) from ({sql})")
            label = f"{model['name']}.{column + '.' if column else ''}{name}"
            out[label] = failing
            if failing:
                show(con, f"FAILING {label}", sql, limit=15)
    return out


def raw_counts(body: bytes) -> dict[str, int]:
    """Data rows per kept GTFS file of one ZIP, counted by DuckDB from the exact bytes."""
    counts = {}
    con = duckdb.connect()
    with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(io.BytesIO(body)) as archive:
        for name in [*gtfs.FILES, "stop_times.txt"]:
            if name in archive.namelist():
                path = Path(tmp) / name
                path.write_bytes(archive.read(name))
                counts[name] = one(
                    con, f"select count(*) from read_csv('{path}', header = true, all_varchar = true, delim = ',')"
                )
        counts["_members"] = sorted(archive.namelist())
    con.close()
    return counts


def trial(source: str, client, cat, con) -> dict:
    stats: dict = {"source": source}
    lake = req("STORAGE_LANDING_BUCKET_NAME")
    if any(s.get("url_from") for s in bulk_file.declaration(source)):
        for spec in bulk_file.declaration(source):
            gtfs._pace()
            print("LISTING", source, bulk_file.page_links(spec)[:12], flush=True)
    for attempt in (1, 2):
        print(f"\n##### {source} landing extract {attempt}", flush=True)
        stats[f"extract_{attempt}"] = gtfs.extract(source, landing_root=LAND)
    print(f"\n##### {source} landing inspect / verify", flush=True)
    stats["inspect_exit"] = gtfs.inspect(source, landing_root=LAND)
    entities = gtfs.entities(source, LAND)
    stats["verify_exit"] = bulk_file.main(source, "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
    receipts = [r for spec in bulk_file.declaration(source) for r in gtfs.held(client, lake, source, spec["entity"], LAND)]
    stats["landing"] = [
        {
            "entity": r["entity"],
            "bytes": r["size_bytes"],
            "version": r["version_token"],
            "disposition": (r["detail"].get("http") or {}).get("content-disposition"),
            "final_url": r["final_url"],
        }
        for r in receipts
    ]
    for attempt in (1, 2):
        print(f"\n##### {source} bronze load {attempt}", flush=True)
        stats[f"bronze_{attempt}"] = gtfs.load(source, landing_root=LAND, namespace=SCRATCH)
    records = cat.load_table((SCRATCH, f"{source}_records")).scan().to_arrow()
    stats["bronze_files"] = len(bf.loaded_hashes(cat.load_table((SCRATCH, f"{source}_records"))))
    try:
        rejected = cat.load_table((SCRATCH, f"{source}_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001 - no rejections table when nothing was rejected
        rejected = []
    stats["rejected"] = [(r["entity"], r["error"][:200]) for r in rejected]
    rec = f"{source}_records"
    con.register(rec, records)

    # Bronze against the raw files
    raw = {}
    for r in receipts:
        raw[r["sha256"]] = raw_counts(bulk_bronze.read_raw(client, lake, source, r))
    bronze_counts = {
        (sha, f): n
        for sha, f, n in con.sql(
            f"select raw_sha256, gtfs_file, sum(case when gtfs_file = 'stop_times.txt' then stop_times_rows else 1 end) "
            f"from {rec} group by raw_sha256, gtfs_file"
        ).fetchall()
    }
    mismatch = [
        (sha[:10], f, n, bronze_counts.get((sha, f)))
        for sha, files in raw.items()
        for f, n in files.items()
        if f != "_members" and bronze_counts.get((sha, f)) != n
    ]
    stats["bronze_vs_raw_mismatch"] = mismatch
    stats["members"] = {sha[:10]: files["_members"] for sha, files in raw.items()}
    stats["bronze_rows"] = records.num_rows
    show(con, f"{source} bronze rows per file", f"select entity, gtfs_file, count(*) rows, sum(stop_times_rows) stop_times from {rec} group by all order by all")
    show(con, f"{source} bronze profile", f"""
        select entity,
          count(distinct trip_id) filter (where gtfs_file = 'trips.txt') trips,
          count(*) filter (where gtfs_file = 'trips.txt') trip_rows,
          count(distinct stop_id) filter (where gtfs_file = 'stops.txt') stops,
          count(*) filter (where gtfs_file = 'stops.txt') stop_rows,
          count(distinct route_id) filter (where gtfs_file = 'routes.txt') routes,
          count(*) filter (where gtfs_file = 'routes.txt') route_rows,
          count(*) filter (where gtfs_file = 'trips.txt' and (trip_id is null or route_id is null or service_id is null)) trip_null_keys,
          count(*) filter (where gtfs_file = 'stops.txt' and stop_id is null) stop_null_keys,
          string_agg(distinct route_type, ',') filter (where gtfs_file = 'routes.txt') route_types,
          string_agg(distinct coalesce(location_type, '<empty>'), ',') filter (where gtfs_file = 'stops.txt') location_types,
          count(*) filter (where gtfs_file = 'stops.txt' and (try_cast(stop_lat as double) is null or try_cast(stop_lon as double) is null)) stops_bad_coords,
          count(*) filter (where gtfs_file = 'calendar.txt') calendar_rows,
          min(start_date) filter (where gtfs_file = 'calendar.txt') cal_first, max(end_date) filter (where gtfs_file = 'calendar.txt') cal_last,
          count(*) filter (where gtfs_file = 'calendar_dates.txt') calendar_dates_rows,
          string_agg(distinct exception_type, ',') filter (where gtfs_file = 'calendar_dates.txt') exception_types,
          min("date") filter (where gtfs_file = 'calendar_dates.txt') cd_first, max("date") filter (where gtfs_file = 'calendar_dates.txt') cd_last,
          count(*) filter (where gtfs_file = 'trips.txt' and trip_id not in (select trip_id from {rec} r2 where r2.gtfs_file = 'stop_times.txt' and r2.entity = {rec}.entity)) trips_without_stop_times,
          string_agg(distinct agency_name, ' | ') filter (where gtfs_file = 'agency.txt') agencies,
          string_agg(distinct feed_publisher_name || ' ' || coalesce(feed_start_date, '') || '-' || coalesce(feed_end_date, ''), ' | ') filter (where gtfs_file = 'feed_info.txt') feed_info
        from {rec} group by entity order by entity""")
    show(con, f"{source} bronze duplicate keys", f"""
        select gtfs_file, count(*) dup_keys from (
          select entity, gtfs_file, coalesce(trip_id, stop_id, route_id) k from {rec}
          where gtfs_file in ('trips.txt', 'stops.txt', 'routes.txt', 'stop_times.txt') group by all having count(*) > 1)
        group by all""")

    # Silver
    silver_dir = ROOT / f"warehouse/models/silver/{source}"
    tables = {rec: rec}
    for model in ("stops", "routes", "service_days"):
        name = f"{source}_{model}"
        con.sql(f"create or replace table {name} as " + render((silver_dir / f"{name}.sql").read_text(), tables))
        tables[name] = name
    sd = f"{source}_service_days"
    stats["silver_rows"] = {m: one(con, f"select count(*) from {source}_{m}") for m in ("stops", "routes", "service_days")}
    stats["service_days"] = one(con, f"select min(service_date) || ' .. ' || max(service_date) || ' (' || count(distinct service_date) || ' days)' from {sd}")
    show(con, f"{source} silver service days", f"select feed, service_date, route_type, trips, departures, routes, trips_without_stop_times, snapshots_covering, snapshot_date from {sd} order by all", limit=200)
    stats["silver_tests_failing"] = {k: v for k, v in run_tests(con, silver_dir / f"_{source}.yml", tables).items() if v}

    # Gold
    con.sql(f"""create or replace table map_geography_{source} as
        with geo as (select 'TERC:' || terc_code as geography_key from terc where unit_level = 'gmina'),
        {render('{{ poc_geography__' + source + '() }}', tables).strip()}
        select * from {source}""")
    tables["poc_map_geography"] = f"map_geography_{source}"
    gold_dir = ROOT / "warehouse/models/gold"
    for model, alias in (
        (f"poc_build_{source}", f"build_{source}"),
        (f"transit_stops_{source}", f"transit_stops_{source}"),
        (f"transit_routes_{source}", f"transit_routes_{source}"),
    ):
        con.sql(f"create or replace table {alias} as " + render((gold_dir / f"{model}.sql").read_text(), tables))
        tables[model] = alias
    g = f"build_{source}"
    stats["gold_rows"] = one(con, f"select count(*) from {g}")
    stats["gold_dup_series_date"] = one(con, f"select count(*) from (select 1 from {g} group by measure_key || '|' || geography_key || '|' || breakdown_json, date_key having count(*) > 1)")
    stats["gold_unmatched"] = one(con, f"select count(*) from {g} where geography_key = '_X'")
    stats["gold_status"] = one(con, f"select string_agg(observation_status_key || '=' || n, ' ') from (select observation_status_key, count(*) n from {g} group by 1 order by 1)")
    stats["gold_days"] = one(con, f"select min(date_key) || ' .. ' || max(date_key) from {g}")
    stats["entities"] = {t: one(con, f"select count(*) from {t}_{source}") for t in ("transit_stops", "transit_routes")}
    stats["gold_tests_failing"] = {k: v for k, v in run_tests(con, gold_dir / f"_{source}.yml", tables).items() if v}
    stats["spot"] = con.sql(f"""select strftime(date_key, '%a %Y-%m-%d') || ' ' || coalesce(json_extract_string(breakdown_json, '$.route_type'), 'total') || '=' || cast(value as bigint) || ' ' || observation_status_key
        from {g} where date_key in (select min(date_key) from {g} where dayofweek(date_key) = 3 union select min(date_key) from {g} where dayofweek(date_key) = 0)
        order by date_key, is_total desc, breakdown_json""").fetchall()
    stats["spot"] = [s[0] for s in stats["spot"]]
    show(con, f"{source} gold sample", f"select date_key, breakdown_json, breakdown_label_json, value, observation_status_key, geography_key from {g} order by date_key, breakdown_json", limit=40)
    return stats


def main(argv) -> int:
    sources = argv or SOURCES
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    failed = []
    try:
        con.register("terc", cat.load_table(("silver", "teryt_terc")).scan().to_arrow())
        for source in sources:
            try:
                stats = trial(source, client, cat, con)
            except Exception as exc:  # noqa: BLE001 - one city's failure is reported, the others still run
                import traceback

                traceback.print_exc()
                failed.append(source)
                annotate("error", f"{source} trial failed", {"error": f"{type(exc).__name__}: {exc}"[:1500]})
                continue
            print("STATS", json.dumps(stats, default=str, indent=1), flush=True)
            compact = {
                k: stats[k]
                for k in (
                    "extract_1", "extract_2", "inspect_exit", "verify_exit", "bronze_1", "bronze_2", "bronze_files",
                    "bronze_rows", "rejected", "bronze_vs_raw_mismatch", "silver_rows", "service_days",
                    "silver_tests_failing", "gold_rows", "gold_days", "gold_status", "gold_dup_series_date",
                    "gold_unmatched", "entities", "gold_tests_failing", "spot",
                )
            }
            compact["landing"] = [(x["entity"], x["bytes"], x["version"]) for x in stats["landing"]]
            ok = not (compact["silver_tests_failing"] or compact["gold_tests_failing"] or compact["rejected"]
                      or compact["bronze_vs_raw_mismatch"] or compact["gold_dup_series_date"])
            annotate("notice" if ok else "warning", f"{source} trial", compact)
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:  # noqa: BLE001
            print("drop failed", exc)
        left = sum(
            len(page.get("Contents", []))
            for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/")
        )
        annotate("notice", "city gtfs trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
