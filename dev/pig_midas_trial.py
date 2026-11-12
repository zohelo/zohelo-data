"""Dev trial (PL-ENV-015 PIG-PIB MIDAS, #2870): Landing → Bronze → Silver → Gold into scratch.

Landing load of the six WFS layers into the Landing prefix zz_dev_pig_midas/ (twice; every response embeds its
request time, so the second pass is expected to land again: reported, not hidden), inspect and verify; Bronze load
into Iceberg namespace zz_dev_pig_midas and lake prefix zz_dev_pig_midas/ (twice: the second appends nothing);
Bronze profile; then the Silver models and the Gold build stage rendered with Jinja from warehouse/models into local
DuckDB, each built twice, with every test of _pig_midas.yml run as SQL, build_check, the fact grain by series_key ×
date_key, the metrics rows and the value bound; gold.dim_geography is read (read-only) for the geography match.
The MIDAS terms pages are read once. sqlfluff (the repository's pinned version) lints warehouse/models.
Everything written is removed in `finally`. At most 10 annotations.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
import re
import time

import duckdb
from jinja2 import Environment
import requests
import yaml

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.pig_midas import load as bronze_load
from ingestion.to_landing import pig_midas as landing
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_pig_midas"
SOURCE = "pig_midas"
SILVER = "warehouse/models/silver/pig_midas"
LOG: dict = {}


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return (
        Environment()
        .from_string(Path(path).read_text())
        .render(config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t])
    )


def terms() -> None:
    out = {}
    for name, url in (
        ("robots", "https://www.pgi.gov.pl/robots.txt"),
        ("midas_sources", "https://www.pgi.gov.pl/midas/zrodla-danych.html"),
        ("isp_rules", "https://www.pgi.gov.pl/rejestry-i-bazy-danych-geologicznych/zasady-korzystania-z-isp.html"),
    ):
        time.sleep(3)
        try:
            r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
        except requests.RequestException as exc:
            out[name] = {"error": str(exc)[:200]}
            continue
        if name == "midas_sources":
            links = [u for u in bulk_file.links_matching(r.text, r.url, r"(?i)regulamin") if u != r.url][:3]
            out["regulamin_links"] = links
            for link in links[:1]:
                time.sleep(3)
                g = requests.get(link, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
                body = g.text if "html" in (g.headers.get("Content-Type") or "") else ""
                plain = re.sub(r"\s+", " ", re.sub(r"(?s)<script.*?</script>|<style.*?</style>|<[^>]+>", " ", body))
                out["regulamin"] = {
                    "status": g.status_code,
                    "ct": g.headers.get("Content-Type"),
                    "hits": [
                        plain[max(0, m.start() - 150) : m.start() + 250]
                        for m in re.finditer(r"(?i)komercyj|licencj|źródł|rozpowszechn|odpłatn", plain)
                    ][:4],
                }
        if name == "robots":
            out[name] = {"status": r.status_code, "disallow": re.findall(r"(?im)^disallow:.*$", r.text)[:12]}
            continue
        text = re.sub(r"\s+", " ", re.sub(r"(?s)<script.*?</script>|<style.*?</style>|<[^>]+>", " ", r.text))
        hits = [
            text[max(0, m.start() - 160) : m.start() + 240]
            for m in re.finditer(r"(?i)źródł|komercyj|licencj|warunk", text)
        ]
        out[name] = {"status": r.status_code, "hits": hits[:3]}
    LOG["terms"] = out
    annotate("notice", "midas terms pages", out)


def landing_steps() -> dict:
    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    runs, outcomes = [], []
    for _ in (1, 2):
        started = time.monotonic()
        runs.append(bulk_file.main(SOURCE, "load", landing_root=SCRATCH, pause_seconds=landing.PAUSE_SECONDS))
        outcomes.append(round(time.monotonic() - started))
    held = {}
    for spec in bulk_file.declaration(SOURCE):
        rec = bulk_file.landed(s3(), bucket, bulk_file.prefix(SOURCE, spec["entity"], SCRATCH))
        held[spec["entity"]] = {
            "copies": len(rec),
            "bytes": [r["size_bytes"] for r in rec],
            "same_sha_both_passes": len({r["sha256"] for r in rec}) == 1,
            "version": rec[-1]["version_token"] if rec else None,
        }
    inspect = bulk_file.main(SOURCE, "inspect", landing_root=SCRATCH)
    verify = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH)
    out = {
        "extract_exit_two_passes": runs,
        "seconds": outcomes,
        "inspect_exit": inspect,
        "verify_exit": verify,
        "files": sum(v["copies"] for v in held.values()),
        "bytes_newest": sum(v["bytes"][-1] for v in held.values() if v["bytes"]),
        "per_layer": held,
    }
    LOG["landing"] = out
    annotate("notice", "midas landing (scratch)", out)
    return out


def bronze_steps(con) -> None:
    exits = [bronze_load.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    cat = catalog()
    table = cat.load_table((SCRATCH, "pig_midas_features"))
    files_after = len(list(table.scan().plan_files()))
    arrow = table.scan().to_arrow()
    try:
        rejected = cat.load_table((SCRATCH, "pig_midas_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    con.register("bronze_a", arrow)
    con.execute("create table bronze as select * from bronze_a")
    snaps = [s.summary for s in table.snapshots()] if hasattr(table, "snapshots") else []
    out = {
        "bronze_exit_two_loads": exits,
        "files": files_after,
        "commits": len(snaps),
        "rows": arrow.num_rows,
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3],
        "per_entity": q(
            con,
            """select entity, count(*), count(distinct object_id), count(distinct contour_id),
            count(distinct deposit_number), count(distinct register_number), count(distinct raw_sha256),
            count(*) filter (where geometry_type is null), min(served_at) from bronze group by 1 order by 1""",
        ),
    }
    LOG["bronze"] = out
    annotate("notice", "midas bronze (scratch): entity, rows, objectids, contours, deposits, registers, files", out)
    cols = [c for c, _ in bronze_load.COLUMNS if c not in ("entity", "ingested_at", "source_row_number")]
    blanks = " union all ".join(
        f"select '{c}' as col, count(*) filter (where {c} is null) as nulls, "
        f"count(*) filter (where {c} is not null and trim({c}) = '') as blanks from bronze"
        for c in cols
    )
    profile = {
        "null_and_blank_per_column": [r for r in q(con, blanks) if r[1] != "0" or r[2] != "0"],
        "blank_strings": q(
            con,
            "select '|' || value || '|', count(*) from (select unnest(["
            + ", ".join(cols)
            + "]) as value from bronze) where trim(value) = '' group by 1",
        ),
        "deposit_number_not_integer": q(
            con,
            """select entity, count(*) from bronze where deposit_number is not null
            and not regexp_full_match(deposit_number, '[0-9]+(\\.0+)?') group by 1""",
        ),
        "date_shapes": q(
            con,
            """select count(*) filter (where established_on is not null and not regexp_full_match(established_on, '\\d\\d/\\d\\d/\\d{4}')),
            count(*) filter (where valid_until is not null and not regexp_full_match(valid_until, '\\d\\d/\\d\\d/\\d{4}')),
            count(*) filter (where abolished_on is not null and not regexp_full_match(abolished_on, '\\d\\d/\\d\\d/\\d{4}'))
            from bronze""",
        ),
        "coordinate_system_values": q(
            con,
            "select entity, list(distinct coordinate_system order by coordinate_system) from bronze where entity like '%mining%' group by 1",
        ),
        "status_values": q(
            con, "select entity, status, count(*) from bronze where status is not null group by all order by all"
        ),
        "served_at_minmax": q(con, "select min(served_at), max(served_at) from bronze"),
    }
    LOG["bronze_profile"] = profile
    annotate("notice", "midas bronze profile", profile)


def yml_tests(con) -> dict:
    spec = yaml.safe_load(Path(f"{SILVER}/_pig_midas.yml").read_text())
    results = {}
    for model in spec["models"]:
        name = model["name"]
        for t in model.get("tests") or []:
            expr = t["unique"]["column_name"]
            results[f"{name}:unique({expr[:40]})"] = q(
                con, f"select count(*) from (select {expr} as k from {name} group by 1 having count(*) > 1)"
            )[0][0]
        for col in model.get("columns") or []:
            for t in col.get("tests") or []:
                if t == "not_null":
                    results[f"{name}.{col['name']}:not_null"] = q(
                        con, f"select count(*) from {name} where {col['name']} is null"
                    )[0][0]
                elif isinstance(t, dict) and "accepted_values" in t:
                    values = t["accepted_values"]["arguments"]["values"]
                    listed = ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)
                    results[f"{name}.{col['name']}:accepted_values"] = q(
                        con,
                        f"select count(*) from {name} where {col['name']} is not null "
                        f"and cast({col['name']} as varchar) not in ({listed})",
                    )[0][0]
    return results


def silver_steps(con) -> None:
    sources = {"pig_midas_features": "bronze"}
    dep = render(f"{SILVER}/pig_midas_deposits.sql", {}, sources)
    con_sql = render(f"{SILVER}/pig_midas_mining_contours.sql", {}, sources)
    counts = {"pig_midas_deposits": [], "pig_midas_mining_contours": []}
    for _ in (1, 2):
        con.execute(f"create or replace table pig_midas_deposits as {dep}")
        con.execute(f"create or replace table pig_midas_mining_contours as {con_sql}")
        for t in counts:
            counts[t].append(con.execute(f"select count(*) from {t}").fetchone()[0])
    tests = yml_tests(con)
    out = {
        "rows_two_builds": counts,
        "tests_failing_rows": tests,
        "per_layer": q(
            con,
            """select layer, count(*), count(distinct deposit_number), min(snapshot_date) from pig_midas_deposits
            group by 1 union all select layer, count(*), count(distinct register_number), min(snapshot_date)
            from pig_midas_mining_contours group by 1 order by 1""",
        ),
    }
    LOG["silver"] = out
    annotate("notice", "midas silver: rows, every _pig_midas.yml test (failing rows)", out)
    profile = {
        "mineral_codes_documented": q(
            con,
            "select list(mineral_code order by mineral_code) from (select distinct mineral_code from pig_midas_deposits where layer = 'documented')",
        ),
        "mineral_codes_removed_not_in_documented": q(
            con,
            """select list(distinct mineral_code) from pig_midas_deposits where layer = 'removed' and mineral_code not in
            (select mineral_code from pig_midas_deposits where layer = 'documented')""",
        ),
        "code_name_pairs_not_1to1": q(
            con,
            "select count(*) from (select mineral_code from pig_midas_deposits group by 1 having count(distinct mineral_name) > 1)",
        ),
        "deposits_with_several_codes": q(
            con,
            "select count(*) from (select deposit_number from pig_midas_deposits where layer = 'documented' group by 1 having count(distinct mineral_code) > 1)",
        ),
        "deposits_polygons_max": q(
            con,
            "select max(n) from (select count(*) n from pig_midas_deposits where layer = 'documented' group by deposit_number)",
        ),
        "registers_with_several_contours": q(
            con,
            "select layer, count(*) from (select layer, register_number from pig_midas_mining_contours group by all having count(*) > 1) group by 1 order by 1",
        ),
        "contour_deposit_not_whole": q(
            con,
            "select count(*) from pig_midas_mining_contours where deposit_number is null and deposit_number_text is not null",
        ),
        "current_contours_deposit_not_in_documented": q(
            con,
            """select layer, count(*) from pig_midas_mining_contours where layer in ('mining_areas', 'mining_terrains')
            and deposit_number not in (select deposit_number from pig_midas_deposits where layer = 'documented') group by 1""",
        ),
        "dates": q(
            con,
            """select layer, min(established_on), max(established_on), min(valid_until), max(valid_until),
            min(abolished_on), max(abolished_on), count(*) filter (where valid_until < snapshot_date)
            from pig_midas_mining_contours group by 1 order by 1""",
        ),
        "valid_indefinitely": q(
            con,
            "select layer, count(*) filter (where valid_indefinitely), count(*) filter (where valid_until is null) from pig_midas_mining_contours group by 1 order by 1",
        ),
        "epsg_values": q(
            con,
            "select list(distinct coordinate_system_epsg order by coordinate_system_epsg) from pig_midas_mining_contours",
        ),
        "area_ranges": q(
            con,
            """select layer, min(shape_area_m2), max(shape_area_m2), min(declared_area_m2), max(declared_area_m2)
            from (select layer, shape_area_m2, null::double declared_area_m2 from pig_midas_deposits union all
            select layer, shape_area_m2, declared_area_m2 from pig_midas_mining_contours) group by 1 order by 1""",
        ),
    }
    LOG["silver_profile"] = profile
    annotate("notice", "midas silver profile", profile)


def gold_steps(con, geo_keys) -> None:
    con.register("geo_a", geo_keys)
    con.execute("create table dim_geography as select geography_key from geo_a")
    refs = {"pig_midas_deposits": "pig_midas_deposits", "pig_midas_mining_contours": "pig_midas_mining_contours"}
    build = render("warehouse/models/gold/poc_build_pig_midas.sql", refs, {})
    rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build}")
        rows.append(con.execute("select count(*) from build").fetchone()[0])
    blocks = re.findall(r"\('pig_midas', '([a-z_]+)', '([^']+)'\)", Path("warehouse/macros/poc_blocks.sql").read_text())
    con.execute("create or replace table activated (block_id varchar, block_name varchar)")
    con.executemany("insert into activated values (?, ?)", blocks)
    with open("warehouse/seeds/kpi_measure.csv", encoding="utf-8") as handle:
        patterns = [r["measure_key_pattern"] for r in csv.DictReader(handle) if r["source_system"] == SOURCE]
    likes = " or ".join(f"measure_key like '{p}'" for p in patterns) or "false"
    bound = re.search(
        r"\('mineral_deposits', 'pig_midas', '([^']*)', ([0-9.e+-]+), ([0-9.e+-]+)\)",
        Path("warehouse/tests/metrics_value_range.sql").read_text(),
    )
    out = {
        "rows_two_builds": rows,
        "build_check": q(
            con,
            """select a.block_id, count(b.block_id), count(distinct b.measure_key || '|' || b.geography_key || '|' || b.breakdown_json),
            count(*) filter (where b.geography_key = '_X'), count(*) filter (where b.geography_key not in (select geography_key from dim_geography)),
            min(b.date_key), max(b.date_key), list(distinct b.unit_code)
            from activated as a left join build as b using (block_id) group by 1 order by 1""",
        ),
        "dup_series_key_x_date_key": q(
            con,
            """select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json as series_key,
            date_key from build group by all having count(*) > 1)""",
        ),
        "null_or_nonpositive_values": q(con, "select count(*) from build where value is null or value <= 0"),
        "spot_PL_totals": q(con, "select measure_key, date_key, value from build where is_total order by 1"),
        "top_minerals": q(
            con,
            """select json_extract_string(breakdown_json, '$.mineral_code'), json_extract_string(breakdown_label_json, '$.mineral_code'), value
            from build where not is_total order by value desc limit 6""",
        ),
        "codes_sum_vs_total": q(
            con,
            "select sum(value) filter (where not is_total), sum(value) filter (where is_total) from build where block_id = 'documented_deposits'",
        ),
        "metrics_rows_and_bound": q(
            con,
            f"""select count(*), min(value), max(value),
            {f"count(*) filter (where value < {bound.group(2)} or value > {bound.group(3)})" if bound else "'no bound'"}
            from build where is_total and geography_key <> '_X' and ({likes})""",
        ),
        "kpi_patterns": patterns,
    }
    LOG["gold"] = out
    annotate("notice", "midas gold build_pig_midas (block, rows, series, _X, not in dim_geography, dates, units)", out)


def lint() -> None:
    import subprocess
    import sys

    from ops import repo_check

    subprocess.run([sys.executable, "-m", "pip", "install", "-q", repo_check.SQLFLUFF], capture_output=True, text=True)
    files = [
        f"{SILVER}/pig_midas_deposits.sql",
        f"{SILVER}/pig_midas_mining_contours.sql",
        "warehouse/models/gold/poc_build_pig_midas.sql",
        "warehouse/models/gold/poc_metrics_mineral_deposits.sql",
    ]
    problems = repo_check.tool([sys.executable, "-m", "sqlfluff", "lint", *files, "--disable-progress-bar"])
    LOG["sqlfluff"] = problems
    annotate(
        "notice" if not problems else "warning",
        "midas sqlfluff (pinned, own models)",
        {"version": repo_check.SQLFLUFF, "files": len(files), "problems": [p[-3000:] for p in problems]},
    )


def cleanup() -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for var, label in (
        ("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
        ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects"),
    ):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
    annotate("notice", "midas scratch removed", removed)


def main() -> int:
    from ingestion.common import bulk_bronze

    bulk_file.annotate = bulk_bronze.annotate = lambda *_a, **_k: None  # their summaries go to the log only (cap 10)
    failed = 0
    try:
        for step in (terms, lint):
            try:
                step()
            except Exception as exc:  # noqa: BLE001
                print(json.dumps({f"{step.__name__}_error": str(exc)[:300]}))
        try:
            landing_steps()
            con = duckdb.connect()
            bronze_steps(con)
            silver_steps(con)
            geo = catalog().load_table(("gold", "dim_geography")).scan(selected_fields=("geography_key",)).to_arrow()
            gold_steps(con, geo)
        except Exception as exc:  # noqa: BLE001
            failed = 1
            annotate("error", "midas trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
        print(json.dumps(LOG, default=str, ensure_ascii=False))
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
