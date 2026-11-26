"""Dev trial of rspo (#3031) on the final code: Landing and Bronze into zz_dev_rspo/ (both buckets) and namespace
zz_dev_rspo, each twice; Bronze profiled before Silver; then the branch's Silver SQL and its tests (unique,
not_null, accepted_values from _rspo.yml, tests/rspo_institutions_complete.sql), the Gold build stage and the Gold
entity table with their tests, applied in memory (Jinja-rendered with warehouse/macros/sources/rspo.sql). Reads
silver.teryt_terc (read-only) for the geography map. Everything written is removed at the end (finally). Never
merged."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2
import requests
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.rspo import load as bronze
from ingestion.to_landing import rspo as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_rspo"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/rspo.sql").read_text()


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def landed_state(client, bucket) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def yml_tests(con, n, path: Path, table: str) -> dict[str, int]:
    """unique, not_null and accepted_values of a schema file, run on an in-memory table."""
    failures = {}
    for model in yaml.safe_load(path.read_text())["models"]:
        if not n(f"select count(*) from information_schema.tables where table_name = '{model['name']}'"):
            continue
        name = model["name"]
        for test in model.get("tests", []):
            if "unique" in test:
                expr = test["unique"]["column_name"]
                failures[f"{name} unique"] = n(f"select count(*) from (select 1 from {name} group by {expr} having count(*) > 1)")
        for column in model.get("columns", []):
            for test in column.get("tests", []):
                c = column["name"]
                if test == "not_null":
                    failures[f"{name} not_null {c}"] = n(f"select count(*) from {name} where {c} is null")
                elif test == "unique":
                    failures[f"{name} unique {c}"] = n(f"select count(*) from (select 1 from {name} group by {c} having count(*) > 1)")
                elif isinstance(test, dict) and "accepted_values" in test:
                    values = test["accepted_values"]["arguments"]["values"]
                    con.execute("create or replace temp table allowed as select unnest(?) as v", [values])
                    failures[f"{name} accepted_values {c}"] = n(
                        f"select count(*) from {name} where {c} is not null and cast({c} as varchar) not in (select v from allowed)"
                    )
    return failures


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    con.execute("SET memory_limit = '10GB'")
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    started = time.monotonic()
    try:  # one short request first: an unreachable portal ends the trial in a minute
        r = requests.get("https://api.dane.gov.pl/1.4/datasets/839", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 90))
        a = r.json()["data"]["attributes"]
        inst = r.json()["data"]["relationships"]["institution"]["data"]["id"]
        time.sleep(2)
        i = requests.get(f"https://api.dane.gov.pl/1.4/institutions/{inst}", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 90))
        reach = {"status": r.status_code, "title": a.get("title"), "licence": a.get("license_name"),
                 "default_cc40": a.get("license_condition_default_cc40"), "frequency": a.get("update_frequency"),
                 "institution": inst, "institution_title": i.json()["data"]["attributes"].get("title")}
    except Exception as exc:  # noqa: BLE001
        reach = {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    reach["seconds"] = round(time.monotonic() - started, 1)
    annotate("notice" if reach.get("status") == 200 else "error", "rspo trial reach dane.gov.pl", reach)
    if reach.get("status") != 200:
        return 1
    try:
        # Landing twice: the second run must write nothing
        states, codes = [], []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            codes.append(landing.extract(120, landing_root=LAND))
            states.append(landed_state(client, land_bucket))
        entities = sorted(landing.held(LAND))
        verify_rc = bulk_file.main("rspo", "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
        landing_stats = {
            "files": len([k for k, _ in states[0] if not k.endswith(".receipt.json")]),
            "bytes": sum(s for k, s in states[0] if not k.endswith(".receipt.json")),
            "entities": entities,
            "exit_codes": codes,
            "second_run_unchanged": states[0] == states[1],
            "verify_exit": verify_rc,
        }
        annotate("notice", "rspo trial landing", landing_stats)

        # Bronze twice: the second run must add nothing
        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            bronze.load(60, landing_root=LAND, namespace=SCRATCH)
            table = cat.load_table((SCRATCH, "rspo_institutions"))
            snap = table.current_snapshot()
            snapshots.append(snap.snapshot_id if snap else None)
        rows = cat.load_table((SCRATCH, "rspo_institutions")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "rspo_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001  the table exists only after a rejection
            rejected = []
        con.register("b", rows)
        counts = ["pupils", "pupils_girls", "pupils_preschool_classes", "classes", "teachers", "teachers_full_time",
                  "teachers_part_time", "teacher_fte", "teacher_fte_part_time"]
        bronze_stats = {
            "rows": rows.num_rows,
            "per_entity": n("select string_agg(entity || '=' || c || '/' || d, ' ' order by entity) from (select entity, count(*) c, count(distinct rspo_number) d from b group by 1)"),
            "rejected": [(x.get("entity"), (x.get("error") or "")[:100]) for x in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
            "null_keys": n("select count(*) from b where nullif(trim(rspo_number), '') is null or nullif(trim(school_year), '') is null or nullif(trim(gmina_teryt), '') is null"),
            "dup_rspo_year": n("select count(*) from (select 1 from b group by school_year, rspo_number having count(*) > 1)"),
            "years": n("select string_agg(distinct school_year, ',' order by school_year) from b"),
        }
        annotate("notice" if not rejected else "error", "rspo trial bronze", bronze_stats)
        # look before modelling: empty markers and non-numeric text in the count columns, per column
        profile = {}
        for c in counts:
            profile[c] = n(f"""select count(*) filter (where {c} is null) || ' null, ' || count(*) filter (where trim({c}) = '') || ' empty, '
                || count(*) filter (where nullif(trim({c}), '') is not null and try_cast({c} as double) is null) || ' text '
                || coalesce(string_agg(distinct {c}, '|') filter (where nullif(trim({c}), '') is not null and try_cast({c} as double) is null), '')
                || ', zero ' || count(*) filter (where try_cast({c} as double) = 0) from b""")
        profile["code_text"] = n("""select count(*) from b where not regexp_full_match(coalesce(nullif(trim(institution_type_id), ''), '0'), '[0-9]+')
            or not regexp_full_match(coalesce(nullif(trim(rspo_number), ''), '0'), '[0-9]+')""")
        profile["complex_with_pupils"] = n("select count(*) filter (where nullif(trim(pupils), '') is not null) || ' of ' || count(*) from b where unit_kind_id = '6'")
        annotate("notice", "rspo trial bronze profile", profile)

        # Silver and its tests; a second rendering gives the same rows (idempotent rebuild)
        silver_sql = render(ROOT / "warehouse/models/silver/rspo/rspo_institutions.sql", {"rspo_institutions": "b"})
        con.sql("create table rspo_institutions as " + silver_sql)
        rebuilt = n(f"select count(*) from ({silver_sql})")
        failures = yml_tests(con, n, ROOT / "warehouse/models/silver/rspo/_rspo.yml", "rspo_institutions")
        complete = render(ROOT / "warehouse/tests/rspo_institutions_complete.sql", {"rspo_institutions": "rspo_institutions"})
        failures["complete"] = n(f"select count(*) from ({complete})")
        silver_stats = {
            "rows": n("select count(*) from rspo_institutions"),
            "rebuild_rows": rebuilt,
            "per_year": n("select string_agg(reference_year || '=' || c, ' ' order by reference_year) from (select reference_year, count(*) c from rspo_institutions group by 1)"),
            "distinct_rspo": n("select count(distinct rspo_number) from rspo_institutions"),
            "tests_run": len(failures),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
            "pupils_null": n("select count(*) from rspo_institutions where pupils is null"),
            "pupils_zero": n("select count(*) from rspo_institutions where pupils = 0"),
            "pl_pupils_2025_non_complex": n("select sum(pupils) from rspo_institutions where reference_year = 2025 and unit_kind_id <> '6'"),
        }
        annotate("notice" if not any(failures.values()) else "error", "rspo trial silver", silver_stats)

        # Gold: geography map as poc_geography__rspo (TERYT codes of silver.teryt_terc), build stage, entity table
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("""create table geo as select 'rspo' as source_system, terc_code as source_code, 'TERC:' || terc_code as geography_key
            from terc union all select 'rspo', 'PL', 'PL'""")
        con.sql("create table poc_build_rspo as " + render(ROOT / "warehouse/models/gold/poc_build_rspo.sql",
                                                           {"rspo_institutions": "rspo_institutions", "poc_map_geography": "geo"}))
        con.sql("create table rspo_institution as " + render(ROOT / "warehouse/models/gold/rspo_institution.sql",
                                                             {"rspo_institutions": "rspo_institutions"}))
        gold_failures = yml_tests(con, n, ROOT / "warehouse/models/gold/_rspo.yml", "poc_build_rspo")
        g = "poc_build_rspo"
        # as poc_series_key() (macros/poc_blocks.sql): an unmatched area keeps its source code
        series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
                  "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
        gold_stats = {
            "rows": n(f"select count(*) from {g}"),
            "by_dimension": n(f"select string_agg(k || '=' || c, ' ') from (select coalesce(json_keys(breakdown_json::json)[1], '_T') k, count(*) c from {g} group by 1)"),
            "dup_series_date": n(f"select count(*) from (select 1 from {g} group by {series}, date_key having count(*) > 1)"),
            "unmatched_rows": n(f"select count(*) from {g} where geography_key = '_X'"),
            "unmatched_codes": n(f"select string_agg(distinct geography_source_code || '@' || period_label, ' ') from {g} where geography_key = '_X' and is_total"),
            "geographies": n(f"select count(distinct geography_key) from {g}"),
            "null_values": n(f"select count(*) from {g} where value is null"),
            "levels_add_up": n(f"""select count(*) from (select date_key, length(geography_source_code) l, sum(value) v from {g} where is_total and geography_source_code <> 'PL' group by 1, 2) x
                join (select date_key, value from {g} where is_total and geography_source_code = 'PL') p using (date_key) where x.v <> p.value"""),
            "entity_rows": n("select count(*) from rspo_institution"),
            "entity_in_newest": n("select count(*) from rspo_institution where in_newest_list"),
            "entity_geo_missing": n("select count(*) from rspo_institution where geography_key not in (select geography_key from geo)"),
            "test_failures": {k: v for k, v in gold_failures.items() if v} or "none",
            "tests_run": len(gold_failures),
        }
        annotate("notice" if not gold_failures or not any(gold_failures.values()) else "error", "rspo trial gold", gold_stats)
        spot = {
            "pl_total": n(f"select string_agg(period_label || '=' || cast(value as bigint), ' ' order by date_key) from {g} where is_total and geography_source_code = 'PL'"),
            "pl_2025_type": n(f"""select string_agg(json_extract_string(breakdown_label_json, '$.institution_type') || '=' || cast(value as bigint), '; ' order by value desc)
                from {g} where geography_source_code = 'PL' and period_label = '30.09.2025' and json_extract_string(breakdown_json, '$.institution_type') in ('1', '3', '14', '16', '93')"""),
            "pl_2025_public": n(f"""select string_agg(json_extract_string(breakdown_label_json, '$.public_status') || '=' || cast(value as bigint), '; ')
                from {g} where geography_source_code = 'PL' and period_label = '30.09.2025' and breakdown_json like '%public_status%'"""),
            "pl_2025_class": n(f"""select string_agg(json_extract_string(breakdown_json, '$.governing_body_class') || '=' || cast(value as bigint), '; ')
                from {g} where geography_source_code = 'PL' and period_label = '30.09.2025' and breakdown_json like '%governing_body_class%'"""),
            "areas_2025": n(f"select string_agg(geography_source_code || '=' || cast(value as bigint), ' ') from {g} where is_total and period_label = '30.09.2025' and geography_source_code in ('14', '1465', '1465011', '02', '1261011')"),
        }
        annotate("notice", "rspo trial spot values", spot)
        # licence gate: the source row's licence_key and its licence row, as the checkout's seed rows
        src = list(csv.DictReader((ROOT / "warehouse/seed_rows/source/rspo.csv").open()))
        lic = {r["licence_key"]: r for f in (ROOT / "warehouse/seed_rows/licence").glob("*.csv") for r in csv.DictReader(f.open())}
        annotate("notice", "rspo trial licence row", {"source": src, "licence": lic.get(src[0]["licence_key"])})
        print("STATS", landing_stats, bronze_stats, profile, silver_stats, gold_stats, spot, flush=True)
    finally:
        removed = 0
        for bucket in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
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
        except Exception as exc:  # noqa: BLE001  report, do not hide
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        annotate("notice", "rspo trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
