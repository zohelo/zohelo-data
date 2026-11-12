"""Dev trial of gios_groundwater (register PL-ENV-012, #2870): Landing → Bronze → Silver → Gold into scratch.

Landing extract twice into zz_dev_gios_groundwater/01_landing (Landing bucket), Bronze load twice into namespace
zz_dev_gios_groundwater (lake prefix zz_dev_gios_groundwater/), a Bronze profile, the Silver models and their schema
tests and the Gold build stage rendered with Jinja from warehouse/ and run in local DuckDB over the scratch Bronze
table (each built twice), sqlfluff on the new SQL. Everything written is removed in `finally`. Deleted before merge.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import duckdb
from jinja2 import Environment
import requests
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common import report
from ingestion.common.report import annotate
from ingestion.to_bronze.gios_groundwater import load as bronze_load
from ingestion.to_landing import gios_groundwater as landing
from ingestion.to_landing.patterns import bulk_file

CAPTURED: list = []


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:6000]}", flush=True)


bulk_bronze.annotate = _capture
report.annotate = _capture  # landing.extract imports it at call time; the trial's own notes use the original

SCRATCH = "zz_dev_gios_groundwater"
LANDING_ROOT = f"{SCRATCH}/01_landing"
SILVER = [
    "gios_groundwater_quality_classes",
    "gios_groundwater_samples",
    "gios_groundwater_results_inorganic_1991_2007",
    "gios_groundwater_results_inorganic",
    "gios_groundwater_results_organic",
    "gios_groundwater_indicator_classes",
]
SQL_FILES = [f"warehouse/models/silver/gios_groundwater/{m}.sql" for m in SILVER] + [
    "warehouse/models/gold/poc_build_gios_groundwater.sql",
    "warehouse/models/gold/poc_metrics_groundwater_good_quality_share.sql",
    "warehouse/tests/metrics_value_range.sql",
]
STATS: dict = {"head": os.environ.get("GITHUB_SHA", "")[:7]}


def note(title: str, data, level: str = "notice") -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate(level, title, json.loads(text) if len(text) <= 3800 else {"truncated": text[:3700]})


def one(con, q):
    return con.execute(q).fetchone()[0]


def rows(con, q, limit=400):
    return [[None if v is None else str(v) for v in r] for r in con.execute(q).fetchall()][:limit]


def show(con, title, q, limit=400, width=120):
    print(f"\n=== {title}", flush=True)
    rel = con.execute(q)
    print("\t".join(d[0] for d in rel.description))
    out = rel.fetchall()
    for r in out[:limit]:
        print("\t".join("" if v is None else str(v)[:width] for v in r))
    if len(out) > limit:
        print(f"... {len(out)} rows")


def render(path: str) -> str:
    macros = Path("warehouse/macros/poc_blocks.sql").read_text() + Path("warehouse/macros/gios_groundwater.sql").read_text()
    env = Environment(extensions=["jinja2.ext.do"])
    return env.from_string(macros + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda name: name, source=lambda _s, _t: "bronze_cells", var=lambda *_a: []
    )


def schema_tests(con, model: str) -> dict:
    spec = yaml.safe_load(Path("warehouse/models/silver/gios_groundwater/_gios_groundwater.yml").read_text())
    m = next(x for x in spec["models"] if x["name"] == model)
    results = {}
    for t in m.get("tests", []):
        if "unique" in t:
            expr = t["unique"]["column_name"]
            results["unique"] = one(con, f"select count(*) from (select {expr} k from {model} group by 1 having count(*) > 1)")
    for col in m.get("columns", []):
        for t in col.get("tests", []):
            if t == "not_null":
                results[f"not_null {col['name']}"] = one(con, f"select count(*) from {model} where {col['name']} is null")
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lst = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                bad = rows(con, f"select cast({col['name']} as varchar), count(*) from {model} where {col['name']} is not null "
                                f"and cast({col['name']} as varchar) not in ({lst}) group by 1 order by 2 desc limit 10")
                results[f"accepted_values {col['name']}"] = sum(int(c) for _, c in bad)
                if bad:
                    results[f"accepted_values {col['name']} sample"] = bad
    return results


def landing_runs() -> dict:
    out: dict = {}
    pages = landing.pages()
    out["pages"] = list(pages)
    for label in ("2023", "2021"):
        if label in pages:
            r = requests.get(pages[label], headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
            print(f"### page {label} {pages[label]} status {r.status_code} len {len(r.content)} "
                  f"headers {dict(r.headers)}\n{r.text[:2500]}", flush=True)
            out[f"page_{label}"] = {"url": pages[label][-40:], "status": r.status_code, "bytes": len(r.content),
                                    "content_type": r.headers.get("Content-Type")}
    for attempt in (1, 2):
        if attempt == 2:
            time.sleep(180)  # the second pass a few minutes later, as a scheduled run would come
        CAPTURED.clear()
        code = landing.extract(120, landing_root=LANDING_ROOT)
        out[f"extract_{attempt}"] = {"exit": code, **{k: v for t, d in CAPTURED if "landing extract" in t for k, v in d.items()
                                                       if k in ("listed", "planned", "landed", "unchanged", "bytes_landed", "refused", "error", "pages")}}
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landing.held_receipts(LANDING_ROOT)
    out["files"] = len(held)
    out["bytes"] = sum(r["size_bytes"] for r in held.values())
    out["copies_per_entity_max"] = max(
        len(bulk_file.landed(client, bucket, bulk_file.prefix("gios_groundwater", e, LANDING_ROOT))) for e in held
    ) if held else 0
    kinds: dict = {}
    for e in held:
        k, y = e.rsplit("_", 1)
        kinds.setdefault(k, []).append(int(y))
    out["kinds"] = {k: f"{min(v)}-{max(v)} ({len(v)})" for k, v in kinds.items()}
    out["link_text_in_receipt"] = sum(1 for r in held.values() if (r.get("detail") or {}).get("link_text"))
    out["no_last_modified"] = sorted(e for e, r in held.items() if r.get("version_basis") != "Last-Modified")
    for e, r in sorted(held.items()):
        print("RECEIPT", e, r["size_bytes"], r["version_token"], r["source_url"], (r.get("detail") or {}).get("link_text"))
    return out


def bronze_runs(con) -> dict:
    out: dict = {"loads": []}
    for _ in (1, 2):
        CAPTURED.clear()
        bronze_load.load(0, landing_root=LANDING_ROOT, namespace=SCRATCH)
        out["loads"] += [{k: d.get(k) for k in ("candidates", "appended_files", "rows_added", "rejected")} for _t, d in CAPTURED]
    cat = catalog()
    table = cat.load_table((SCRATCH, "gios_groundwater_cells"))
    arrow = table.scan().to_arrow()
    out["files"] = len(list(table.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rejected = cat.load_table((SCRATCH, "gios_groundwater_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rejected]
    con.register("bronze_arrow", arrow)
    con.execute("create table bronze_cells as select * from bronze_arrow")
    return out


def bronze_profile(con) -> dict:
    p: dict = {}
    p["distinct_entities"] = one(con, "select count(distinct entity) from bronze_cells")
    p["years"] = one(con, "select min(regexp_extract(entity, '([0-9]{4})$', 1)) || '-' || max(regexp_extract(entity, '([0-9]{4})$', 1)) from bronze_cells")
    p["null_value_text"] = one(con, "select count(*) from bronze_cells where value_text is null or value_text = ''")
    p["null_keys"] = one(con, "select count(*) from bronze_cells where entity is null or raw_sha256 is null or column_role is null")
    p["roles"] = rows(con, "select column_role, count(*) from bronze_cells group by 1 order by 2 desc")
    show(con, "per entity", "select entity, any_value(sheet_name), count(*) cells, max(row_number) max_row, max(column_number) max_col, "
              "count(distinct column_number) filter (where column_role = 'parameter') params, count(distinct raw_sha256) files, "
              "any_value(link_text) from bronze_cells group by 1 order by 1")
    show(con, "headers per entity (role: text)", "select entity, string_agg(column_number || '=' || coalesce(c.role, '?') || ':' || "
              "regexp_replace(value_text, '\\s+', ' ', 'g'), ' | ' order by column_number) from bronze_cells b "
              "left join (select distinct entity e, column_number cn, column_role role from bronze_cells where row_number > 1) c "
              "on c.e = b.entity and c.cn = b.column_number where row_number = 1 group by 1 order by 1", width=100000)
    shape = ("regexp_replace(regexp_replace(trim(value_text), '[0-9]', '9', 'g'), '9+', '9', 'g')")
    p["param_value_shapes"] = rows(con, f"select cell_type, {shape} s, count(*), min(value_text), max(value_text) from bronze_cells "
                                        f"where column_role = 'parameter' and row_number > 1 and cell_type <> 'n' group by all order by 3 desc limit 25")
    p["param_text_strings"] = rows(con, "select b.entity, b.column_number, left(h.value_text, 60), count(*), min(b.value_text), max(b.value_text) "
                                        "from bronze_cells b join bronze_cells h on h.entity = b.entity and h.column_number = b.column_number and h.row_number = 1 "
                                        "where b.column_role = 'parameter' and b.row_number > 1 and b.cell_type <> 'n' "
                                        "and not regexp_full_match(trim(b.value_text), '<?\\s*-?[0-9]+([.,][0-9]+)?|n\\.o\\.') "
                                        "group by all order by 4 desc limit 30")
    p["numeric_cells_not_numbers"] = one(con, "select count(*) from bronze_cells where cell_type = 'n' and try_cast(value_text as double) is null")
    p["class_values"] = rows(con, "select column_role, trim(value_text), count(*) from bronze_cells where column_role in "
                                  "('final_class', 'parameter_class', 'monitoring_type') and row_number > 1 group by all order by all")
    return p


def silver(con) -> dict:
    s: dict = {}
    for model in SILVER:
        sql = render(f"warehouse/models/silver/gios_groundwater/{model}.sql")
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table {model} as {sql}")
            counts.append(one(con, f"select count(*) || ':' || coalesce(sum(hash(m)), 0) from {model} as m"))
        s[model] = {"rows": int(counts[0].split(":")[0]), "rebuild_identical": counts[0] == counts[1],
                    "tests": schema_tests(con, model)}
    return s


def silver_profile(con) -> dict:
    p: dict = {}
    p["classes_by_year"] = rows(con, "select year, monitoring_network, monitoring_network_stated, count(*), "
                                     "string_agg(final_class || '=' || n, ' ' order by final_class) from (select year, monitoring_network, "
                                     "monitoring_network_stated, final_class, count(*) n from gios_groundwater_quality_classes group by all) "
                                     "group by all order by 1")
    p["classes_point_dup"] = rows(con, "select year, count(*) - count(distinct coalesce(point_no_monbada, '') || '|' || coalesce(eu_point_id_172, '') "
                                       "|| '|' || coalesce(point_no_cbdh, '') || '|' || coalesce(point_id_monitoring, '')) "
                                       "from gios_groundwater_quality_classes group by 1 having count(*) > count(distinct coalesce(point_no_monbada, '') "
                                       "|| '|' || coalesce(eu_point_id_172, '') || '|' || coalesce(point_no_cbdh, '') || '|' || coalesce(point_id_monitoring, '')) order by 1")
    p["classes_voivodeship_names"] = rows(con, "select voivodeship, count(*) from gios_groundwater_quality_classes group by 1 order by 1")
    p["classes_study_year_mismatch"] = one(con, "select count(*) from gios_groundwater_quality_classes where study_year <> year")
    p["samples_by_entity_kind"] = rows(con, "select workbook, monitoring_network, min(year), max(year), count(distinct entity), count(*), "
                                            "count(*) filter (where sample_date is null), count(*) filter (where study_year <> year), "
                                            "count(*) filter (where not regexp_full_match(coalesce(lp, ''), '[0-9]+(\\.0)?')) "
                                            "from gios_groundwater_samples group by all order by 1, 3")
    p["samples_date_text"] = rows(con, "select sample_date_text, count(*) from gios_groundwater_samples where sample_date_text is not null group by 1 order by 2 desc limit 10")
    p["samples_date_range"] = one(con, "select min(sample_date) || '..' || max(sample_date) from gios_groundwater_samples")
    p["samples_lp_not_numeric"] = rows(con, "select entity, row_number, lp, voivodeship from gios_groundwater_samples "
                                            "where not regexp_full_match(coalesce(lp, ''), '[0-9]+(\\.0)?') limit 10")
    for m in SILVER[2:5]:
        p[m] = {
            "flags": rows(con, f"select value_flag, count(*), count(value), min(value), max(value) from {m} group by 1 order by 2 desc"),
            "years": one(con, f"select min(year) || '-' || max(year) from {m}"),
            "parameters": one(con, f"select count(distinct parameter) from {m}"),
            "no_unit": rows(con, f"select parameter, count(*) from {m} where unit is null group by 1 order by 2 desc limit 8"),
            "other": rows(con, f"select entity, column_number, left(parameter, 50), count(*), min(value_text), max(value_text) from {m} "
                               f"where value_flag = 'other' group by all order by 4 desc limit 10"),
            "zero_values": one(con, f"select count(*) from {m} where value = 0"),
            "negative_values": rows(con, f"select parameter, count(*) from {m} where value < 0 group by 1 order by 2 desc limit 5"),
        }
        show(con, f"{m} parameters", f"select parameter, count(*), count(value), min(year), max(year) from {m} group by 1 order by 1")
    p["markers_total"] = one(con, "select " + " + ".join(f"(select count(*) from {m} where value_flag in ('below_limit', 'not_determined'))" for m in SILVER[2:5]))
    p["indicator_classes"] = rows(con, "select parameter, string_agg(quality_class || '=' || n, ' ' order by quality_class) from "
                                       "(select parameter, quality_class, count(*) n from gios_groundwater_indicator_classes group by all) group by 1 order by 1")
    return p


def gold(con) -> dict:
    g: dict = {}
    sql = render("warehouse/models/gold/poc_build_gios_groundwater.sql")
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {sql}")
        counts.append(one(con, "select count(*) || ':' || sum(hash(b)) from build as b"))
    g["rows_two_builds"] = counts
    g["identical"] = counts[0] == counts[1]
    g["columns"] = [d[0] for d in con.execute("select * from build limit 0").description]
    g["dup_series_date"] = one(con, "select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    "then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json, date_key "
                                    "from build group by all having count(*) > 1)")
    g["freq_per_measure"] = one(con, "select count(*) from (select measure_key from build group by 1 having count(distinct frequency) > 1)")
    g["rows_geography_unmatched"] = one(con, "select count(*) from build where geography_key = '_X'")
    g["null_value"] = one(con, "select count(*) from build where value is null")
    g["measures"] = rows(con, "select measure_key, unit_code, count(*), count(distinct date_key), min(period_label), max(period_label) "
                              "from build group by all order by 1")
    g["kpi_rows_per_area_period_max"] = one(con, "select coalesce(max(n), 0) from (select geography_key, date_key, count(*) n from build "
                                                 "where measure_key like 'gios_groundwater:quality_classes:pct:I-III:diagnostic' and is_total group by all)")
    g["kpi_values"] = rows(con, "select period_label, round(value, 2) from build where measure_key = 'gios_groundwater:quality_classes:pct:I-III:diagnostic' order by 1")
    g["kpi_out_of_bounds"] = one(con, "select count(*) from build where unit_code = 'pct' and (value < 0 or value > 100)")
    g["newest_PL"] = rows(con, "select measure_key, period_label, round(value, 2) from build where is_total qualify row_number() over "
                               "(partition by measure_key order by date_key desc) = 1 order by 1")
    g["classes_2025"] = rows(con, "select breakdown_json, value from build where period_label = '2025' and unit_code = 'points' order by 1")
    g["totals_match_silver"] = one(con, "select count(*) from (select year, count(*) n from gios_groundwater_quality_classes group by 1) s "
                                        "join build b on b.period_label = cast(s.year as varchar) and b.unit_code = 'points' and b.is_total "
                                        "where b.value <> s.n")
    return g


def lint() -> dict:
    out: dict = {}
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "sqlfluff==4.4.0"], check=True, timeout=600)
        r = subprocess.run([sys.executable, "-m", "sqlfluff", "lint", *SQL_FILES, "--disable-progress-bar", "--processes", "2"],
                           capture_output=True, text=True, timeout=900)
        print(r.stdout[-20000:], r.stderr[-3000:], flush=True)
        out["exit"] = r.returncode
        out["violations"] = [ln.strip()[:160] for ln in r.stdout.splitlines() if ln.startswith("L:")][:25]
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    return out


def cleanup() -> dict:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(table[-1])
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/"))
    return removed


def main(argv: list[str]) -> int:
    status = 0
    try:
        note("gios_groundwater trial lint", {**STATS, "sqlfluff": lint()})
        land = landing_runs()
        note("gios_groundwater trial landing", land)
        con = duckdb.connect()
        b = bronze_runs(con)
        note("gios_groundwater trial bronze", b)
        note("gios_groundwater trial bronze profile", bronze_profile(con))
        s = silver(con)
        note("gios_groundwater trial silver", s, "warning" if any(
            v for m in s.values() for k, v in m["tests"].items() if not k.endswith("sample") and v) else "notice")
        sp = silver_profile(con)
        note("gios_groundwater trial silver profile", {k: v for k, v in sp.items() if not k.startswith("gios_groundwater_results")})
        note("gios_groundwater trial silver results", {k: v for k, v in sp.items() if k.startswith("gios_groundwater_results")} | {"markers_total": sp["markers_total"]})
        note("gios_groundwater trial gold", gold(con))
    except Exception as exc:  # noqa: BLE001
        status = 1
        import traceback

        traceback.print_exc()
        annotate("error", "gios_groundwater trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        note("gios_groundwater trial cleanup", cleanup())
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
