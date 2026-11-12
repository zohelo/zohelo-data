"""Dev trial (#2861, deleted before merge): the whole Gold project (seeds, models, tests) built by dbt against
production Silver into the scratch namespace zz_dev_intl5 (dbt var gold_schema), with the new build stages of BIS,
ILOSTAT, UNDP HDR, UNdata and UNECE; then checks read from the scratch tables: rows per block and measure, grain by
series_key x date_key, unmatched geography, the facts and metrics the new sources reach, value ranges, Poland's
newest values, and what the build stages leave out of Silver. Read-only against production; the scratch tables
are dropped at the end. Report: one plain summary notice and up to nine notices holding the zlib+base64 JSON detail."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import re
import subprocess
import time
import zlib

import duckdb
import pyarrow as pa

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
NS = "zz_dev_intl5"
NEW = ("bis", "ilostat", "undp_hdr", "undata", "unece")
NEW_SQL = "('bis', 'ilostat', 'undp_hdr', 'undata', 'unece')"
BUILDS = ("build_bis", "build_ilostat", "build_undp_hdr", "build_undata", "build_unece")
EU27 = ("AUT,BEL,BGR,HRV,CYP,CZE,DNK,EST,FIN,FRA,DEU,GRC,HUN,IRL,ITA,LVA,LTU,LUX,MLT,NLD,POL,PRT,ROU,SVK,SVN,"
        "ESP,SWE").split(",")
OUT: dict = {}
SUMMARY: dict = {}

# Silver tables of sources merged on main but not built in production yet (trial only: poc_map_geography reads
# them, so the branch copy is patched to an empty stub and their blocks wait, as the changed-only build does).
STUBS = {
    "unesco_uis_observations": ("poc_build_unesco_uis", "select ''::varchar as geo_unit, false as is_country_code where false"),
    "who_observations": ("poc_build_who", "select ''::varchar as geo_type, ''::varchar as geo_code_m49 where false"),
    "faostat_observations": ("poc_build_faostat", "select 0::integer as area_code, ''::varchar as area_code_m49 where false"),
}


def q(con, key: str, sql: str, limit: int = 400) -> list:
    try:
        rows = con.sql(sql).limit(limit).fetchall()
        OUT[key] = ["|".join("" if v is None else str(v) for v in r) for r in rows]
        return rows
    except Exception as exc:  # reported, the trial goes on
        OUT[key] = {"error": f"{type(exc).__name__}: {str(exc)[:500]}"}
        return []


def table(cat, ns: str, name: str, fields=None):
    scan = cat.load_table((ns, name)).scan(selected_fields=fields) if fields else cat.load_table((ns, name)).scan()
    return bf.retry(scan.to_arrow, name)


def reg(con, cat, alias: str, ns: str, name: str, fields=None) -> bool:
    """Register a catalog table in DuckDB; a missing table is reported, not fatal."""
    try:
        con.register(alias, table(cat, ns, name, fields))
        return True
    except Exception as exc:
        OUT.setdefault("missing_tables", {})[f"{ns}.{name}"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        return False


def records(cat, ns: str, name: str) -> int:
    try:
        snap = cat.load_table((ns, name)).current_snapshot()
        return int((snap.summary or {}).get("total-records", -1)) if snap else 0
    except Exception:  # a table that does not exist
        return -1


def missing_silver(cat) -> list[str]:
    """Blocks whose Silver tables production does not have yet (they wait, as in the changed-only build), found from
    the refs of every poc_build_*.sql; poc_map_geography's refs to such tables are replaced by empty stubs."""
    have = {i[-1] for i in cat.list_tables("silver")}
    silver_models = {p.stem for p in (ROOT / "warehouse/models/silver").rglob("*.sql")}
    waiting = []
    for model in sorted((ROOT / "warehouse/models/gold").glob("poc_build_*.sql")):
        refs = set(re.findall(r"ref\('([a-z0-9_]+)'\)", model.read_text())) & silver_models
        if refs - have:
            waiting.append(model.stem)
            OUT.setdefault("waiting_because", {})[model.stem] = sorted(refs - have)
    path = ROOT / "warehouse/models/gold/poc_map_geography.sql"
    sql = path.read_text()
    for name, (_block, stub) in STUBS.items():
        if name not in have:
            sql = sql.replace("{{ ref('" + name + "') }}", "(" + stub + ")")
    unstubbed = (set(re.findall(r"\{\{ ref\('([a-z0-9_]+)'\) \}\}", sql)) & silver_models) - have
    if unstubbed:
        OUT["map_geography_missing_silver"] = sorted(unstubbed)
    path.write_text(sql)
    SUMMARY["silver_present"] = {t: t in have for t in ("bis_observations", "ilostat_observations", "undp_hdr_indices",
                                                        "undata_syb_values", "unece_observations")}
    return waiting


def build(cat) -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "warehouse"), "DUCKDB_TEMP_DIRECTORY": "/mnt/duckdb_tmp"}
    subprocess.run(["sudo", "mkdir", "-p", "/mnt/duckdb_tmp"], check=False)
    subprocess.run(["sudo", "chmod", "777", "/mnt/duckdb_tmp"], check=False)
    started, failed, counts = time.monotonic(), [], {}
    waiting = missing_silver(cat)
    variables = json.dumps({"gold_schema": NS, "waiting_blocks": waiting})
    excl = ["--exclude", *[f"path:models/gold/{b}.sql" for b in waiting], "path:models/gold/topics",
            "path:tests/topic_quality.sql", "path:tests/topic_quality_warn.sql", "path:tests/topic_seeds.sql"]
    for verb, sel in (("seed", ["path:seeds"]), ("run", ["path:models/gold"]),
                      ("test", ["path:models/gold", "path:seeds", "path:tests"])):
        cmd = ["dbt", verb, "--target", "prod", "--select", *sel, *excl, "--threads", "2", "--vars", variables]
        run = subprocess.run(cmd, cwd=ROOT / "warehouse", env=env, capture_output=True, text=True)
        for wait in (60, 180, 300):  # HTTP 429 from the catalog
            if run.returncode == 0 or verb == "test" or "429" not in run.stdout:
                break
            time.sleep(wait)
            run = subprocess.run(["dbt", "retry", "--target", "prod", "--vars", variables],
                                 cwd=ROOT / "warehouse", env=env, capture_output=True, text=True)
        print(run.stdout[-30000:])
        try:
            res = json.loads((ROOT / "warehouse/target/run_results.json").read_text())["results"]
        except Exception as exc:  # dbt stopped before writing results
            failed.append([verb, "no run_results", str(exc)[:80], run.stdout[-600:]])
            continue
        counts[verb] = {"nodes": len(res), "exit": run.returncode}
        manifest = json.loads((ROOT / "warehouse/target/manifest.json").read_text())["nodes"]
        for r in res:
            if r["status"] not in ("success", "pass"):
                node = manifest.get(r["unique_id"], {})
                deps = [d.split(".")[-1] for d in node.get("depends_on", {}).get("nodes", [])][:3]
                failed.append([verb, node.get("name", r["unique_id"])[:90], r["status"], (r.get("message") or "")[:160], deps])
    SUMMARY["dbt"] = {"minutes": round((time.monotonic() - started) / 60, 1), "waiting_blocks": waiting, "counts": counts,
                      "not_passed": len(failed)}
    OUT["dbt_not_passed"] = failed[:40]
    SUMMARY["not_passed_new"] = [[str(x)[:100] for x in f[:4]] for f in failed if any(s in json.dumps(f) for s in
                                 ("bis", "ilostat", "undp", "undata", "unece", "fact_grain", "value_range", "kpi", "unit", "series"))][:8]


SERIES_KEY = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")


def checks(cat) -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET preserve_insertion_order=false;")
    blocks = []
    OUT["schema"] = {}
    for name in BUILDS:
        try:
            t = table(cat, NS, name)
        except Exception as exc:
            OUT["schema"][name] = f"missing: {type(exc).__name__}: {str(exc)[:160]}"
            continue
        blocks.append(t)
        OUT["schema"][name] = [f"{f.name}:{f.type}" for f in t.schema]
    try:
        OUT["schema"]["build_wdi"] = [f"{f.name}:{f.type}" for f in cat.load_table((NS, "build_wdi")).schema().as_arrow()]
    except Exception as exc:
        OUT["schema"]["build_wdi"] = str(exc)[:200]
    if not blocks:
        raise RuntimeError("no new build table exists in the scratch namespace")
    con.register("b0", pa.concat_tables(blocks, promote_options="permissive"))
    con.execute(f"create table b as select *, {SERIES_KEY} as series_key from b0")
    rows = q(con, "blocks", """select source_system, block_id, count(*), count(distinct series_key),
        count(distinct measure_key), count(distinct geography_key), min(date_key), max(date_key),
        string_agg(distinct frequency, ','), string_agg(distinct observation_status_key, ','),
        count(*) filter (where geography_key = '_X'),
        coalesce(string_agg(distinct geography_source_code, ',') filter (where geography_key = '_X'), ''),
        count(*) filter (where is_total), count(*) filter (where value is null), count(*) filter (where date_key is null)
        from b group by 1, 2 order by 1, 2""")
    SUMMARY["rows"] = {s: sum(r[2] for r in rows if r[0] == s) for s in NEW}
    SUMMARY["unmatched_rows"] = sum(r[10] for r in rows)
    q(con, "measures", """select measure_key, any_value(unit_code), any_value(unit_name), count(*),
        count(distinct geography_key), min(date_key), max(date_key), count(distinct frequency),
        max(date_key) filter (where geography_key = 'PL' and is_total),
        arg_max(value, date_key) filter (where geography_key = 'PL' and is_total), min(value), max(value),
        arg_max(geography_key, value), left(any_value(measure_name), 60)
        from b group by 1 order by 1""")
    dup = q(con, "grain_duplicates_builds", """select source_system, count(*) from (select source_system, series_key,
        date_key from b group by all having count(*) > 1) group by 1""")
    multi = q(con, "measures_with_several_frequencies", """select measure_key, string_agg(distinct frequency, ',')
        from b group by 1 having count(distinct frequency) > 1""")
    q(con, "areas", """select source_system, count(distinct geography_key),
        string_agg(distinct geography_key, ',' order by geography_key) from b group by 1 order by 1""")
    q(con, "nulls_and_zeros", """select source_system, count(*) filter (where value is null),
        count(*) filter (where value = 0), string_agg(distinct measure_key, ',') filter (where value = 0)
        from b group by 1 order by 1""")
    q(con, "statuses", """select source_system, block_id, observation_status_key, count(*) from b
        group by 1, 2, 3 order by 1, 2, 3""")
    q(con, "unece_zero_rates", """select measure_key, count(*) from b where source_system = 'unece' and value = 0
        group by 1 order by 1""")
    q(con, "unece_aggregates", """select block_id, geography_key, count(*) from b where source_system = 'unece'
        and geography_key like 'EUROSTAT:%' group by 1, 2 order by 1, 2""")
    q(con, "newest_periods", """select source_system, frequency, max(date_key), count(*) filter (where date_key > current_date)
        from b group by 1, 2 order by 1, 2""")
    q(con, "pl_series", """select measure_key, breakdown_json, count(*), min(date_key), max(date_key),
        arg_max(value, date_key), arg_max(observation_status_key, date_key) from b
        where geography_key = 'PL' group by 1, 2 order by 1, 2""", 200)

    facts = []
    fact_rows = {}
    for name in ("fact_annual", "fact_semiannual", "fact_quarterly", "fact_monthly", "fact_daily"):
        try:
            t = table(cat, NS, name, ("series_key", "date_key", "source_system", "frequency"))
        except Exception as exc:
            fact_rows[name] = {"missing": f"{type(exc).__name__}: {str(exc)[:160]}"}
            continue
        facts.append(t)
        fact_rows[name] = {"scratch": t.num_rows, "production_now": records(cat, "gold", name)}
    if facts:
        con.register("f", pa.concat_tables(facts))
    fdup = q(con, "grain_duplicates_facts", """select source_system, count(*) from (select series_key, date_key,
        any_value(source_system) as source_system from f group by 1, 2 having count(*) > 1) group by 1""")
    for r in q(con, "fact_rows_new_sources", f"""select frequency, source_system, count(*) from f
        where source_system in {NEW_SQL} group by 1, 2 order by 1, 2"""):
        fact_rows.setdefault("new_" + r[0], {})[r[1]] = r[2]
    OUT["fact_rows"] = fact_rows
    SUMMARY["grain"] = {"duplicate_keys_builds": dup, "duplicate_keys_facts_all_sources": fdup,
                        "measures_with_several_frequencies": multi}
    SUMMARY["facts"] = {k: v for k, v in fact_rows.items() if not k.startswith("new_")}
    OUT["other_rows"] = {n: {"scratch": records(cat, NS, n), "production_now": records(cat, "gold", n)}
                         for n in ("dim_series", "dim_measure", "dim_unit", "map_geography", "concept_measures",
                                   "build_check", "grain_check", "kpi", "kpi_measure", "kpi_v2_map", "unit")}

    reg(con, cat, "bc", NS, "build_check")
    q(con, "build_check", f"""select source_system, block_id, row_count, series, measures, rows_geography_unmatched,
        rows_total_series, rows_period_unplaced, first_date, last_date, frequencies, statuses, note
        from bc where source_system in {NEW_SQL} order by 1, 2""")
    q(con, "build_check_waiting_or_empty", "select source_system, block_id, note from bc where row_count = 0", 40)
    reg(con, cat, "gc", NS, "grain_check")
    q(con, "grain_check", "select count(*) from gc")
    reg(con, cat, "mg", NS, "map_geography")
    q(con, "map_geography", f"""select source_system, count(*), count(*) filter (where not matched),
        coalesce(string_agg(source_code, ',') filter (where not matched), '') from mg
        where source_system in {NEW_SQL} group by 1 order by 1""")
    reg(con, cat, "cm", NS, "concept_measures")
    cm = q(con, "concept_measures", f"""select concept_key, source_system, measure_key from cm
        where source_system in {NEW_SQL} order by 1, 2, 3""")
    reg(con, cat, "dm", NS, "dim_measure")
    q(con, "dim_measure", f"""select measure_key, unit_key, frequencies, series, points, areas, first_date, last_date
        from dm where source_system in {NEW_SQL} order by 1""")
    reg(con, cat, "du", NS, "dim_unit")
    q(con, "dim_unit", """select unit_key, unit_name, conformed_unit, sources from du where
        list_has_any(sources, ['bis', 'ilostat', 'undp_hdr', 'undata', 'unece']) order by 1""")
    q(con, "dim_unit_unmapped", "select unit_key, unit_name, sources from du where not mapped order by 1", 60)

    metrics = {}
    for concept in sorted({r[0] for r in cm}):
        try:
            t = table(cat, NS, f"metrics_{concept}")
            con.register("m", t)
            total = t.num_rows
            rows = con.sql(f"""select source_system, measure_key, count(*), count(distinct geography_key), min(date_key),
                max(date_key), min(value), max(value), arg_max(geography_key, value),
                max(date_key) filter (where geography_key = 'PL'),
                arg_max(value, date_key) filter (where geography_key = 'PL'), string_agg(distinct frequency, ',')
                from m where source_system in {NEW_SQL} group by 1, 2 order by 1, 2""").fetchall()
            other = con.sql(f"""select source_system, max(date_key), arg_max(value, date_key) from m
                where geography_key = 'PL' and source_system not in {NEW_SQL} and frequency = 'A'
                group by 1 order by 1""").fetchall()
            per = con.sql(f"""select max(n) from (select count(*) as n from m where source_system in {NEW_SQL}
                group by source_system, geography_key, date_key, frequency)""").fetchall()[0][0]
            metrics[concept] = {"rows": total, "max_rows_per_source_area_period": per, "production_now": records(cat, "gold", f"metrics_{concept}"),
                                "new": [[str(v) for v in r] for r in rows],
                                "pl_other_sources_annual": [[str(v) for v in r] for r in other]}
            con.unregister("m")
        except Exception as exc:
            metrics[concept] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    OUT["metrics"] = metrics
    SUMMARY["metrics_rows"] = {k: v.get("rows") for k, v in metrics.items()}

    try:  # the value-range test's SQL on the scratch metrics tables
        sql = (ROOT / "warehouse/tests/metrics_value_range.sql").read_text()
        for name in sorted(set(re.findall(r"\{\{ ref\('([a-z0-9_]+)'\) \}\}", sql))):
            con.register(f"t_{name}", table(cat, NS, name.replace("poc_", "", 1)))
            sql = sql.replace(f"{{{{ ref('{name}') }}}}", f"t_{name}")
        OUT["value_range_new"] = [[str(v) for v in r] for r in con.sql(f"""select concept_key, source_system,
            count(*), min(value), max(value), any_value(value_min), any_value(value_max),
            count(*) filter (where value_min is null or value < value_min or value > value_max)
            from ({sql}) where source_system in {NEW_SQL} group by 1, 2 order by 1, 2""").fetchall()]
        OUT["value_range_outside_all"] = con.sql(f"""select count(*) from ({sql})
            where value_min is null or value < value_min or value > value_max""").fetchall()[0][0]
    except Exception as exc:
        OUT["value_range_new"] = {"error": f"{type(exc).__name__}: {str(exc)[:400]}"}
    silver(con, cat)


def silver(con, cat) -> None:
    """What the build stages leave out of production Silver, and the unit test's SQL."""
    eu = ", ".join(f"'{c}'" for c in EU27)
    OUT["silver_rows"] = {t: records(cat, "silver", t) for t in ("bis_observations", "ilostat_observations",
                                                                 "undp_hdr_indices", "undata_syb_values",
                                                                 "unece_observations")}
    reg(con, cat, "ilo", "silver", "ilostat_observations", ("entity", "ref_area", "sex", "classif1", "classif2",
                                                           "time_period", "obs_value", "obs_status", "source_code"))
    q(con, "ilostat_left_out", f"""select entity, count(*) filter (where ref_area in ({eu})),
        count(*) filter (where ref_area in ({eu}) and (classif1 like 'AGE_YTHADULT%' or classif1 is null)),
        count(*) filter (where ref_area in ({eu}) and (classif1 like 'AGE_YTHADULT%' or classif1 is null) and sex = 'SEX_O'),
        count(*) filter (where ref_area in ({eu}) and (classif1 like 'AGE_YTHADULT%' or classif1 is null) and obs_value is null),
        count(*) filter (where ref_area in ({eu}) and classif2 is not null),
        count(*) filter (where ref_area in ({eu}) and try_cast(left(time_period, 4) as integer) > year(current_date)),
        count(*) filter (where try_cast(left(time_period, 4) as integer) > year(current_date)),
        coalesce(string_agg(distinct ref_area, ',') filter (where try_cast(left(time_period, 4) as integer) > year(current_date)), ''),
        string_agg(distinct coalesce(obs_status, '-'), ',') filter (where ref_area in ({eu}))
        from ilo group by 1 order by 1""")
    q(con, "ilostat_several_surveys_per_period", f"""select entity, count(*) from (select entity, ref_area, sex,
        classif1, time_period from ilo where ref_area in ({eu}) group by all having count(*) > 1) group by 1""")
    q(con, "ilostat_eu27_latest_total", """select ref_area, max(time_period), arg_max(obs_value, time_period),
        arg_max(source_code, time_period) from ilo where entity = 'une_deap_sex_age_rt_a' and sex = 'SEX_T'
        and classif1 = 'AGE_YTHADULT_YGE15' and obs_value is not null and ref_area in (""" + eu + """)
        group by 1 order by 1""")
    reg(con, cat, "bis_observations", "silver", "bis_observations", ("entity", "series_key", "ref_area", "dimensions_json",
                                                                     "time_period", "obs_value", "unit_measure",
                                                                     "obs_status", "freq"))
    q(con, "bis_left_out", """select entity, count(*), count(*) filter (where obs_value is null),
        count(*) filter (where ref_area in ('4T', '5R', '5A', 'G2', 'XW')), count(distinct series_key),
        string_agg(distinct coalesce(obs_status, '-'), ','), string_agg(distinct unit_measure, ',')
        from bis_observations group by 1 order by 1""")
    reg(con, cat, "syb", "silver", "undata_syb_values", ("entity", "area_code", "year", "series", "value", "value_text",
                                                         "footnotes"))
    q(con, "undata_left_out", """select entity, series, count(*), count(*) filter (where value is null),
        coalesce(string_agg(distinct value_text, ',') filter (where value is null), ''),
        count(*) filter (where footnotes ilike '%project%'), count(*) filter (where footnotes ilike '%estimate%'),
        min(year), max(year) from syb where entity in ('syb_230', 'syb_285', 'syb_314', 'syb_325')
        group by 1, 2 order by 1, 2""")
    q(con, "undata_pl_footnotes", """select entity, series, year, value, left(footnotes, 90) from syb
        where area_code = '616' and entity in ('syb_230', 'syb_285', 'syb_314', 'syb_325') and footnotes is not null
        order by 1, 2, 3""", 60)
    q(con, "undata_footnote_status", """select case
            when nullif(trim(footnotes), '') is null then 'ok'
            when regexp_full_match(footnotes, '(?i)\\s*(estimates?|estimated( data| values?)?)\\.?\\s*') then 'estimated'
            when regexp_full_match(footnotes, '(?i)\\s*break in (the )?(time )?series\\.?\\s*') then 'break'
            when regexp_full_match(footnotes, '(?i)\\s*(provisional|preliminary)( data| estimates?)?\\.?\\s*') then 'provisional'
            else 'definition_differs' end, left(coalesce(footnotes, ''), 110), count(*),
        count(*) filter (where footnotes ilike '%estimate%'), count(*) filter (where footnotes ilike '%break%')
        from syb where entity in ('syb_230', 'syb_285', 'syb_314', 'syb_325') and value is not null
        and series in ('GDP in current prices (millions of US dollars)', 'GDP per capita (US dollars)',
            'GDP real rates of growth (percent)', 'Gross domestic expenditure on R&D: as a percentage of GDP',
            'Percentage of individuals using the internet', 'Current health expenditure (% of GDP)')
        group by 1, 2 order by 3 desc""", 70)
    unece(con, cat)


def unece(con, cat) -> None:
    """UNECE: Silver rows of the built tables against the build, zeros in level series, areas left out."""
    built = ("40_en_MEUnRateY_r", "55_en_MEUnRateM_r", "0_en_MECCPriceInterestY_r", "1_en_MECCPriceInterestQ_r",
             "60_en_MECCWagesY_r", "0_en_MECCIndProdY_r", "10_en_MELFOverviewY_r", "30_en_MECCExchPPPsNEWY_r",
             "30_en_MEEmpTotQ_r")
    tables = ", ".join(f"'{t}'" for t in built)
    if not reg(con, cat, "une0", "silver", "unece_observations", ("table_id", "area_code", "area_name", "dimensions_json",
                                                              "time_period", "obs_value", "obs_status")):
        return
    con.execute(f"create table une as select * from une0 where table_id in ({tables})")
    q(con, "unece_silver", """select table_id, count(*), count(*) filter (where obs_value is null),
        count(*) filter (where area_code in ('CIS', 'CWG', 'EAEU', 'ECE', 'NA', 'ST7')),
        count(*) filter (where obs_value = 0), count(*) filter (where obs_status is not null),
        count(distinct area_code), min(time_period), max(time_period) from une group by 1 order by 1""")
    q(con, "unece_silver_zeros", """select table_id, concat_ws('.', json_extract_string(dimensions_json, '$.Indicator'),
        json_extract_string(dimensions_json, '$.Variable'), json_extract_string(dimensions_json, '$.Measurement')),
        count(*), count(distinct area_code), min(time_period), max(time_period),
        string_agg(distinct area_name, ',' order by area_name)[:120]
        from une where obs_value = 0 group by 1, 2 order by 1, 2""", 60)
    q(con, "unece_silver_all_tables", "select count(distinct table_id), count(*) from une0")


def unmapped() -> None:
    """The rows of the warning test metrics_unit_unmapped (dbt show prints the test's query result)."""
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "warehouse"), "DUCKDB_TEMP_DIRECTORY": "/mnt/duckdb_tmp"}
    variables = json.dumps({"gold_schema": NS, "waiting_blocks": SUMMARY.get("dbt", {}).get("waiting_blocks", [])})
    try:
        run = subprocess.run(["dbt", "show", "--target", "prod", "--select", "metrics_unit_unmapped", "--limit", "20",
                              "--output", "json", "--log-format", "json", "--vars", variables], cwd=ROOT / "warehouse",
                             env=env, capture_output=True, text=True, timeout=900)
        found = re.findall(r'\\"measure_key\\": \\"([^"\\]+)\\"[^}]*?\\"unit_code\\": \\"([^"\\]*)\\"', run.stdout)
        OUT["metrics_unit_unmapped"] = sorted(set(found))[:30] or run.stdout[-700:]
    except Exception as exc:  # reported
        OUT["metrics_unit_unmapped"] = f"{type(exc).__name__}: {str(exc)[:200]}"


def cleanup(cat) -> None:
    dropped, failed = 0, []
    for _ in range(4):
        left = list(cat.list_tables(NS))
        if not left:
            break
        for ident in left:
            err = ""
            for wait in (0, 10, 30, 60):  # HTTP 429 from the catalog
                time.sleep(wait)
                try:
                    cat.drop_table(ident)
                    dropped += 1
                    break
                except Exception as exc:  # retried, then reported
                    err = f"{ident[-1]}: {type(exc).__name__}"
            else:
                failed.append(err)
    left = [i[-1] for i in cat.list_tables(NS)]
    if not left:
        try:
            cat.drop_namespace(NS)
        except Exception as exc:  # reported
            failed.append(f"namespace: {type(exc).__name__}")
    SUMMARY["scratch"] = {"namespace": NS, "tables_dropped": dropped, "left": left[:20], "failed": failed[:5]}


def emit() -> None:
    annotate("notice", "p00 summary", SUMMARY)
    drop_order = ["undata_pl_footnotes", "ilostat_eu27_latest_total", "schema", "ilostat_left_out", "dim_measure", "pl_series", "areas"]
    dropped: list[str] = []
    while True:
        OUT["dropped_for_size"] = dropped
        raw = json.dumps(OUT, ensure_ascii=False, separators=(",", ":"), default=str).encode()
        text = base64.b64encode(zlib.compress(raw, 9)).decode()
        if len(text) <= 9 * 3700 or not drop_order:
            break
        key = drop_order.pop(0)
        if key in OUT:
            OUT.pop(key)
            dropped.append(key)
    parts = [text[i:i + 3700] for i in range(0, len(text), 3700)][:9]
    for i, part in enumerate(parts, 1):
        annotate("notice", f"p{i:02d} of {len(parts)}", part)


def main() -> int:
    cat = bf.retry(catalog, "catalog")
    try:
        build(cat)
        checks(cat)
        unmapped()
    except Exception as exc:
        SUMMARY["fatal"] = f"{type(exc).__name__}: {str(exc)[:700]}"
    finally:
        try:
            cleanup(cat)
        except Exception as exc:
            SUMMARY["scratch"] = {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}
        emit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
