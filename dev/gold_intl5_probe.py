"""Read-only probe (test branch only, never merged): profile production Silver of bis, ilostat, undp_hdr, unece
and undata for the Gold build stages (#2859). No writes. At most 10 annotations."""

from __future__ import annotations

import json
import time

import duckdb

from ingestion.common.r2 import req
from ingestion.common.report import annotate

LIMIT = 3700


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET preserve_insertion_order=false;")
    for ext in ("httpfs", "iceberg"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute(f"""CREATE SECRET r2_catalog (TYPE iceberg, TOKEN '{req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")}')""")
    con.execute(
        f"""CREATE SECRET r2_files (TYPE s3, KEY_ID '{req("STORAGE_WRITE_ACCESS_KEY")}',
        SECRET '{req("STORAGE_WRITE_SECRET_KEY")}', ENDPOINT '{endpoint}', REGION 'auto', URL_STYLE 'path')"""
    )
    con.execute(
        f"""ATTACH '{req("STORAGE_LAKEHOUSE_CATALOG_NAME")}' AS r2 (TYPE iceberg, SECRET r2_catalog,
        ENDPOINT '{req("STORAGE_LAKEHOUSE_CATALOG_URL")}')"""
    )
    return con


def q(con, sql: str, width: int = 60):
    try:
        return [[str(v)[:width] for v in r] for r in con.execute(sql).fetchall()]
    except Exception as exc:  # noqa: BLE001 - reported, the probe goes on
        return [f"ERR {type(exc).__name__}: {str(exc)[:200]}"]


def emit(title: str, parts: dict) -> None:
    out, used = {}, 0
    for key, rows in parts.items():
        kept = []
        for row in rows if isinstance(rows, list) else [rows]:
            size = len(json.dumps(row, ensure_ascii=False, separators=(",", ":"))) + 1
            if used + size > LIMIT:
                kept.append("CUT")
                break
            kept.append(row)
            used += size
        out[key] = kept
    annotate("notice", title, out)


def local(con, name: str, table: str, where: str = "true") -> bool:
    started = time.monotonic()
    try:
        con.execute(f"create or replace temp table {name} as select * from r2.silver.{table} where {where}")
        n = con.execute(f"select count(*) from {name}").fetchone()[0]
        TABLES[name] = [n, round(time.monotonic() - started)]
        return True
    except Exception as exc:  # noqa: BLE001
        TABLES[name] = f"ERR {type(exc).__name__}: {str(exc)[:160]}"
        con.execute(f"create or replace temp table {name} as select 1 as missing where false")
        return False


TABLES: dict = {}
CODES = """array_to_string(list_transform(list_filter(json_keys(dimensions_json::json), x -> x not in
    ('FREQ','REF_AREA','BORROWERS_CTY')), x -> json_extract_string(dimensions_json, '$.' || x)), ':')"""


def main() -> int:
    con = connect()
    local(con, "bis", "bis_observations")
    local(con, "ilo", "ilostat_observations")
    local(con, "hdr", "undp_hdr_indices")
    local(con, "unece", "unece_observations")
    local(con, "undata", "undata_syb_values")

    emit("1 tables, bis per entity", {
        "tables": [TABLES],
        "bis": q(con, """select entity, count(*), count(*) filter (where obs_value is null), list(distinct freq),
            list(distinct unit_measure), list(distinct unit_mult), list(distinct obs_status),
            any_value(json_keys(dimensions_json::json)), count(distinct ref_area), min(time_period), max(time_period),
            max(ingested_at)::varchar from bis group by 1 order by 1""", 90),
        "non_iso2": q(con, """select entity, list(distinct ref_area order by ref_area) from bis
            where not regexp_matches(ref_area, '^[A-Z]{2}$') or ref_area like 'X%' or regexp_matches(ref_area, '[0-9]')
            group by 1 order by 1""", 200),
        "x_titles": q(con, """select ref_area, any_value(title) from bis where ref_area in ('XM','XW','4T','5R','5A','G2')
            group by 1""", 80),
    })
    emit("2 bis measures", {
        "m": q(con, f"""select entity, {CODES} as codes, unit_measure, freq, count(distinct ref_area), count(*),
            max(time_period) filter (where ref_area = 'PL' and obs_value is not null),
            arg_max(obs_value, time_period) filter (where ref_area = 'PL' and obs_value is not null),
            any_value(title) filter (where ref_area = 'PL')
            from bis where entity <> 'tc' group by all order by 1, 2""", 70),
    })
    emit("3 bis tc", {
        "units": q(con, "select unit_measure, unit_mult, count(*), count(distinct series_key) from bis where entity = 'tc' group by all"),
        "m770": q(con, f"""select {CODES} as codes, unit_measure, count(distinct ref_area), count(*),
            max(time_period) filter (where ref_area = 'PL' and obs_value is not null),
            arg_max(obs_value, time_period) filter (where ref_area = 'PL' and obs_value is not null),
            any_value(title) filter (where ref_area = 'PL')
            from bis where entity = 'tc' and (unit_measure = '770' or series_key like '%.770.%') group by all order by 1""", 70),
        "dims": q(con, "select distinct dimensions_json from bis where entity = 'tc' and ref_area = 'PL' limit 6", 160),
    })
    emit("4 ilostat per entity", {
        "e": q(con, """select entity, freq, count(*), count(*) filter (where obs_value is null), list(distinct sex),
            list(distinct obs_status), count(distinct ref_area), min(time_period), max(time_period),
            list(distinct regexp_replace(time_period, '[0-9]', '9', 'g')), count(classif2), max(ingested_at)::varchar,
            list(distinct regexp_extract(coalesce(classif1, '-'), '^(AGE_[A-Z0-9]+)_', 1))
            from ilo group by all order by 1""", 150),
        "yth": q(con, "select entity, list(distinct classif1 order by classif1) from ilo where classif1 like 'AGE_YTHADULT%' or classif1 is null or classif1 like '%TOTAL' group by 1 order by 1", 220),
        "status": q(con, "select obs_status, count(*), count(obs_value) from ilo group by 1 order by 2 desc"),
        "non_iso3": q(con, "select list(distinct ref_area order by ref_area) from ilo where not regexp_matches(ref_area, '^[A-Z]{3}$') or ref_area like 'X%'", 500),
        "after_download": q(con, """select entity, time_period, count(*), count(distinct ref_area), list(distinct ref_area)[1:6],
            list(distinct obs_status) from ilo where try_cast(left(time_period, 4) as int) >= 2026 and obs_value is not null
            group by all order by 1, 2""", 80),
    })
    emit("5 ilostat PL and sources", {
        "pl": q(con, """select entity, classif1, source_code, max(time_period), arg_max(obs_value, time_period), count(*)
            from ilo where ref_area = 'POL' and sex = 'SEX_T' and obs_value is not null
            and (classif1 like 'AGE_YTHADULT%' or classif1 is null) group by all order by 1, 2, 3"""),
        "multi_source": q(con, """select entity, count(*) as area_periods, count(distinct ref_area), list(distinct ref_area)[1:12]
            from (select entity, ref_area, time_period, coalesce(classif1, '') c from ilo where sex = 'SEX_T' and obs_value is not null
            and (classif1 like 'AGE_YTHADULT%' or classif1 is null) group by all having count(distinct source_code) > 1)
            group by 1 order by 1""", 120),
        "sources_per_area": q(con, """select entity, n, count(*) from (select entity, ref_area, count(distinct source_code) n
            from ilo where sex = 'SEX_T' and obs_value is not null and (classif1 like 'AGE_YTHADULT%' or classif1 is null)
            group by all) group by all order by 1, 2"""),
        "hdr": q(con, """select indicator, max(year), arg_max(value, year) filter (where iso3 = 'POL'), count(distinct iso3),
            count(*) filter (where value is null) from hdr where indicator in ('hdi','hdi_f','hdi_m','le','le_f','le_m','eys',
            'eys_f','mys','mys_f','gnipc','gni_pc_f','gii','ihdi','gdi') group by 1 order by 1"""),
        "hdr_zz": q(con, "select distinct iso3, country from hdr where iso3 like 'ZZ%' or length(iso3) <> 3 order by 1"),
    })
    emit("6 undata general", {
        "e": q(con, """select entity, any_value(table_title), count(*), count(distinct series), max(year), count(*) filter (where value is null),
            bool_or(area_code = '616'), max(ingested_at)::varchar from undata group by 1 order by 1""", 50),
        "areas": q(con, """select list(distinct area_code order by area_code) filter (where length(area_code) < 3),
            count(distinct area_code), list(distinct area_code || '=' || area_name) filter (where area_code in ('1','001','616','97','097','40','040'))
            from undata""", 300),
    })
    emit("7 undata series", {
        "s": q(con, """select entity, series, max(year) filter (where area_code = '616'),
            arg_max(value, year) filter (where area_code = '616'), count(distinct year), count(distinct area_code)
            from undata where entity in ('syb_1','syb_230','syb_329','syb_128','syb_246','syb_200','syb_125','syb_314','syb_285','syb_310','syb_325')
            group by all order by case entity when 'syb_329' then 1 when 'syb_230' then 2 when 'syb_1' then 3 when 'syb_246' then 4
            when 'syb_128' then 5 when 'syb_200' then 6 else 7 end, entity, 2""", 75),
    })
    emit("8 unece tables", {
        "t": q(con, """select table_id, left(any_value(table_title), 58), count(distinct entity), count(*), min(time_period), max(time_period),
            count(distinct area_code), bool_or(area_code in ('616')), list(distinct regexp_replace(time_period, '[0-9]', '9', 'g')),
            list(distinct obs_status)[1:4] from unece group by 1 order by 1""", 62),
    })
    pri = """case when table_title ilike '%unemployment rate%' then 1 when table_title ilike '%wages%' then 2
        when table_title ilike '%consumer price%' then 3 when table_title ilike '%gross domestic product%' or table_title ilike '%GDP%' then 4
        when table_title ilike '%employment%' or table_title ilike '%labour force%' then 5 else 9 end"""
    emit("9 unece candidates", {
        "areas": q(con, """select list(distinct area_code order by area_code) filter (where not regexp_matches(area_code, '^[0-9]+$')),
            list(distinct area_code order by area_code)[1:8], list(distinct area_code || '=' || area_name) filter (where area_code in ('616','40','040','E27','EAA'))
            from unece""", 400),
        "pl": q(con, f"""select table_id, any_value(labels_json), count(distinct series_key), max(time_period), arg_max(obs_value, time_period)
            from unece where area_code = '616' and obs_value is not null and {pri} < 9 group by table_id, {pri}, series_key
            order by {pri}, table_id, 2""", 150),
    })
    emit("10 unece codes", {
        "c": q(con, f"""select table_id, left(any_value(table_title), 40), any_value(dimensions_json), any_value(labels_json), count(distinct series_key)
            from unece where area_code = '616' and {pri} < 9 group by table_id, {pri},
            regexp_replace(dimensions_json, '"Country[^,}}]*', '') order by {pri}, table_id""", 170),
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
