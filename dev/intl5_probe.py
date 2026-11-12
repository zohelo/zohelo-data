"""Read-only probe (#2861, deleted before merge): shapes of the Silver tables of BIS, ILOSTAT, UNDP HDR, UNECE and
UNdata for their Gold build stages. Reads production tables through the catalog into local DuckDB; no writes.
The report is one JSON document, zlib-compressed and base64-encoded into at most 10 notices (p01..p10)."""

from __future__ import annotations

import base64
import json
from pathlib import Path
import re
import zlib

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
OUT: dict = {}
EU27 = ("AUT,BEL,BGR,HRV,CYP,CZE,DNK,EST,FIN,FRA,DEU,GRC,HUN,IRL,ITA,LVA,LTU,LUX,MLT,NLD,POL,PRT,ROU,SVK,SVN,"
        "ESP,SWE").split(",")
SILVER = {
    "bis": ("bis_observations", "silver/bis/bis_observations.sql", ["bis_observations"]),
    "ilo": ("ilostat_observations", "silver/ilostat/ilostat_observations.sql", ["ilostat_observations"]),
    "hdr": ("undp_hdr_indices", "silver/undp_hdr/undp_hdr_indices.sql", ["undp_hdr_values"]),
    "unece": ("unece_observations", "silver/unece/unece_observations.sql", ["unece_observations", "unece_codes"]),
    "syb": ("undata_syb_values", "silver/undata/undata_syb_values.sql", ["undata_syb_values"]),
}


def q(con, key: str, sql: str, limit: int = 500) -> None:
    try:
        rows = con.sql(sql).limit(limit).fetchall()
        OUT[key] = ["|".join("" if v is None else str(v) for v in r) for r in rows]
    except Exception as exc:  # reported, the probe goes on
        OUT[key] = {"error": f"{type(exc).__name__}: {str(exc)[:400]}"}


def load(con, cat, have: dict, name: str, table: str, model: str, bronze: list[str]) -> bool:
    """Register `name`: the production Silver table, or the Silver model rendered over production Bronze."""
    try:
        if table in have["silver"]:
            con.register(name, bf.retry(lambda: cat.load_table(("silver", table)).scan().to_arrow(), table))
            OUT.setdefault("from", {})[name] = "silver"
            return True
        if not all(b in have["bronze"] for b in bronze):
            OUT.setdefault("from", {})[name] = "missing in silver and bronze"
            return False
        for b in bronze:
            con.register(f"b_{b}", bf.retry(lambda b=b: cat.load_table(("bronze", b)).scan().to_arrow(), b))
        sql = re.sub(r"\{#.*?#\}", "", (ROOT / "warehouse/models" / model).read_text(), flags=re.S)
        sql = re.sub(r"\{\{\s*source\('[a-z_]+',\s*'([a-z_]+)'\)\s*\}\}", r"b_\1", sql)
        con.execute(f"create table {name} as {sql}")
        OUT.setdefault("from", {})[name] = "bronze, Silver model rendered locally"
        return True
    except Exception as exc:
        OUT.setdefault("from", {})[name] = f"error {type(exc).__name__}: {str(exc)[:300]}"
        return False


def bis(con) -> None:
    q(con, "bis_profile", """select entity, any_value(dataflow_id), count(*), count(distinct series_key),
        count(distinct ref_area), string_agg(distinct freq, ','), string_agg(distinct unit_measure, ','),
        string_agg(distinct coalesce(unit_mult, '-'), ','), string_agg(distinct coalesce(obs_status, '-'), ','),
        min(time_period), max(time_period), count(*) filter (where obs_value is null), any_value(dimensions_json),
        max(series_attribute_variants), string_agg(distinct cast(length(time_period) as varchar), ',')
        from bis group by 1 order by 1""")
    q(con, "bis_dim_codes", """select entity, je.key, string_agg(distinct json_extract_string(je.value, '$'), ',')
        from bis, json_each(dimensions_json) as je where je.key not in ('REF_AREA', 'BORROWERS_CTY')
        group by 1, 2 order by 1, 2""")
    q(con, "bis_pl", """select entity, series_key, unit_measure, unit_mult, left(title, 80), count(*),
        min(time_period), max(time_period), arg_max(obs_value, time_period),
        coalesce(arg_max(obs_status, time_period), '-'), count(*) filter (where obs_value is null)
        from bis where ref_area = 'PL' group by all order by 1, 2""")
    q(con, "bis_areas_not_country", """select entity, string_agg(distinct ref_area, ',' order by ref_area)
        from bis where ref_area not in (select geography_key from geo where level_name = 'country')
        group by 1 order by 1""")
    q(con, "bis_titles", """select entity, series_key, left(title, 110) from bis
        where ref_area in ('XM', 'US') and (entity <> 'tc' or series_key like '%.P.A.M.770.A')
        group by all order by 1, 2""", 80)


def ilo(con) -> None:
    eu = ", ".join(f"'{c}'" for c in EU27)
    q(con, "ilo_profile", """select entity, any_value(indicator), count(*), count(distinct ref_area),
        count(distinct source_code), string_agg(distinct sex, ','), string_agg(distinct coalesce(classif1, '-'), ','),
        count(*) filter (where classif2 is not null), min(time_period), max(time_period),
        count(*) filter (where obs_value is null), string_agg(distinct cast(length(time_period) as varchar), ','),
        string_agg(distinct freq, ',') from ilo group by 1 order by 1""")
    q(con, "ilo_status", "select coalesce(obs_status, '-'), count(*) from ilo group by 1 order by 2 desc")
    q(con, "ilo_multi_source", f"""select entity, count(*) as groups_with_several_sources,
        count(distinct ref_area), count(distinct ref_area) filter (where ref_area in ({eu})),
        string_agg(distinct ref_area, ',') filter (where ref_area in ({eu}))
        from (select entity, ref_area, sex, classif1, time_period from ilo group by all having count(*) > 1)
        group by 1 order by 1""")
    q(con, "ilo_sources_eu27", f"""select entity, ref_area, string_agg(src, ';' order by src) from (
        select entity, ref_area, source_code || ' ' || min(time_period) || '-' || max(time_period) as src
        from ilo where ref_area in ({eu}) and sex = 'SEX_T' group by entity, ref_area, source_code)
        group by 1, 2 order by 1, 2""")
    q(con, "ilo_source_kinds", """select split_part(source_code, ':', 1), count(distinct ref_area), count(*)
        from ilo group by 1 order by 3 desc""")
    q(con, "ilo_pol_latest", """select entity, sex, classif1, source_code, count(*), min(time_period),
        max(time_period), arg_max(obs_value, time_period), coalesce(arg_max(obs_status, time_period), '-')
        from ilo where ref_area = 'POL' group by all order by 1, 3, 2, 4""")
    q(con, "ilo_areas_not_iso3", """select string_agg(distinct ref_area, ',' order by ref_area) from ilo
        where ref_area not in (select iso3 from geo where level_name = 'country' and iso3 is not null)""")
    q(con, "ilo_notes_pol", """select entity, coalesce(note_indicator, '-'), coalesce(note_source, '-'),
        coalesce(note_classif, '-'), count(*) from ilo where ref_area = 'POL' and sex = 'SEX_T'
        group by all order by 5 desc""", 25)


def hdr(con) -> None:
    q(con, "hdr_aggregates", """select distinct iso3, country, coalesce(hdicode, '-'), coalesce(region, '-') from hdr
        where iso3 not in (select iso3 from geo where level_name = 'country' and iso3 is not null) order by 1""")
    q(con, "hdr_indicators", """select indicator, count(*), count(distinct iso3), min(year), max(year),
        count(*) filter (where value is null), min(value), max(value),
        max(year) filter (where iso3 = 'POL'), arg_max(value, year) filter (where iso3 = 'POL')
        from hdr group by 1 order by 1""")
    q(con, "hdr_pol", """select indicator, year, value from hdr where iso3 = 'POL' and year >= 2021
        and indicator in ('hdi', 'le', 'le_f', 'le_m', 'eys', 'mys', 'gnipc', 'gii', 'gdi', 'ihdi', 'hdi_rank')
        order by 1, 2""")
    q(con, "hdr_version", "select distinct source_version, raw_sha256 from hdr")


def unece(con) -> None:
    q(con, "unece_tables", """select table_id, left(any_value(table_title), 90), count(*), count(distinct area_code),
        count(*) filter (where area_code = '616'), any_value(dimensions_json), min(time_period), max(time_period),
        string_agg(distinct cast(length(time_period) as varchar), ','), count(*) filter (where obs_value is null),
        string_agg(distinct coalesce(obs_status, '-'), ','), count(distinct entity)
        from unece group by 1 order by 1""")
    q(con, "unece_areas", """select area_code, any_value(area_name), count(distinct table_id) from unece
        group by 1 order by 1""")
    q(con, "unece_null_area", "select table_id, count(*) from unece where area_code is null group by 1")
    q(con, "unece_codes", """select table_id, d.key, left(string_agg(distinct json_extract_string(d.value, '$')
        || '=' || left(json_extract_string(l.value, '$'), 46), ';'), 900)
        from unece, json_each(dimensions_json) as d, json_each(labels_json) as l
        where d.key = l.key and d.key not in ('Country', 'Country/Region') group by 1, 2 order by 1, 2""")
    q(con, "unece_pl", """select table_id, dimensions_json, count(*), max(time_period),
        arg_max(obs_value, time_period) from unece where area_code = '616' and (
        table_title ilike '%unemployment%' or table_title ilike '%consumer price%' or table_title ilike '%per capita%'
        or table_title ilike '%wages%' or table_title ilike '%overview%' or table_title ilike '%employment%')
        group by 1, 2 order by 1, 2""", 260)


def syb(con) -> None:
    q(con, "syb_series", """select entity, series, count(*), count(distinct area_code), min(year), max(year),
        count(*) filter (where value is null), max(year) filter (where area_code in ('616', '0616')),
        arg_max(value, year) filter (where area_code in ('616', '0616')),
        count(*) filter (where footnotes ilike '%project%'), count(*) filter (where footnotes ilike '%estimate%')
        from syb group by 1, 2 order by 1, 2""")
    q(con, "syb_tables", "select entity, any_value(table_code), left(any_value(table_title), 80) from syb group by 1 order by 1")
    q(con, "syb_area_codes", """select string_agg(distinct cast(length(area_code) as varchar), ','),
        count(distinct area_code) from syb""")
    q(con, "syb_areas_sample", """select distinct area_code, area_name from syb
        where area_name in ('Poland', 'Germany', 'World', 'Total, all countries or areas', 'European Union', 'Europe')
        or try_cast(area_code as integer) in (1, 150, 616, 97, 276) order by 1""")
    q(con, "syb_footnotes_pl", """select entity, series, left(footnotes, 110), count(*) from syb
        where area_code in ('616', '0616') and footnotes is not null group by all order by 4 desc""", 40)
    q(con, "syb_source_text", """select entity, left(source_text, 120), count(*) from syb
        where entity in ('syb_1', 'syb_230', 'syb_128', 'syb_329', 'syb_246', 'syb_314', 'syb_325', 'syb_285', 'syb_310', 'syb_200')
        group by all order by 1, 3 desc""", 40)


def emit() -> None:
    drop_order = ["syb_source_text", "syb_footnotes_pl", "ilo_notes_pol", "bis_titles", "unece_pl", "ilo_sources_eu27",
                  "unece_codes", "bis_dim_codes"]
    dropped = []
    while True:
        OUT["dropped_for_size"] = dropped
        text = base64.b64encode(zlib.compress(json.dumps(OUT, ensure_ascii=False, separators=(",", ":")).encode(), 9)).decode()
        if len(text) <= 10 * 3700 or not drop_order:
            break
        key = drop_order.pop(0)
        if key in OUT:
            OUT.pop(key)
            dropped.append(key)
    parts = [text[i:i + 3700] for i in range(0, len(text), 3700)][:10]
    for i, part in enumerate(parts, 1):
        annotate("notice", f"p{i:02d} of {len(parts)}", part)


def main() -> int:
    try:
        cat = bf.retry(catalog, "catalog")
        have = {ns: sorted(i[-1] for i in cat.list_tables(ns)) for ns in ("bronze", "silver", "gold")}
        OUT["silver_tables"] = ",".join(have["silver"])
        OUT["gold_builds"] = ",".join(t for t in have["gold"] if t.startswith(("build_", "src_", "dim_", "map_")))
        OUT["bronze_new"] = ",".join(t for t in have["bronze"] if t.startswith(("bis_", "ilostat_", "undp_", "unece_", "undata_")))
        con = duckdb.connect()
        con.execute("SET memory_limit='10GB'; SET preserve_insertion_order=false;")
        con.register("geo", cat.load_table(("gold", "dim_geography")).scan().to_arrow())
        q(con, "geo_cols", "select string_agg(column_name, ',') from (describe geo)")
        q(con, "geo_levels", "select level_name, count(*), count(iso3) from geo group by 1 order by 2 desc")
        q(con, "geo_aggregates", """select string_agg(geography_key, ',' order by geography_key) from geo
            where level_name not in ('country') and geography_key not like 'TERC:%' and geography_key not like 'NUTS:%'
            and geography_key not like 'BDL:%'""")
        q(con, "geo_nuts_ea", "select geography_key, level_name, name_en from geo where geography_key in ('NUTS:EA19', 'NUTS:EA20', 'EUROSTAT:EA', 'EUROSTAT:EU27_2020', 'WB:WLD', 'WB:EUU', 'WB:EMU', '_Z', '_X', 'XK', 'TW', 'HK')")
        for name, fn in (("bis", bis), ("ilo", ilo), ("hdr", hdr), ("unece", unece), ("syb", syb)):
            if load(con, cat, have, name, *SILVER[name]):
                fn(con)
                try:
                    con.unregister(name)
                except Exception:  # a locally rendered table stays
                    pass
    except Exception as exc:
        OUT["fatal"] = f"{type(exc).__name__}: {str(exc)[:600]}"
    emit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
