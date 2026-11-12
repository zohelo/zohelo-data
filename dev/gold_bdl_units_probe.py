"""Dev probe (read-only): why GUS BDL units of the per-block Gold trial are unmatched or do not hold their
area. Reads the unit columns of a few subgroups' Silver files and the small lookups; writes nothing.
Deleted before merge.

python -m dev.gold_bdl_units_probe
"""

from __future__ import annotations

import os
from pathlib import Path
import tempfile

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req
from ingestion.common.report import annotate
from warehouse import observations as obs

SOURCE = "gus_bdl"
BIG = ["P2137", "P3824", "P2179", "P2017", "P1410", "P2392", "P3111"]
SAMPLE = ("P1316 P1410 P1731 P1796 P1869 P1873 P1947 P1989 P2017 P2137 P2162 P2179 P2239 P2324 P2346 P2359 P2392 "
          "P2425 P2497 P2545 P2730 P2840 P2955 P2961 P3022 P3111 P3125 P3134 P3361 P3433 P3475 P3502 P3518 P3523 "
          "P3759 P3788 P3804 P3824 P4058 P4138 P4143 P4164 P4179 P4206 P4363 P4364 P4624").split()


def connect():
    con = duckdb.connect()
    con.execute("SET memory_limit = '9GB'; SET preserve_insertion_order = false")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    return con


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def section(title: str, fn) -> None:
    try:
        annotate("notice", title, fn())
    except Exception as exc:
        annotate("warning", title, {"error": f"{type(exc).__name__}: {str(exc)[:800]}"})


def main() -> None:
    cat = catalog()
    con = connect()
    silver = et.state(cat.load_table(obs.SOURCES[SOURCE]["silver"]), "subgroup_id")
    tmp = tempfile.mkdtemp(dir=os.environ.get("RUNNER_TEMP"))
    lookups, _ = obs.export_lookups(cat, SOURCE, Path(tmp))
    for name, path in lookups.items():
        con.execute(f"create view {name} as select * from read_parquet('{path}')")
    paths = [p for b in SAMPLE if b in silver for p in silver[b]["paths"]]
    # every unit (code + name) of the sampled subgroups with its valued rows, per subgroup
    con.execute(
        f"""create table held as
            select subgroup_id, unit_code, unit_name, any_value(unit_code_scheme) as scheme, any_value(channel) as channel,
                count(*) as n_rows, min(year) as first_year, max(year) as last_year
            from {files(paths)} where value is not null group by all"""
    )
    con.execute(
        """create table described as
           select h.*, u.name as id_name, u.level as id_level, g.geography_key as mapped,
               case when length(h.unit_code) = 12
                   then substr(h.unit_code, 3, 2) || substr(h.unit_code, 8, 2) || substr(h.unit_code, 10, 2) || substr(h.unit_code, 12, 1)
               end as teryt7,
               coalesce(u.name is not null and upper(trim(u.name)) <> upper(trim(h.unit_name)), false) as borrowed
           from held as h left join bdl_units as u on h.unit_code = u.unit_id
           left join geo_map as g on h.unit_code = g.source_code"""
    )
    section("bdl units probe: lookups", lambda: {
        "geo_map_rows_and_codes": con.execute("select count(*), count(distinct source_code) from geo_map").fetchone(),
        "geo_map_by_key_kind": con.execute(
            """select length(source_code), split_part(geography_key, ':', 1), count(*) from geo_map group by 1, 2 order by 3 desc"""
        ).fetchall(),
        "bdl_units_by_level": con.execute(
            """select u.level, count(*), count(*) filter (where coalesce(g.geography_key, '_X') = '_X')
               from bdl_units as u left join geo_map as g on u.unit_id = g.source_code group by 1 order by 1"""
        ).fetchall(),
        "unit_name_samples_level_5_6": con.execute(
            "select level, unit_id, name from bdl_units where level in (5, 6) using sample 8"
        ).fetchall(),
    })
    section("bdl units probe: classes", lambda: {
        "units_by_scheme_level_class (scheme, id level, borrowed, map kind, units, rows)": con.execute(
            """select scheme, id_level, borrowed, coalesce(split_part(mapped, ':', 1), 'absent'), count(*), sum(n_rows)
               from described group by all order by 6 desc limit 40"""
        ).fetchall(),
        "blocks_by_schemes": con.execute(
            """select schemes, count(*), list(subgroup_id order by subgroup_id)[1:12] from (
                 select subgroup_id, string_agg(distinct scheme || '/' || channel, ' + ' order by scheme || '/' || channel) as schemes
                 from held group by 1) group by 1 order by 2 desc"""
        ).fetchall(),
    })
    section("bdl units probe: names", lambda: {
        "borrowed_examples (level, code, export name, id name, mapped)": con.execute(
            """select id_level, unit_code, unit_name, id_name, mapped from described where borrowed
               qualify row_number() over (partition by id_level order by unit_code) <= 5 order by 1, 2 limit 25"""
        ).fetchall(),
        "borrowed_units_by_level": con.execute(
            "select id_level, count(distinct unit_code || coalesce(unit_name, '')) from described where borrowed group by 1 order by 1"
        ).fetchall(),
    })
    section("bdl units probe: unmatched ids", lambda: {
        "examples (level, code, export name, id name, teryt7, map of teryt7)": con.execute(
            """select d.id_level, d.unit_code, d.unit_name, d.id_name, d.teryt7, t.geography_key
               from (select distinct id_level, unit_code, unit_name, id_name, teryt7 from described
                     where scheme = 'BDL' and coalesce(mapped, '_X') = '_X') as d
               left join geo_map as t on d.teryt7 = t.source_code
               qualify row_number() over (partition by d.id_level order by d.unit_code) <= 6 order by 1, 2 limit 30"""
        ).fetchall(),
        "unmatched_ids_by_level (level, units, teryt7 in map, units with no id name)": con.execute(
            """select d.id_level, count(*), count(t.geography_key), count(*) filter (where d.id_name is null)
               from (select distinct id_level, unit_code, id_name, teryt7 from described
                     where scheme = 'BDL' and coalesce(mapped, '_X') = '_X') as d
               left join geo_map as t on d.teryt7 = t.source_code group by 1 order by 1"""
        ).fetchall(),
        "derivation_check_on_matched_ids (level, units, map equals map of teryt7, differs, teryt7 absent)": con.execute(
            """select d.id_level, count(*), count(*) filter (where t.geography_key = d.mapped),
                   count(*) filter (where t.geography_key <> d.mapped), count(*) filter (where t.geography_key is null)
               from (select distinct id_level, unit_code, mapped, teryt7 from described
                     where scheme = 'BDL' and mapped like 'TERC:%' and id_level in (2, 5, 6)) as d
               left join geo_map as t on d.teryt7 = t.source_code group by 1 order by 1"""
        ).fetchall(),
        "derivation_differs_examples": con.execute(
            """select d.id_level, d.unit_code, d.unit_name, d.mapped, d.teryt7, t.geography_key
               from (select distinct id_level, unit_code, unit_name, mapped, teryt7 from described
                     where scheme = 'BDL' and mapped like 'TERC:%' and id_level in (2, 5, 6)) as d
               left join geo_map as t on d.teryt7 = t.source_code
               where t.geography_key is distinct from d.mapped limit 8"""
        ).fetchall(),
    })
    section("bdl units probe: terc codes", lambda: {
        "terc_scheme_units (code length, map kind, units, rows)": con.execute(
            """select length(unit_code), coalesce(split_part(mapped, ':', 1), 'absent'), count(*), sum(n_rows)
               from described where scheme = 'TERC' group by all order by 4 desc"""
        ).fetchall(),
        "terc_unmatched_examples": con.execute(
            """select unit_code, unit_name, count(*), sum(n_rows) from described
               where scheme = 'TERC' and coalesce(mapped, '_X') = '_X' group by 1, 2 order by 4 desc limit 12"""
        ).fetchall(),
        "areas_with_several_units_in_a_block (block, area, units)": con.execute(
            """select subgroup_id, mapped, list(unit_code || ' ' || coalesce(unit_name, '') order by unit_code)[1:4]
               from described where coalesce(mapped, '_X') <> '_X' group by 1, 2 having count(*) > 1
               order by 1, 2 limit 14"""
        ).fetchall(),
        "blocks_and_areas_with_several_units": con.execute(
            """select count(distinct subgroup_id), count(*) from (
                 select subgroup_id, mapped from described where coalesce(mapped, '_X') <> '_X' group by 1, 2 having count(*) > 1)"""
        ).fetchone(),
    })
    section("bdl units probe: per block", lambda: {
        b: con.execute(
            """select scheme, id_level, borrowed, coalesce(split_part(mapped, ':', 1), 'absent'), count(*), sum(n_rows)
               from described where subgroup_id = ? group by all order by 6 desc limit 9""", [b]
        ).fetchall()
        for b in BIG
    })


if __name__ == "__main__":
    main()
