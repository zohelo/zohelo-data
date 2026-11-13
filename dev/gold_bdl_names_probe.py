"""Dev probe (read-only): BDL unit names of the website export against the unit list, after the type
digit is removed, and regions against voivodeships. Reads unit columns of four subgroups; writes nothing.
Deleted before merge.

python -m dev.gold_bdl_names_probe
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
BLOCKS = ["P2179", "P2017", "P1410", "P2137", "P1873", "P3502"]


def main() -> None:
    cat = catalog()
    con = duckdb.connect()
    con.execute("SET memory_limit = '9GB'; SET preserve_insertion_order = false")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    silver = et.state(cat.load_table(obs.SOURCES[SOURCE]["silver"]), "subgroup_id")
    tmp = tempfile.mkdtemp(dir=os.environ.get("RUNNER_TEMP"))
    lookups, _ = obs.export_lookups(cat, SOURCE, Path(tmp))
    for name, path in lookups.items():
        con.execute(f"create view {name} as select * from read_parquet('{path}')")
    paths = [p for b in BLOCKS if b in silver for p in silver[b]["paths"]]
    listing = "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"
    con.execute(
        f"""create table held as
            select subgroup_id, unit_code, unit_name, count(*) as n_rows, min(year) as first_year, max(year) as last_year
            from {listing} where value is not null group by all"""
    )
    con.execute(
        """create table named as
           select h.*, u.name as id_name, u.level, g.geography_key as mapped,
               upper(trim(regexp_replace(h.unit_name, '\\s*\\([0-9]\\)\\s*$', ''))) as export_u, upper(trim(u.name)) as id_u
           from held as h inner join bdl_units as u on h.unit_code = u.unit_id
           left join geo_map as g on h.unit_code = g.source_code"""
    )
    annotate("notice", "bdl names probe: mismatches", {
        "by_level (level, units, names equal, differ)": con.execute(
            """select level, count(*), count(*) filter (where export_u = id_u), count(*) filter (where export_u <> id_u)
               from (select distinct level, unit_code, export_u, id_u from named) group by 1 order by 1"""
        ).fetchall(),
        "differ_examples (level, code, export name, id name, mapped, years)": con.execute(
            """select level, unit_code, unit_name, id_name, mapped, min(first_year), max(last_year), sum(n_rows)
               from named where export_u <> id_u group by all
               qualify row_number() over (partition by level order by unit_code) <= 22 order by 1, 2 limit 44"""
        ).fetchall(),
    })
    annotate("notice", "bdl names probe: mismatch shapes", {
        "export_suffixes": con.execute(
            """select regexp_extract(unit_name, '(\\s+(od|do)\\s+[0-9]{4}.*|\\s*\\([^)]*\\)\\s*)$', 1), count(distinct unit_code)
               from named where export_u <> id_u group by 1 order by 2 desc limit 12"""
        ).fetchall(),
        "id_suffixes": con.execute(
            """select regexp_extract(id_name, '(\\s+(od|do)\\s+[0-9]{4}.*|\\s*\\([^)]*\\)\\s*)$', 1), count(distinct unit_code)
               from named where export_u <> id_u group by 1 order by 2 desc limit 12"""
        ).fetchall(),
        "one_starts_with_the_other (level, units)": con.execute(
            """select level, count(distinct unit_code) from named
               where export_u <> id_u and (starts_with(export_u, id_u) or starts_with(id_u, export_u)) group by 1"""
        ).fetchall(),
        "codes_with_several_names_in_a_block": con.execute(
            """select subgroup_id, unit_code, list(unit_name order by unit_name)[1:3], list(first_year || '-' || last_year order by unit_name)[1:3]
               from named group by 1, 2 having count(*) > 1 order by 1, 2 limit 12"""
        ).fetchall(),
        "blocks_codes_with_several_names": con.execute(
            "select count(distinct subgroup_id), count(*) from (select subgroup_id, unit_code from named group by 1, 2 having count(*) > 1)"
        ).fetchone(),
    })
    # regions against the voivodeship of the same area: cells both have, and their values
    con.execute(
        f"""create table cells as
            select s.subgroup_id, g.geography_key as area, u.level, s.dimension_values, s.year, s.value, s.unit_code
            from {listing} as s inner join bdl_units as u on s.unit_code = u.unit_id
            inner join geo_map as g on s.unit_code = g.source_code
            where s.value is not null and u.level in (1, 2, 3) and g.geography_key like 'TERC:%'"""
    )
    annotate("notice", "bdl names probe: regions", {
        "region_cells (block, level, cells, voivodeship has the cell, same value, other value)": con.execute(
            """select r.subgroup_id, r.level, count(*), count(v.value), count(*) filter (where v.value = r.value),
                   count(*) filter (where v.value <> r.value)
               from cells as r left join cells as v
                   on v.level = 2 and r.subgroup_id = v.subgroup_id and r.area = v.area
                   and r.dimension_values = v.dimension_values and r.year = v.year
               where r.level in (1, 3) group by 1, 2 order by 1, 2"""
        ).fetchall(),
        "other_value_examples": con.execute(
            """select r.subgroup_id, r.unit_code, v.unit_code, r.dimension_values, r.year, r.value, v.value
               from cells as r inner join cells as v
                   on v.level = 2 and r.subgroup_id = v.subgroup_id and r.area = v.area
                   and r.dimension_values = v.dimension_values and r.year = v.year
               where r.level in (1, 3) and v.value <> r.value limit 8"""
        ).fetchall(),
        "voivodeship_units_per_block": con.execute(
            "select subgroup_id, count(distinct unit_code) filter (where level = 2), count(distinct unit_code) filter (where level = 3) from cells group by 1 order by 1"
        ).fetchall(),
    })


if __name__ == "__main__":
    main()
