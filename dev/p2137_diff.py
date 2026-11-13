"""Read-only (#2873 item 2d): which published P2137 (population) cells changed between Silver versions of the
subgroup, and whether the Gold pick of one row per cell is decided. Reads the P2137 Silver files (current and
superseded ones still in the bucket), silver.bdl_variables, silver.bdl_units, gold.map_geography and the P2137
rows of gold.build_bdl. Writes nothing."""

from __future__ import annotations

import io
from urllib.parse import urlparse

import duckdb
import pyarrow.parquet as pq
from pyiceberg.expressions import EqualTo

from ingestion.common import r2
from ingestion.common.report import annotate

SUBGROUP = "P2137"

CELLS = """
with s as (
    select o.*, v.n1, v.n2, v.n3, v.n4, v.n5, u.name as unit_name_of_id,
        coalesce(u.level, case when o.unit_code = '0000000' then 0
            when regexp_matches(o.unit_code, '^[0-9]{2}00000$') then 2
            when regexp_matches(o.unit_code, '^[0-9]{4}000$') then 5 else 6 end) as unit_level,
        o.value = 0 and ((o.channel = 'web_export' and coalesce(lower(o.attribute), '') in ('x', 'n'))
            or (o.channel = 'api' and coalesce(o.attribute, '') in ('4', '14', '50', '91'))) as no_value,
        list_filter([cast(v.n1 as varchar), cast(v.n2 as varchar), cast(v.n3 as varchar), cast(v.n4 as varchar),
            cast(v.n5 as varchar)], lambda x: x is not null) as labels
    from __OBS__ as o
    left join units as u on o.unit_code = u.unit_id
    left join vars as v on o.variable_id = v.variable_id
    where o.value is not null and o.year >= 2010
),
kept as (
    select * from s
    where not (unit_level = 3 and upper(unit_name) not like '%WARSZAWSKI STO%'
            and upper(unit_name) not like '%MAZOWIECKI REGIONALNY%')
        and not (unit_level = 1 and upper(unit_name) like '%MAZOWIECKIE%')
        and (unit_level <= 5 or year >= 2015)
        and len(list_filter(labels, lambda x: lower(x) not in ('ogółem', 'mężczyźni', 'kobiety'))) = 0
),
mapped as (
    select k.*, case when k.unit_name_of_id is not null and upper(trim(k.unit_name_of_id)) <> upper(trim(k.unit_name))
        then '_X' else coalesce(g.geography_key, '_X') end as geography_key
    from kept as k left join geo as g on k.unit_code = g.source_code
),
ident as (
    select *, case when geography_key = '_X' then '_X:' || unit_code else geography_key end as gid,
        array_to_string(labels, ' / ') as breakdown
    from mapped
)
select *,
    row_number() over (partition by measure_unit, labels, gid, year
        order by case when channel = 'api' then 0 else 1 end, source_version desc, unit_code asc) as rn,
    count(*) over (partition by measure_unit, labels, gid, year) as candidates,
    count(distinct value) over (partition by measure_unit, labels, gid, year) as candidate_values,
    count(*) over (partition by measure_unit, labels, gid, year, channel, source_version, unit_code) as tied,
    count(distinct value) over (partition by measure_unit, labels, gid, year, channel, source_version, unit_code)
        as tied_values
from ident
"""


def rows(con, sql: str, limit: int = 12) -> list[dict]:
    cur = con.execute(f"{sql} limit {limit}")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def main() -> None:
    cat, client = r2.catalog(), r2.s3()
    con = duckdb.connect()
    silver = cat.load_table(("silver", "bdl_observations"))
    current = [t.file.file_path for t in silver.scan(row_filter=EqualTo("subgroup_id", SUBGROUP)).plan_files()]
    parsed = urlparse(current[0])
    bucket, prefix = parsed.netloc, parsed.path.lstrip("/").rsplit("/", 1)[0] + "/"
    listed = client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    files = sorted(listed, key=lambda o: o["LastModified"])
    bronze = cat.load_table(("bronze", "bdl_observations"))
    bronze_now = [t.file.file_path for t in bronze.scan(row_filter=EqualTo("subgroup_id", SUBGROUP)).plan_files()]
    bparsed = urlparse(bronze_now[0])
    bronze_listed = client.list_objects_v2(
        Bucket=bparsed.netloc, Prefix=bparsed.path.lstrip("/").rsplit("/", 1)[0] + "/"
    ).get("Contents", [])
    current_keys = {urlparse(p).path.lstrip("/") for p in current}
    annotate(
        "notice",
        "1 P2137 files",
        {
            "silver_in_table": [p.rsplit("/", 1)[-1][:28] for p in current],
            "silver_in_bucket": [
                {"file": o["Key"].rsplit("/", 1)[-1][:28], "modified": str(o["LastModified"])[:19], "mb": round(o["Size"] / 1e6, 1),
                 "current": o["Key"] in current_keys}
                for o in files
            ],
            "bronze_in_table": [p.rsplit("/", 1)[-1][:20] for p in bronze_now],
            "bronze_in_bucket": [
                {"file": o["Key"].rsplit("/", 1)[-1][:20], "modified": str(o["LastModified"])[:19], "mb": round(o["Size"] / 1e6, 1)}
                for o in sorted(bronze_listed, key=lambda o: o["LastModified"])
            ],
            "silver_snapshots": [
                [str(s.timestamp_ms), (s.summary or {}).get("zohelo.operation")] for s in silver.snapshots()[-3:]
            ],
        },
    )

    con.register("vars_all", cat.load_table(("silver", "bdl_variables")).scan(row_filter=EqualTo("subgroup_id", SUBGROUP)).to_arrow())
    con.execute("create table vars as select * from vars_all")
    con.register("units_all", cat.load_table(("silver", "bdl_units")).scan().to_arrow())
    con.execute("create table units as select unit_id, level, name from units_all")
    con.register("geo_all", cat.load_table(("gold", "map_geography")).scan(row_filter=EqualTo("source_system", "gus_bdl")).to_arrow())
    con.execute("create table geo as select source_code, geography_key from geo_all")

    versions = []
    for i, obj in enumerate(files[-3:]):
        body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
        con.register(f"raw{i}", pq.read_table(io.BytesIO(body)))
        con.execute(f"create table obs{i} as select * from raw{i}")
        con.unregister(f"raw{i}")
        con.execute(f"create table cells{i} as {CELLS.replace('__OBS__', f'obs{i}')}")
        con.execute(f"create table final{i} as select * from cells{i} where rn = 1 and not no_value")
        versions.append((i, obj))
    stats = {}
    for i, obj in versions:
        stats[obj["Key"].rsplit("/", 1)[-1][:16]] = {
            "modified": str(obj["LastModified"])[:19],
            "silver_rows": con.execute(f"select count(*) from obs{i}").fetchall()[0][0],
            "by_channel_version": rows(con, f"select channel, source_version, count(*) n from obs{i} group by all order by 1, 2", 8),
            "rows_without_variable": con.execute(f"select count(*) from obs{i} where variable_id is null").fetchall()[0][0],
            "value_conflict_rows": con.execute(f"select count(*) from obs{i} where value_conflict").fetchall()[0][0],
            "gold_cells": con.execute(f"select count(*) from final{i}").fetchall()[0][0],
            "cells_with_several_candidates": con.execute(f"select count(*) from cells{i} where rn = 1 and candidates > 1").fetchall()[0][0],
            "of_them_values_differ": con.execute(f"select count(*) from cells{i} where rn = 1 and candidate_values > 1").fetchall()[0][0],
            "pick_undecided_cells": con.execute(f"select count(*) from cells{i} where rn = 1 and tied > 1").fetchall()[0][0],
            "pick_undecided_values_differ": con.execute(f"select count(*) from cells{i} where rn = 1 and tied_values > 1").fetchall()[0][0],
        }
    annotate("notice", "2 versions", stats)

    key = "measure_unit, breakdown, gid, year"
    for (a, oa), (b, ob) in zip(versions, versions[1:], strict=False):
        join = f"final{a} as a full join final{b} as b using ({key})"
        summary = con.execute(
            f"""select count(*) filter (where a.value is not null and b.value is null) as gone,
                count(*) filter (where a.value is null and b.value is not null) as added,
                count(*) filter (where a.value is distinct from b.value and a.value is not null and b.value is not null) as changed,
                count(*) filter (where a.value = b.value) as same from {join}"""
        ).fetchall()[0]
        changed = con.execute(
            f"""select breakdown, gid, year, a.value, b.value, a.channel || '>' || b.channel,
                a.source_version || '>' || b.source_version, coalesce(a.attribute, '') || '>' || coalesce(b.attribute, ''),
                a.unit_code || '>' || b.unit_code, b.unit_name
                from {join} where a.value is distinct from b.value and a.value is not null and b.value is not null
                order by gid, year, breakdown limit 20"""
        ).fetchall()
        gone = rows(con, f"select a.breakdown, a.gid, a.year, a.value, a.channel, a.attribute from final{a} a anti join final{b} b using ({key}) order by 2, 3", 6)
        annotate(
            "notice",
            f"3 diff {str(oa['LastModified'])[5:16]} to {str(ob['LastModified'])[5:16]}",
            {"gone": summary[0], "added": summary[1], "changed": summary[2], "same": summary[3], "gone_rows": gone,
             "changed_columns": "breakdown, geography, year, old, new, channel, version, attribute, unit code, unit name",
             "changed_rows": changed},
        )

    last = versions[-1][0]
    undecided = con.execute(
        f"""select breakdown, gid, year, unit_code, unit_name, channel, source_version, attribute, value, rn
            from cells{last} where tied_values > 1 order by gid, year, breakdown, rn limit 16"""
    ).fetchall()
    annotate(
        "notice",
        "4 undecided picks in the current version",
        {
            "columns": "breakdown, geography, year, unit code, unit name, channel, version, attribute, value, rank",
            "rows": undecided,
            "no_value_winners": con.execute(f"select count(*) from cells{last} where rn = 1 and no_value").fetchall()[0][0],
        },
    )
    differ = con.execute(
        f"""select breakdown, gid, year, unit_code, channel, source_version, attribute, value, rn
            from cells{last} where candidate_values > 1 order by gid, year, breakdown, rn limit 20"""
    ).fetchall()
    by_pair = con.execute(
        f"""select count(*) from cells{last} where rn = 1 and candidate_values > 1"""
    ).fetchall()[0][0]
    annotate(
        "notice",
        "4b cells whose candidates differ in value (current version)",
        {"cells": by_pair, "columns": "breakdown, geography, year, unit code, channel, version, attribute, value, rank", "rows": differ},
    )

    con.register("gold_all", cat.load_table(("gold", "build_bdl")).scan(row_filter=EqualTo("block_id", SUBGROUP)).to_arrow())
    compare = con.execute(
        f"""with g as (select geography_key, geography_source_code, year(date_key) as year, breakdown_json, value from gold_all),
            f as (select geography_key, unit_code, year, cast(to_json(map(list_transform(generate_series(1, len(labels)), lambda i: 'n' || i), labels)) as varchar) as breakdown_json, value from final{last})
            select (select count(*) from g), (select count(*) from f),
                count(*) filter (where g.value is not null and f.value is not null and g.value = f.value),
                count(*) filter (where g.value is not null and f.value is not null and g.value <> f.value),
                count(*) filter (where f.value is null), count(*) filter (where g.value is null)
            from g full join f on g.geography_key = f.geography_key and g.geography_source_code = f.unit_code
                and g.year = f.year and g.breakdown_json = f.breakdown_json"""
    ).fetchall()[0]
    annotate(
        "notice",
        "5 production gold.build_bdl P2137 against this script",
        dict(zip(["gold_rows", "script_rows", "equal", "different", "only_gold", "only_script"], compare, strict=True)),
    )


if __name__ == "__main__":
    main()
