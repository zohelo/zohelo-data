"""Read-only probe (#803): why nbp_fx_rates has rows beyond one per table × date × currency."""

from __future__ import annotations

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

rows = catalog().load_table(("bronze", "nbp_observations")).scan().to_arrow()
con = duckdb.connect()
con.register("bronze", rows)
con.execute("""
create table silver as
with source_rows as (
    select * from bronze
    qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
), ranked as (
    select *, rank() over (partition by entity order by source_version desc, ingested_at desc) as version_rank
    from source_rows where table_code in ('A', 'B', 'C')
)
select * from ranked where version_rank = 1
""")
key = "table_code, effective_date, upper(currency_code)"
dup = f"select *, count(*) over (partition by {key}) as n from silver qualify n > 1"
summary = con.execute(f"""
with d as ({dup})
select
    count(*) as rows_in_groups,
    count(*) - count(distinct ({key})) as extra_rows,
    count(distinct ({key})) as groups
from d
""").fetchone()
by = con.execute(f"""
with d as ({dup}), g as (
    select {key} as k, count(distinct entity) as entities, count(distinct raw_sha256) as files,
           count(distinct currency_name) as names, count(distinct coalesce(mid, bid)) as rates,
           count(distinct table_no) as table_nos, any_value(table_code) as table_code
    from d group by all
)
select table_code, entities > 1 as several_entities, files > 1 as several_files, names > 1 as several_names,
       rates > 1 as several_rates, table_nos > 1 as several_table_nos, count(*) as groups
from g group by all order by groups desc limit 12
""").fetchall()
sample = con.execute(f"""
with d as ({dup})
select table_code, cast(effective_date as varchar), currency_code, currency_name, entity, table_no,
       cast(coalesce(mid, bid) as varchar), substr(raw_sha256, 1, 8), source_version
from d order by effective_date desc, currency_code limit 8
""").fetchall()
annotate(
    "notice",
    "nbp_fx_rates extra rows",
    {
        "silver_rows": con.execute("select count(*) from silver").fetchone()[0],
        "rows_in_groups": summary[0],
        "extra_rows": summary[1],
        "groups": summary[2],
        "by_kind": [list(r) for r in by],
        "sample": [list(map(str, r)) for r in sample],
    },
)
