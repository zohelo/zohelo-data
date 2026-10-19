"""Dev probe (#852 review): conflicting NBP Silver keys in the newest Bronze version. Read-only."""

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

rows = catalog().load_table(("bronze", "nbp_observations")).scan().to_arrow()
con = duckdb.connect()
con.register("obs", rows)
con.execute(
    """
    create table cur as
    with s as (
        select * from obs
        qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
    ), r as (
        select *, rank() over (partition by entity order by source_version desc, ingested_at desc) as vr
        from s where table_code in ('A', 'B', 'C')
    )
    select * from r where vr = 1
    """
)
groups = con.execute(
    """
    select table_code, effective_date, upper(currency_code) as cc,
           count(distinct (table_no, trading_date, currency_name, mid, bid, ask)) as variants,
           count(distinct table_no) as table_nos,
           count(distinct (trading_date, currency_name, mid, bid, ask)) as value_sets,
           count(distinct (mid, bid, ask)) as rate_sets,
           count(distinct raw_sha256) as files
    from cur group by all having variants > 1
    """
).df()
summary = {
    "rows_current": con.execute("select count(*) from cur").fetchone()[0],
    "conflicting_keys": len(groups),
    "keys_by_table": groups.groupby("table_code").size().to_dict(),
    "keys_differing_only_in_table_no": int(((groups.table_nos > 1) & (groups.value_sets == 1)).sum()),
    "keys_with_different_rates": int((groups.rate_sets > 1).sum()),
    "keys_spanning_files": int((groups.files > 1).sum()),
    "years": groups.effective_date.astype(str).str[:4].value_counts().sort_index().to_dict(),
}
sample = con.execute(
    """
    select c.table_code, c.table_no, c.effective_date, c.currency_code, c.currency_name, c.mid, c.bid, c.ask,
           c.entity, left(c.raw_sha256, 12) as sha
    from cur c join groups g
      on c.table_code = g.table_code and c.effective_date = g.effective_date and upper(c.currency_code) = g.cc
    where g.rate_sets > 1 order by 1, 3, 4, 2 limit 8
    """
).fetchall()
summary["sample_different_rates"] = [[str(v) for v in r] for r in sample]
annotate("notice", "dev nbp conflicts", summary)
