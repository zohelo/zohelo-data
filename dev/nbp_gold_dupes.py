"""Read-only probe: why bronze.nbp_observations GOLD rows give duplicate effective dates."""

from collections import Counter

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

table = catalog().load_table(("bronze", "nbp_observations"))
gold = table.scan(row_filter="table_code == 'GOLD'").to_arrow()
con = duckdb.connect()
con.register("g", gold)
q = lambda s: [dict(zip([d[0] for d in con.description], r)) for r in con.execute(s).fetchall()]  # noqa: E731
annotate("notice", "gold rows", q("select count(*) n, count(distinct entity) entities, count(distinct raw_sha256) files, count(distinct effective_date) dates from g"))
annotate("notice", "silver logic: rank-1 rows per date", q("""
with r as (select *, rank() over (partition by entity order by source_version desc, ingested_at desc) k from g)
select n, count(*) dates from (select effective_date, count(*) n from r where k = 1 group by 1) group by 1 order by 1"""))
annotate("notice", "rank-1 duplicates: same file twice?", q("""
with r as (select *, rank() over (partition by entity order by source_version desc, ingested_at desc) k from g),
d as (select effective_date from r where k = 1 group by 1 having count(*) > 1)
select count(*) rows, count(distinct raw_sha256) files, count(distinct entity) entities,
       count(distinct (raw_sha256, source_row_number)) distinct_file_rows
from r where k = 1 and effective_date in (select effective_date from d)"""))
annotate("notice", "entities sample", q("""
select entity, count(*) rows, count(distinct raw_sha256) files, min(effective_date) lo, max(effective_date) hi,
       min(ingested_at) first_in, max(ingested_at) last_in
from g group by 1 order by rows desc limit 12"""))
snap = Counter(s.summary.operation.value for s in table.snapshots())
annotate("notice", "snapshots", {"count": len(table.snapshots()), "ops": dict(snap),
         "last": [(s.timestamp_ms, s.summary.additional_properties) for s in table.snapshots()[-4:]]})
