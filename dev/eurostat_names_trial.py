"""Dev trial for #2365: run dim_dataset's SELECT over gold.coverage and silver.eurostat_dataflows; writes nothing."""

import json
from pathlib import Path
import re

import duckdb

from ingestion.common.r2 import catalog

cat = catalog()
con = duckdb.connect()
coverage = cat.load_table(("gold", "coverage")).scan().to_arrow()
dataflows = cat.load_table(("silver", "eurostat_dataflows")).scan().to_arrow()
con.register("coverage_src", coverage)
con.register("eurostat_dataflows", dataflows)
sql = Path("warehouse/models/gold/dim_dataset.sql").read_text()
sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
sql = sql.replace("{{ source('gold_coverage', 'coverage') }}", "coverage_src")
sql = sql.replace("{{ ref('eurostat_dataflows') }}", "eurostat_dataflows")
con.execute(f"create table result as {sql}")
rows = con.sql("select count(*) from result").fetchone()[0]
before = con.sql(
    "select count(*) from (select source, case when source = 'gus_dbw' then regexp_replace(block_id, "
    "'(_[0-9]{4})?_pl$', '') when source = 'eurostat' then upper(block_id) else block_id end "
    "from coverage_src group by all)"
).fetchone()[0]
EMPTY = "count(*) filter (where name is null or name = '')"
eu = con.sql(f"select count(*), {EMPTY} from result where source_key = 'eurostat'").fetchone()
first = con.sql(
    "select code, name from result where source_key = 'eurostat' and name <> '' order by code limit 3"
).fetchall()
other_empty = con.sql(
    f"select source_key, {EMPTY} from result where source_key <> 'eurostat' group by 1 order by 1"
).fetchall()
report = {
    "eurostat": {"rows": eu[0], "empty_name": eu[1], "first": first},
    "rows_all": rows,
    "rows_before": before,
    "other_empty": other_empty,
}
print(f"::notice title=eurostat names::{json.dumps(report, ensure_ascii=False)}")
