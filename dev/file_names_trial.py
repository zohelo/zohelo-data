"""Trial for #2512: the new dim_dataset SELECT over prod inputs, compared with gold.dim_dataset. Writes nothing."""

from __future__ import annotations

from pathlib import Path
import re

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

MODEL = Path("warehouse/models/gold/dim_dataset.sql")
FILE_LIKE = r"^[0-9]{8}T[0-9]{6}Z__[0-9]{8}T[0-9]{6}Z\.[a-z]+$"


def main() -> None:
    cat = catalog()
    con = duckdb.connect()
    con.register("coverage_t", cat.load_table("gold.coverage").scan().to_arrow())
    con.register("dataflows_t", cat.load_table("silver.eurostat_dataflows").scan().to_arrow())
    con.register("today_t", cat.load_table("gold.dim_dataset").scan().to_arrow())
    con.execute("create table source_t as select * from read_csv('warehouse/seeds/source.csv', header = true)")
    sql = re.sub(r"\{#.*?#\}", "", MODEL.read_text(encoding="utf-8"), flags=re.S)
    sql = sql.replace("{{ source('gold_coverage', 'coverage') }}", "coverage_t")
    sql = sql.replace("{{ ref('eurostat_dataflows') }}", "dataflows_t").replace("{{ ref('source') }}", "source_t")
    con.execute(f"create table new_t as {sql}")
    result = {}
    for (source,) in con.execute("select distinct source_key from new_t order by 1").fetchall():
        rows, file_like, empty = con.execute(
            "select count(*), count(*) filter (where regexp_matches(name, ?)),"
            " count(*) filter (where name is null or name = '') from new_t where source_key = ?",
            [FILE_LIKE, source],
        ).fetchone()
        today_rows, changed = con.execute(
            "select (select count(*) from today_t where source_key = $s),"
            " (select count(*) from new_t n join today_t t using (dataset_key)"
            "  where n.source_key = $s and n.name is distinct from t.name)",
            {"s": source},
        ).fetchone()
        first = con.execute(
            "select n.code, t.name, n.name from new_t n join today_t t using (dataset_key)"
            " where n.source_key = ? and n.name is distinct from t.name order by n.code limit 3",
            [source],
        ).fetchall()
        old_names = con.execute(
            "select coalesce(t.name, '<null>'), count(*) from new_t n join today_t t using (dataset_key)"
            " where n.source_key = ? and n.name is distinct from t.name group by 1 order by 2 desc limit 3",
            [source],
        ).fetchall()
        result[source] = {
            "rows": rows,
            "today_rows": today_rows,
            "file_like": file_like,
            "empty_name": empty,
            "changed": changed,
            "first_changed": [list(r) for r in first],
            "today_names_changed": [list(r) for r in old_names],
        }
    annotate("notice", "file names", result)


if __name__ == "__main__":
    main()
