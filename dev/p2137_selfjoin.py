"""Read-only (#2873 item 2d): the comparison of trial 43292078190 joined production gold.build_bdl to the trial's
build_bdl on (measure_key, geography_key, breakdown_json, date_key). This joins production to itself on that key:
rows of several unmatched units share geography_key '_X', so they pair crosswise. Writes nothing."""

from __future__ import annotations

import duckdb
from pyiceberg.expressions import In

from ingestion.common import r2
from ingestion.common.report import annotate


def main() -> None:
    fields = ("block_id", "measure_key", "geography_key", "geography_source_code", "breakdown_json", "date_key", "value")
    table = r2.catalog().load_table(("gold", "build_bdl"))
    con = duckdb.connect()
    con.register("raw", table.scan(row_filter=In("block_id", ["P2137", "P2361"]), selected_fields=fields).to_arrow())
    con.execute("create table p as select * from raw")
    key = "measure_key, geography_key, breakdown_json, date_key"
    summary = con.execute(
        f"""select p.block_id, count(*) as pairs, count(*) filter (where p.value <> n.value) as changed,
                count(*) filter (where p.geography_source_code <> n.geography_source_code) as pairs_of_different_units
            from p join p as n using ({key}) group by 1 order by 1"""
    ).fetchall()
    rows = con.execute("select block_id, count(*) from p group by 1 order by 1").fetchall()
    shared = con.execute(
        f"""select block_id, geography_key, date_key, breakdown_json, list(geography_source_code order by geography_source_code),
                list(value order by geography_source_code)
            from p group by block_id, {key} having count(*) > 1 order by 1, 3, 4"""
    ).fetchall()
    unmatched = con.execute(
        """select geography_source_code, count(*), min(year(date_key)), max(year(date_key))
           from p where block_id = 'P2137' and geography_key = '_X' group by 1 order by 1"""
    ).fetchall()
    annotate(
        "notice",
        "production build_bdl joined to itself on the trial's key",
        {
            "snapshot": str(table.current_snapshot().timestamp_ms),
            "rows": rows,
            "by_block [block, pairs, changed, pairs of different units]": summary,
            "keys_held_by_several_rows [block, geography, date, breakdown, unit codes, values]": shared,
            "P2137 unmatched units [code, rows, first year, last year]": unmatched,
        },
    )


if __name__ == "__main__":
    main()
