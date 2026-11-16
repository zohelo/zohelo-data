"""Read-only check for queue item 2d (#2873): the 6 "changed" P2137 values of the #2781 trial (run 43292078190).

That trial joined its build_bdl to production build_bdl on (measure_key, geography_key, breakdown_json, date_key).
Rows with an unmatched geography ('_X') of different BDL units share that key, so the join pairs them crosswise.
This script repeats the trial's join of production gold.build_bdl (P2137) with itself: a non-zero `changed` there is
an artefact of the join, not a changed value. It also lists the keys with more than one row and their units.
Writes nothing."""

from __future__ import annotations

import duckdb
from pyiceberg.expressions import EqualTo

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

KEY = "measure_key, geography_key, breakdown_json, date_key"


def main() -> int:
    cat = bf.retry(catalog, "catalog")
    table = cat.load_table(("gold", "build_bdl"))
    snap = table.current_snapshot()
    fields = (
        "block_id",
        "measure_key",
        "geography_key",
        "geography_source_code",
        "breakdown_json",
        "date_key",
        "value",
        "observation_status_key",
    )
    rows = table.scan(row_filter=EqualTo("block_id", "P2137"), selected_fields=fields).to_arrow()
    con = duckdb.connect()
    con.register("p", rows)
    n = con.sql("select count(*), count(*) filter (where geography_key = '_X') from p").fetchone()
    self_join = con.sql(
        "select count(*) filter (where b.value is null) gone, count(*) filter (where a.value is null) added, "
        "count(*) filter (where a.value <> b.value) changed, "
        "count(*) filter (where a.observation_status_key <> b.observation_status_key) status_changed "
        f"from p as a full join p as b using ({KEY})"
    ).fetchone()
    dups = con.sql(
        f"select {KEY}, count(*) n, list(geography_source_code order by geography_source_code) units, "
        "list(value order by geography_source_code) vals, count(distinct value) distinct_values "
        f"from p group by all having count(*) > 1 order by date_key"
    ).fetchall()
    annotate(
        "notice",
        "1 production build_bdl P2137 joined with itself (the trial's join)",
        {
            "snapshot_ms": snap.timestamp_ms if snap else None,
            "rows": n[0],
            "rows_unmatched_geography": n[1],
            "gone_added_changed_status": list(self_join),
            "keys_with_several_rows": len(dups),
        },
    )
    sample = [[r[0][-60:], r[1], r[3].strftime("%Y") if r[3] else None, r[4], r[5], r[6], r[7]] for r in dups[:20]]
    annotate(
        "notice",
        "2 P2137 keys with several rows (measure tail, geography, year, rows, units, values, distinct)",
        sample,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
