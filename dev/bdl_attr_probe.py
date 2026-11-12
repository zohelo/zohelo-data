"""Read-only: BDL attribute values in Silver for the activated subgroups (counts, zero values). Deleted before merge."""

import re

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

SRC = open("warehouse/macros/poc_blocks.sql").read()
SUBS = sorted(set(re.findall(r"\('(P\d+)', '", SRC)) | {"P2692", "P2837", "P2656", "P2713", "P2721", "P4610", "P3788",
                                                         "P3794"})


def main():
    from pyiceberg.expressions import In

    t = catalog().load_table(("silver", "bdl_observations")).scan(
        row_filter=In("subgroup_id", SUBS),
        selected_fields=("subgroup_id", "channel", "attribute", "value", "value_text", "year", "unit_code")).to_arrow()
    con = duckdb.connect()
    con.register("o", t)
    a = con.sql("select channel, attribute, count(*) n, count(*) filter (where value = 0) zeros, "
                "count(*) filter (where value is null) nulls, string_agg(distinct subgroup_id, ' ') subs "
                "from o group by all order by n desc").fetchall()
    annotate("notice", "bdl attributes", [[str(x)[:120] for x in r] for r in a[:30]])
    z = con.sql("select subgroup_id, channel, coalesce(attribute,'-') attr, count(*) zeros from o where value = 0 "
                "and year >= 2010 group by all order by 4 desc").fetchall()
    annotate("notice", "bdl zero values by subgroup", [list(r) for r in z[:40]])


if __name__ == "__main__":
    main()
