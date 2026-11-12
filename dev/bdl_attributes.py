"""Dev trial for #2806 (deleted before the merge): read-only. The BDL API attribute dictionary and, per channel
and attribute, the Silver rows and their share of zero values."""

import json
import sys

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from ingestion.to_landing.gus_bdl.api import Bdl


def main() -> int:
    api = Bdl.shared()
    attrs = []
    for lang in ("pl", "en"):
        page = api.get("/attributes", lang=lang, **{"page-size": 100})
        attrs.append({lang: [{k: a.get(k) for k in ("id", "name", "symbol", "description")} for a in page["results"]]})
    text = json.dumps(attrs, ensure_ascii=False)
    for i in range(0, len(text), 3500):
        annotate("notice", f"dev bdl_attributes dictionary {i // 3500}", text[i : i + 3500])
    table = (
        catalog()
        .load_table(("silver", "bdl_observations"))
        .scan(selected_fields=("channel", "attribute", "value"))
        .to_arrow()
    )
    con = duckdb.connect()
    con.register("t", table)
    rows = con.sql(
        """select channel, attribute, count(*) as n, count(*) filter (where value = 0) as zeros,
                  count(*) filter (where value is null) as nulls
           from t group by all order by channel, n desc"""
    ).fetchall()
    annotate("notice", "dev bdl_attributes silver", {"rows": [list(r) for r in rows][:80], "total": table.num_rows})
    return 0


if __name__ == "__main__":
    sys.exit(main())
