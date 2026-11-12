"""Read-only: the rows tests/metrics_value_range.sql fails on prod Gold, grouped by KPI, source,
measure and unit with count and min/max value, as one compact annotation (D29). No writes."""

import json
import os
from pathlib import Path
import re

import duckdb

from ingestion.common.r2 import req
from ingestion.common.report import annotate

TEST = Path("warehouse/tests/metrics_value_range.sql").read_text()
BOUNDS = {
    (c, s): (float(lo), float(hi))
    for c, s, lo, hi in re.findall(r"\('(\w+)', '(\w+)', (-?[\d.e]+), (-?[\d.e]+)\)", TEST)
}
KPIS = re.findall(r"ref\('poc_metrics_(\w+)'\)", TEST)


def main() -> int:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    con.execute(f"CREATE SECRET r2_catalog (TYPE iceberg, TOKEN '{req('STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN')}')")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute(
        f"CREATE SECRET r2_files (TYPE s3, KEY_ID '{req('STORAGE_WRITE_ACCESS_KEY')}', "
        f"SECRET '{req('STORAGE_WRITE_SECRET_KEY')}', ENDPOINT '{endpoint}', REGION 'auto', URL_STYLE 'path')"
    )
    con.execute(
        f"ATTACH '{req('STORAGE_LAKEHOUSE_CATALOG_NAME')}' AS r2 "
        f"(TYPE iceberg, SECRET r2_catalog, ENDPOINT '{req('STORAGE_LAKEHOUSE_CATALOG_URL')}', READ_ONLY)"
    )
    union = " union all ".join(
        f"select concept_key, source_system, measure_key, unit, geography_key, date_key, value from r2.gold.metrics_{k}"
        for k in KPIS
    )
    bounds = ", ".join(f"('{c}', '{s}', {lo}, {hi})" for (c, s), (lo, hi) in BOUNDS.items())
    rows = con.execute(
        f"""
        with m as ({union}),
        b (concept_key, source_system, lo, hi) as (values {bounds})
        select m.concept_key, m.source_system, measure_key, unit, count(*) n, min(value) vmin, max(value) vmax,
               count(*) filter (where b.lo is null or value < b.lo or value > b.hi) bad,
               arg_min(geography_key || '@' || cast(date_key as varchar), value) at_min,
               arg_max(geography_key || '@' || cast(date_key as varchar), value) at_max
        from m left join b using (concept_key, source_system)
        group by all order by all
        """
    ).fetchall()
    out, total = [], 0
    for c, s, m, u, n, lo, hi, bad, at_lo, at_hi in rows:
        total += bad
        row = [c, s, m, u, n, float(f"{lo:.4g}"), float(f"{hi:.4g}"), bad]
        if bad:
            row += [at_lo, at_hi]
        out.append(row)
    failing = [r for r in out if r[7]]
    passing = [r[2] for r in out if not r[7]]
    data = {"failing_total": total, "cols": "kpi,source,measure,unit,n,min,max,fail,at_min,at_max",
            "failing": failing, "ok_measures": len(passing)}
    Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp"), "metrics_value_range.json").write_text(
        json.dumps({"all": out, "bounds": {f"{c}|{s}": v for (c, s), v in BOUNDS.items()}}, default=str, indent=1)
    )
    annotate("notice", "metrics_value_range on prod", data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
