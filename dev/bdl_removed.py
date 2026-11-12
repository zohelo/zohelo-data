"""Dev trial for #2806 review (deleted before the merge): read-only. The rows poc_build_bdl now leaves out (no-value
signs, after the year, level, block and API-over-web filters), per published KPI x geography x period. Runs
`dbt show` on an ephemeral copy of the model; nothing is written to the catalog."""

import csv
import json
from pathlib import Path
import subprocess
import sys

from ingestion.common.report import annotate

WAREHOUSE = Path("warehouse")
MODEL = WAREHOUSE / "models" / "gold" / "poc_build_bdl.sql"
COPY = WAREHOUSE / "models" / "gold" / "zz_dev_bdl_removed.sql"


def patterns() -> dict[str, list[str]]:
    """Published scope: ready gus_bdl rows of kpi_v2_map (v2) and kpi_measure rows of published KPIs (v1)."""
    seeds = WAREHOUSE / "seeds"
    out: dict[str, list[str]] = {}
    with (seeds / "kpi_v2_map.csv").open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["source_key"] == "gus_bdl" and r["status"] == "ready":
                out.setdefault(f"v2:{r['v2_key']}", []).extend(p for p in r["measure_key_pattern"].split(";") if p)
    with (seeds / "kpi.csv").open(encoding="utf-8") as f:
        published = {r["kpi_key"] for r in csv.DictReader(f) if r["status"] == "published"}
    with (seeds / "kpi_measure.csv").open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["source_system"] == "gus_bdl" and r["kpi_key"] in published:
                out.setdefault(f"v1:{r['kpi_key']}", []).append(r["measure_key_pattern"])
    return out


def main() -> int:
    text = MODEL.read_text()
    config = "{{ config(alias='build_bdl', tags=['gold_block']) }}"
    text = text.replace(config, "{{ config(materialized='ephemeral') }}")
    assert text.endswith("from with_value as k\n")
    text = text[: -len("from with_value as k\n")] + "from deduplicated as k\nwhere k.no_value\n"
    COPY.write_text(text)
    scope = patterns()
    cases = " ".join(
        f"when measure_key like '{p.replace(chr(39), chr(39) * 2)}' then '{kpi}'"
        for kpi, ps in scope.items()
        for p in ps
    )
    inline = f"""
        select kpi, geography_key, frequency, period_label, is_total, count(*) as n, max(abs(value)) as max_abs
        from (select *, case {cases} end as kpi from {{{{ ref('zz_dev_bdl_removed') }}}})
        where kpi is not null and (geography_key = 'PL' or regexp_matches(geography_key, '^TERC:[0-9]{{2}}00000$'))
        group by all order by kpi, geography_key, period_label
    """
    command = ["dbt", "show", "--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse"]
    command += ["--inline", inline, "--limit", "-1", "--log-format", "json"]
    done = subprocess.run(command, capture_output=True, text=True)
    rows = None
    for line in done.stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("info", {}).get("name") == "ShowNode":
            rows = json.loads(event["data"]["preview"])
    COPY.unlink()
    if rows is None:
        annotate("error", "dev bdl_removed failed", done.stdout[-3000:] + done.stderr[-800:])
        return 1
    total = sum(r["n"] for r in rows)
    by_kpi: dict[str, dict] = {}
    for r in rows:
        k = by_kpi.setdefault(r["kpi"], {"rows": 0, "cells": 0, "totals": 0, "geographies": set(), "periods": set()})
        k["rows"] += r["n"]
        k["cells"] += 1
        k["totals"] += r["n"] if r["is_total"] else 0
        k["geographies"].add(r["geography_key"])
        k["periods"].add(r["period_label"])
    summary = {
        kpi: {
            **{x: v[x] for x in ("rows", "cells", "totals")},
            "geographies": sorted(v["geographies"])[:20],
            "periods": sorted(v["periods"]),
        }
        for kpi, v in by_kpi.items()
    }
    annotate(
        "notice",
        "dev bdl_removed summary",
        {
            "removed_rows_in_scope": total,
            "max_abs_value": max([r["max_abs"] or 0 for r in rows] or [0]),
            "kpis_in_scope": len(scope),
            "by_kpi": summary,
        },
    )
    detail = [[r["kpi"], r["geography_key"], r["period_label"], r["n"]] for r in rows if r["is_total"]]
    text = json.dumps(detail, ensure_ascii=False)
    for i in range(0, min(len(text), 7 * 3500), 3500):
        annotate("notice", f"dev bdl_removed totals {i // 3500}", text[i : i + 3500])
    return 0


if __name__ == "__main__":
    sys.exit(main())
