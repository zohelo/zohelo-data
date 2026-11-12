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
    text = text[: -len("from with_value as k\n")] + "from deduplicated as k\n"
    text = text.replace(
        "\n    k.source_version,\n    k.raw_sha256\n", "\n    k.source_version,\n    k.raw_sha256,\n    k.no_value\n"
    )
    assert "k.no_value\n" in text
    COPY.write_text(text)
    scope = patterns()
    cases = " ".join(
        f"when measure_key like '{p.replace(chr(39), chr(39) * 2)}' then '{kpi}'"
        for kpi, ps in scope.items()
        for p in ps
    )
    inline = f"""
        with t as (
            select *, case {cases} end as kpi,
                geography_key = 'PL' or regexp_matches(geography_key, '^TERC:[0-9]{{2}}$') as pl_voiv
            from {{{{ ref('zz_dev_bdl_removed') }}}}
        )
        select coalesce(kpi, '-') as kpi, geography_key, frequency, period_label, is_total, no_value, pl_voiv,
            count(*) as n, max(abs(value)) as max_abs
        from t
        where no_value or (kpi is not null and pl_voiv)
        group by all order by kpi, geography_key, period_label
    """
    command = ["dbt", "show", "--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse"]
    command += ["--inline", inline, "--limit", "-1", "--output", "json", "--log-format", "json"]
    done = subprocess.run(command, capture_output=True, text=True)
    rows = None
    for line in done.stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("info", {}).get("name") == "ShowNode":
            preview = event["data"]["preview"]
            rows = json.loads(preview) if isinstance(preview, str) else preview
            rows = rows.get("show", rows) if isinstance(rows, dict) else rows
    COPY.unlink()
    if rows is None:
        annotate("error", "dev bdl_removed failed", done.stdout[-3000:] + done.stderr[-800:])
        return 1
    removed = [r for r in rows if r["no_value"]]
    in_scope = [r for r in removed if r["kpi"] != "-" and r["pl_voiv"]]
    kept = [r for r in rows if not r["no_value"]]
    control = {}
    for r in kept:
        control[r["kpi"]] = control.get(r["kpi"], 0) + r["n"]
    by_kpi: dict[str, dict] = {}
    for r in in_scope:
        k = by_kpi.setdefault(r["kpi"], {"rows": 0, "totals": 0, "geographies": set(), "periods": set()})
        k["rows"] += r["n"]
        k["totals"] += r["n"] if r["is_total"] else 0
        k["geographies"].add(r["geography_key"])
        k["periods"].add(r["period_label"])
    levels: dict[str, int] = {}
    for r in removed:
        key = ("in_kpi" if r["kpi"] != "-" else "no_kpi") + (":pl_voiv" if r["pl_voiv"] else ":other_geo")
        levels[key] = levels.get(key, 0) + r["n"]
    summary = {
        "removed_rows_all": sum(r["n"] for r in removed),
        "removed_by_scope": levels,
        "removed_in_kpi_other_geo": sorted(
            {
                (r["kpi"], r["geography_key"][:9], r["frequency"]): 0
                for r in removed
                if r["kpi"] != "-" and not r["pl_voiv"]
            }
        )[:60],
        "removed_in_kpi_other_geo_by_kpi": {
            kpi: sum(r["n"] for r in removed if r["kpi"] == kpi and not r["pl_voiv"])
            for kpi in sorted({r["kpi"] for r in removed if r["kpi"] != "-" and not r["pl_voiv"]})
        },
        "removed_max_abs_value": max([r["max_abs"] or 0 for r in removed] or [0]),
        "kept_rows_in_scope_control": control,
        "removed_in_scope_by_kpi": {
            kpi: {
                "rows": v["rows"],
                "totals": v["totals"],
                "geographies": sorted(v["geographies"])[:20],
                "periods": sorted(v["periods"]),
            }
            for kpi, v in by_kpi.items()
        },
    }
    annotate("notice", "dev bdl_removed summary", summary)
    detail = [[r["kpi"], r["geography_key"], r["period_label"], r["n"]] for r in in_scope if r["is_total"]]
    text = json.dumps(detail, ensure_ascii=False)
    for i in range(0, min(len(text), 6 * 3500), 3500):
        annotate("notice", f"dev bdl_removed totals {i // 3500}", text[i : i + 3500])
    return 0


if __name__ == "__main__":
    sys.exit(main())
