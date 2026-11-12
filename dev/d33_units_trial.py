"""Dev trial for D33 units (#2829), deleted before merge. Read-only against production: R2 Data Catalog attached
READ_ONLY, published files only read; nothing is written outside the job's DEV_OUTPUT_DIR (uploaded as artifact).

The branch's compiled metrics SQL (poc_concept_metrics with the unit normalisation) runs in memory against copies of
prod gold facts and dimensions, with the branch seeds (unit, unit_map, kpi, kpi_measure) in place of gold's. Reports:
every KPI measure's source unit code → unit_map unit → target unit and ratio; the base-mismatch and unmapped tests;
per KPI × measure the rows whose value or unit changes against prod gold.metrics_<kpi>; the v1 latest.json and
series.parquet of status=published KPIs from main's seeds and prod metrics (checked against the files in production)
against the branch's; PRG hectares; BDL P1410 against PRG and WDI."""

from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import traceback

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from warehouse import publish

ROOT = Path(__file__).resolve().parents[1]
W = ROOT / "warehouse"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
SEEDS = ("unit", "unit_map", "kpi", "kpi_measure")
GOLD_COPIES = (
    "fact_annual",
    "fact_quarterly",
    "fact_monthly",
    "fact_daily",
    "dim_series",
    "dim_measure",
    "dim_geography",
    "dim_calendar",
)


def say(title: str, data) -> None:
    text = json.dumps(data, ensure_ascii=False, default=str)
    for i in range(0, max(len(text), 1), 3500):
        annotate("notice", f"{title} [{i // 3500 + 1}]", text[i : i + 3500])
    (OUT / "log.txt").open("a", encoding="utf-8").write(f"## {title}\n{text}\n")


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'; SET preserve_insertion_order=false;")
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
    return con


def compile_branch() -> Path:
    env = {**os.environ, "DBT_PROFILES_DIR": str(W)}
    run = subprocess.run(
        [
            "dbt",
            "compile",
            "--target",
            "prod",
            "--select",
            "path:models/gold/poc_metrics_*.sql",
            "poc_concept_measures",
            "poc_build_prg",
            "metrics_unit_base",
            "metrics_unit_unmapped",
            "--threads",
            "4",
        ],
        cwd=W,
        env=env,
        capture_output=True,
        text=True,
    )
    say("compile", {"exit": run.returncode, "tail": run.stdout.splitlines()[-6:]})
    if run.returncode:
        raise SystemExit(1)
    return W / "target" / "compiled" / "zohelo"


def local(sql: str) -> str:
    """Compiled SQL with the branch seeds and the in-memory gold copies in place of r2.gold."""
    for name in (*SEEDS, *GOLD_COPIES, "concept_measures"):
        sql = sql.replace(f'"r2"."gold"."{name}"', f"d33.{name}")
    return sql


def arrow(cur) -> pa.Table:
    """The result as a pyarrow Table (duckdb versions differ: Table or RecordBatchReader)."""
    got = cur.arrow()
    return got.read_all() if hasattr(got, "read_all") else got


def rows(con, sql: str, limit: int | None = None) -> list[list]:
    cur = con.execute(sql)
    got = cur.fetchmany(limit) if limit else cur.fetchall()
    return [[v if isinstance(v, (int, float)) or v is None else str(v) for v in r] for r in got]


def setup(con, compiled: Path) -> list[str]:
    con.execute("CREATE SCHEMA d33")
    for s in SEEDS:
        con.execute(
            f"CREATE TABLE d33.{s} AS SELECT * FROM read_csv('{W}/seeds/{s}.csv', header=true, all_varchar=true)"
        )
    con.execute(
        "CREATE OR REPLACE TABLE d33.unit AS SELECT * REPLACE (CAST(factor AS DECIMAL(38,9)) AS factor) FROM d33.unit"
    )
    con.execute(
        "CREATE OR REPLACE TABLE d33.kpi_measure AS SELECT * REPLACE (CAST(priority AS INTEGER) AS priority) FROM d33.kpi_measure"
    )
    started = time.monotonic()
    counts = {}
    for t in GOLD_COPIES:
        con.execute(f"CREATE TABLE d33.{t} AS SELECT * FROM r2.gold.{t}")
        counts[t] = con.execute(f"SELECT count(*) FROM d33.{t}").fetchone()[0]
    sql = local((compiled / "models/gold/poc_concept_measures.sql").read_text())
    con.execute(f"CREATE TABLE d33.concept_measures AS {sql}")
    counts["concept_measures"] = con.execute("SELECT count(*) FROM d33.concept_measures").fetchone()[0]
    say("gold copies", {"rows": counts, "s": round(time.monotonic() - started)})
    have = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'r2' AND table_schema = 'gold'"
        ).fetchall()
    }
    return sorted(
        k["kpi_key"]
        for k in csv.DictReader((W / "seeds/kpi.csv").open(encoding="utf-8"))
        if f"metrics_{k['kpi_key']}" in have
    )


def units_report(con, compiled: Path) -> None:
    measure_units = (compiled / "tests/metrics_unit_base.sql").read_text()
    inner = re.search(r"from \((.*)\)\s*where", measure_units, re.S).group(1)
    con.execute(f"CREATE TABLE d33.measure_units AS {local(inner)}")
    # unit names as the build stages publish them, for the measures of a KPI
    names = []
    for b in (
        "eurostat",
        "bdl",
        "dbw",
        "wdi",
        "nbp",
        "unctad",
        "public_finance",
        "oecd",
        "gleif",
        "mf_biala_lista",
        "prg",
    ):
        names.append(
            f"SELECT DISTINCT measure_key, unit_code, unit_name FROM r2.gold.build_{b} WHERE measure_key IN (SELECT measure_key FROM d33.measure_units)"
        )
    con.execute("CREATE TABLE d33.unit_names AS " + " UNION ALL ".join(names))
    report = rows(
        con,
        """
        SELECT u.concept_key, u.source_system, u.measure_key, u.unit_code, any_value(n.unit_name) AS unit_name,
            u.source_unit_key, u.target_unit_key, u.ratio
        FROM d33.measure_units u LEFT JOIN d33.unit_names n USING (measure_key, unit_code)
        GROUP BY ALL ORDER BY ALL""",
    )
    with (OUT / "measure_units.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "concept_key",
                "source_system",
                "measure_key",
                "unit_code",
                "unit_name",
                "source_unit_key",
                "target_unit_key",
                "ratio",
            ]
        )
        w.writerows(report)
    say("measure units: ratio <> 1", [r for r in report if r[7] not in (None, 1.0)])
    say("measure units: unmapped", [r for r in report if r[5] is None])
    say(
        "measure units: counts",
        {
            "measures": len(report),
            "mapped": sum(r[5] is not None for r in report),
            "converted": sum(r[7] not in (None, 1.0) for r in report),
        },
    )
    for test in ("metrics_unit_base", "metrics_unit_unmapped"):
        sql = local((compiled / f"tests/{test}.sql").read_text())
        got = rows(con, sql)
        say(f"test {test}", {"rows": len(got), "all": got})
    say(
        "source codes of KPI measures",
        rows(
            con,
            """
        SELECT source_system, unit_code, any_value(n.unit_name), count(DISTINCT measure_key), list(DISTINCT source_unit_key)
        FROM d33.measure_units LEFT JOIN d33.unit_names n USING (measure_key, unit_code) GROUP BY ALL ORDER BY ALL""",
        ),
    )


def metrics_diff(con, compiled: Path, kpis: list[str]) -> dict[str, pa.Table]:
    new_tables: dict[str, pa.Table] = {}
    summary, changed_all = [], []
    keep = (
        "concept_key, source_system, measure_key, series_key, geography_key, geography_level, date_key, frequency, "
        "value, unit, observation_status_key"
    )
    dumps_old, dumps_new = [], []
    for kpi in kpis:
        sql = local((compiled / f"models/gold/poc_metrics_{kpi}.sql").read_text())
        try:
            con.execute(f"CREATE OR REPLACE TABLE d33.new AS {sql}")
            con.execute(f"CREATE OR REPLACE TABLE d33.old AS SELECT * FROM r2.gold.metrics_{kpi}")
        except Exception as exc:
            summary.append([kpi, "error", str(exc)[:300]])
            continue
        new_tables[kpi] = arrow(con.execute("SELECT * FROM d33.new"))
        dumps_old.append(arrow(con.execute(f"SELECT {keep} FROM d33.old")))
        dumps_new.append(arrow(con.execute(f"SELECT {keep}, value_published, unit_published FROM d33.new")))
        per = rows(
            con,
            """
            WITH j AS (
                SELECT coalesce(n.source_system, o.source_system) AS source_system,
                    coalesce(n.measure_key, o.measure_key) AS measure_key,
                    o.value AS old_value, n.value AS new_value, o.unit AS old_unit, n.unit AS new_unit,
                    o.series_key IS NULL AS only_new, n.series_key IS NULL AS only_old,
                    coalesce(n.geography_key, o.geography_key) AS geography_key, coalesce(n.date_key, o.date_key) AS date_key
                FROM d33.old o FULL JOIN d33.new n USING (series_key, date_key)
            )
            SELECT source_system, measure_key, count(*) AS n,
                count(*) FILTER (WHERE only_new) AS only_new, count(*) FILTER (WHERE only_old) AS only_old,
                count(*) FILTER (WHERE old_value IS DISTINCT FROM new_value AND NOT only_new AND NOT only_old) AS value_changed,
                any_value(old_unit) AS old_unit, any_value(new_unit) AS new_unit,
                arg_max([geography_key, cast(date_key AS varchar), cast(old_value AS varchar), cast(new_value AS varchar)],
                        (geography_key = 'PL')::int * 100000 + year(date_key)) AS sample_pl_latest,
                max(abs(new_value / nullif(old_value, 0) - 1)) FILTER (WHERE NOT only_new AND NOT only_old) AS max_rel_change
            FROM j GROUP BY ALL ORDER BY ALL""",
        )
        for r in per:
            summary.append([kpi, *r])
        changed = arrow(
            con.execute("""
            SELECT o.concept_key, o.source_system, o.measure_key, o.geography_key, o.date_key, o.value AS old_value,
                n.value AS new_value, o.unit AS old_unit, n.unit AS new_unit
            FROM d33.old o JOIN d33.new n USING (series_key, date_key)
            WHERE o.value IS DISTINCT FROM n.value""")
        )
        changed_all.append(changed)
    pq.write_table(
        pa.concat_tables(dumps_old, promote_options="permissive"), OUT / "metrics_old.parquet", compression="zstd"
    )
    pq.write_table(
        pa.concat_tables(dumps_new, promote_options="permissive"), OUT / "metrics_new.parquet", compression="zstd"
    )
    if changed_all:
        pq.write_table(
            pa.concat_tables(changed_all, promote_options="permissive"),
            OUT / "changed_rows.parquet",
            compression="zstd",
        )
    with (OUT / "metrics_diff.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "kpi",
                "source_system",
                "measure_key",
                "n",
                "only_new",
                "only_old",
                "value_changed",
                "old_unit",
                "new_unit",
                "sample_pl_latest",
                "max_rel_change",
            ]
        )
        w.writerows(summary)
    say(
        "metrics diff: measures with changed values or rows", [r for r in summary if len(r) < 4 or r[4] or r[5] or r[6]]
    )
    say(
        "metrics diff: totals",
        {
            "kpis": len(new_tables),
            "measures": len(summary),
            "rows": sum(r[3] for r in summary if len(r) > 3),
            "rows_value_changed": sum(r[6] for r in summary if len(r) > 6),
        },
    )
    return new_tables


def strip(obj):
    if isinstance(obj, dict):
        return {k: strip(v) for k, v in obj.items() if k not in ("generated_at", "checked_at")}
    if isinstance(obj, list):
        return [strip(v) for v in obj]
    return obj


def v1_compare(con, new_tables: dict[str, pa.Table]) -> None:
    client = s3()
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    main_measures = list(csv.DictReader((ROOT / "dev/kpi_measure_main.csv").open(encoding="utf-8")))
    main_kpis = {k["kpi_key"]: k for k in csv.DictReader((ROOT / "dev/kpi_main.csv").open(encoding="utf-8"))}
    branch_measures = publish.seed("kpi_measure")
    branch_kpis = {k["kpi_key"]: k for k in publish.seed("kpi")}
    out = {}
    for kpi_key, kpi in branch_kpis.items():
        if kpi["status"] != "published" and main_kpis.get(kpi_key, {}).get("status") != "published":
            continue
        old_metrics = arrow(con.execute(f"SELECT * FROM r2.gold.metrics_{kpi_key}"))
        new_metrics = new_tables[kpi_key]
        res: dict = {}
        old_series = publish.kpi_series(old_metrics, main_measures, kpi_key, main_kpis[kpi_key]["unit_key"])
        new_series = publish.kpi_series(new_metrics, branch_measures, kpi_key, kpi["unit_key"])
        try:
            prod = pq.read_table(
                io.BytesIO(
                    client.get_object(Bucket=bucket, Key=f"{publish.PREFIX}kpi/{kpi_key}/series.parquet")["Body"].read()
                )
            )
            order = [(c, "ascending") for c in ("source_key", "measure_key", "geography_key", "period_start")]
            res["series_main_equals_production"] = prod.num_rows == old_series.num_rows and prod.select(
                ["source_key", "measure_key", "geography_key", "period", "value", "unit_key"]
            ).sort_by(order).equals(
                old_series.select(
                    ["source_key", "measure_key", "geography_key", "period", "value", "unit_key"]
                ).sort_by(order)
            )
        except Exception as exc:
            res["series_main_equals_production"] = f"error: {str(exc)[:200]}"
        con.register("os", old_series)
        con.register("ns", new_series)
        res["series_rows"] = [old_series.num_rows, new_series.num_rows]
        res["series_changed"] = rows(
            con,
            """
            SELECT coalesce(o.source_key, n.source_key) src, coalesce(o.measure_key, n.measure_key) m,
                count(*) FILTER (WHERE o.value IS DISTINCT FROM n.value OR o.unit_key IS DISTINCT FROM n.unit_key) changed,
                count(*) FILTER (WHERE o.source_key IS NULL OR n.source_key IS NULL) unmatched,
                any_value(o.unit_key) ou, any_value(n.unit_key) nu,
                arg_max([o.geography_key, o.period, cast(o.value AS varchar), cast(n.value AS varchar)], o.period_start)
                    FILTER (WHERE o.geography_key = 'PL') sample_pl
            FROM os o FULL JOIN ns n USING (source_key, measure_key, geography_key, period, frequency)
            GROUP BY ALL HAVING changed > 0 OR unmatched > 0 ORDER BY ALL""",
        )
        con.unregister("os")
        con.unregister("ns")
        if kpi_key not in publish.SKIP:
            old_latest = publish.kpi_latest(
                old_metrics, main_measures, {}, kpi_key, main_kpis[kpi_key]["unit_key"], "x"
            )
            new_latest = publish.kpi_latest(new_metrics, branch_measures, {}, kpi_key, kpi["unit_key"], "x")
            try:
                prod_latest = json.loads(
                    client.get_object(Bucket=bucket, Key=f"{publish.PREFIX}kpi/{kpi_key}/latest.json")["Body"].read()
                )
                res["latest_main_equals_production"] = strip(prod_latest) == strip(old_latest)
            except Exception as exc:
                res["latest_main_equals_production"] = f"error: {str(exc)[:200]}"
            old_v = {(v["source_key"], v["geography_key"]): v for v in old_latest["values"]}
            new_v = {(v["source_key"], v["geography_key"]): v for v in new_latest["values"]}
            diffs = []
            for key in sorted(set(old_v) | set(new_v)):
                a, b = strip(old_v.get(key)), strip(new_v.get(key))
                if a != b:
                    diffs.append({"key": key, "before": a, "after": b})
            res["latest_values"] = [len(old_v), len(new_v)]
            res["latest_changed"] = len(diffs)
            res["latest_changed_by_source"] = {}
            for d in diffs:
                res["latest_changed_by_source"][d["key"][0]] = res["latest_changed_by_source"].get(d["key"][0], 0) + 1
            res["latest_changed_first"] = diffs[:6]
            (OUT / f"v1_latest_diff_{kpi_key}.json").write_text(json.dumps(diffs, ensure_ascii=False, indent=1))
        out[kpi_key] = res
        say(f"v1 {kpi_key}", res)


def prg_and_area(con, compiled: Path) -> None:
    sql = (compiled / "models/gold/poc_build_prg.sql").read_text().replace('"r2"."silver"', "r2.silver")
    con.execute(f"CREATE TABLE d33.build_prg_new AS {sql}")
    say(
        "prg: new build vs prod",
        rows(
            con,
            """
        SELECT count(*) n, count(o.geography_key) AS "matched", any_value(n.measure_key) new_measure, any_value(o.measure_key) old_measure,
            any_value(n.unit_code) new_unit, any_value(o.unit_code) old_unit,
            count(*) FILTER (WHERE abs(n.value / 100 - o.value) > 1e-9 * greatest(abs(o.value), 1)) differ_beyond_float,
            max(abs(n.value / 100 - o.value)) max_abs_diff,
            arg_max([n.geography_key, cast(n.value AS varchar), cast(o.value AS varchar)], n.geography_key = 'TERC:0200000') AS sample
        FROM d33.build_prg_new n LEFT JOIN r2.gold.build_prg o USING (geography_key, date_key)""",
        ),
    )
    # what metrics_area would show for PRG: the prod km² value against the rescaled hectares
    say(
        "prg: metrics_area km² from hectares vs prod",
        rows(
            con,
            """
        WITH r AS (SELECT geography_key, date_key, value AS ha, """
            + _rescale("value", "0.01")
            + """ AS km2 FROM d33.build_prg_new)
        SELECT count(*) n, count(m.value) AS "matched", count(*) FILTER (WHERE r.km2 = m.value) AS exact,
            max(abs(r.km2 - m.value)) max_abs_diff,
            arg_max([r.geography_key, cast(r.ha AS varchar), cast(r.km2 AS varchar), cast(m.value AS varchar)], abs(r.km2 - m.value)) AS worst
        FROM r LEFT JOIN r2.gold.metrics_area m ON m.source_system = 'gugik_prg' AND m.geography_key = r.geography_key AND m.date_key = r.date_key""",
        ),
    )
    say(
        "area: gus_bdl P1410 against PRG and WDI (prod metrics_area, newest period)",
        rows(
            con,
            """
        WITH m AS (SELECT * FROM r2.gold.metrics_area WHERE geography_key IN ('PL', 'TERC:0200000', 'TERC:1400000', 'TERC:1200000'))
        SELECT source_system, measure_key, unit, geography_key, max(date_key) AS d, arg_max(value, date_key) AS v
        FROM m GROUP BY ALL ORDER BY geography_key, source_system""",
        ),
    )
    say(
        "area: gus_bdl P1410 unit as published (build_bdl)",
        rows(
            con,
            """
        SELECT measure_key, unit_code, unit_name, count(*), min(value), max(value),
            arg_max(value, date_key) FILTER (WHERE geography_key = 'PL') pl_latest, max(date_key)
        FROM r2.gold.build_bdl WHERE block_id = 'P1410' GROUP BY ALL""",
        ),
    )
    say(
        "area: new metrics (branch) PL and Mazowieckie",
        rows(
            con,
            """
        SELECT source_system, measure_key, geography_key, date_key, value_published, unit_published, value, unit
        FROM d33.new_area WHERE geography_key IN ('PL', 'TERC:1400000') QUALIFY row_number() OVER (PARTITION BY source_system, geography_key ORDER BY date_key DESC) = 1
        ORDER BY ALL""",
        ),
    )


def _rescale(value: str, ratio: str) -> str:
    text = (W / "macros/poc_metrics.sql").read_text()
    body = text[
        text.index("{% macro poc_rescale(value, ratio) %}")
        + len("{% macro poc_rescale(value, ratio) %}") : text.rindex("{% endmacro %}")
    ]
    return body.replace("{{ value }}", value).replace("{{ ratio }}", ratio)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    compiled = compile_branch()
    con = connect()
    status = 0
    try:
        kpis = setup(con, compiled)
        units_report(con, compiled)
        new_tables = metrics_diff(con, compiled, kpis)
        if "area" in new_tables:
            con.register("new_area_arrow", new_tables["area"])
            con.execute("CREATE TABLE d33.new_area AS SELECT * FROM new_area_arrow")
        for step in (lambda: v1_compare(con, new_tables), lambda: prg_and_area(con, compiled)):
            try:
                step()
            except Exception:
                say("step failed", traceback.format_exc()[-3000:])
                status = 1
    except Exception:
        say("trial failed", traceback.format_exc()[-3000:])
        status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
