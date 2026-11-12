"""Dev trial for D33 units (#2829), deleted before merge. Read-only against production: the R2 Data Catalog is
attached READ_ONLY and published files are only read; nothing is written to R2.

The branch's compiled metrics SQL (poc_concept_metrics with the unit normalisation) runs in memory against copies of
prod gold facts and dimensions, with the branch seeds (unit, unit_map, kpi, kpi_measure) in place of gold's.
Reports (compact annotations, at most 10 notices): unit mapping and the two new tests; per KPI × measure the rows
whose value or unit changes against prod gold.metrics_<kpi>; v1 latest.json and series.parquet of the published KPIs
from main's seeds and prod metrics (checked against the files in production) against the branch's; PRG hectares;
GUS BDL P1410 against PRG and WDI. Errors are collected and reported as warnings."""

from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import traceback

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common.r2 import req, s3
from warehouse import publish

ROOT = Path(__file__).resolve().parents[1]
W = ROOT / "warehouse"
SEEDS = ("unit", "unit_map", "kpi", "kpi_measure")
GOLD_COPIES = ("fact_annual", "fact_quarterly", "fact_monthly", "fact_daily", "dim_series", "dim_measure",
               "dim_geography", "dim_calendar")
REPORT: dict[str, object] = {}
ERRORS: list[str] = []
CHUNK = 3700


def emit(level: str, title: str, data) -> int:
    text = json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))
    parts = [text[i : i + CHUNK] for i in range(0, max(len(text), 1), CHUNK)]
    for n, part in enumerate(parts, 1):
        msg = part.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::{level} title={title} [{n}/{len(parts)}]::{msg}", flush=True)
    return len(parts)


def flush() -> None:
    """REPORT as notices (at most 10 annotations: sections are packed in order), ERRORS as warnings."""
    budget = 10
    for key, value in REPORT.items():
        size = len(json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":")))
        need = size // CHUNK + 1
        if need > budget:
            ERRORS.append(f"report section {key} left out ({size} chars, {budget} annotations left)")
            continue
        budget -= emit("notice", key, value)
    if ERRORS:
        emit("warning", "errors", ERRORS[:40])


def guard(name: str, fn, *args):
    try:
        return fn(*args)
    except Exception:  # noqa: BLE001 - report, keep the other checks
        ERRORS.append(f"{name}: {traceback.format_exc()[-1500:]}")
        return None


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
        ["dbt", "compile", "--target", "prod", "--select", "path:models/gold/poc_metrics_*.sql",
         "poc_concept_measures", "poc_build_prg", "metrics_unit_base", "metrics_unit_unmapped", "--threads", "4"],
        cwd=W, env=env, capture_output=True, text=True,
    )
    if run.returncode:
        ERRORS.append("compile: " + " | ".join(run.stdout.splitlines()[-15:]))
        raise RuntimeError("dbt compile failed")
    return W / "target" / "compiled" / "zohelo"


def local(sql: str) -> str:
    """Compiled SQL with the branch seeds and the in-memory gold copies in place of r2.gold."""
    for name in (*SEEDS, *GOLD_COPIES, "concept_measures"):
        sql = sql.replace(f'"r2"."gold"."{name}"', f"d33.{name}")
    return sql


def arrow(cur) -> pa.Table:
    if hasattr(cur, "to_arrow_table"):  # duckdb 2
        return cur.to_arrow_table()
    got = cur.arrow()
    return got.read_all() if hasattr(got, "read_all") else got


def rows(con, sql: str) -> list[list]:
    return [[v if isinstance(v, (int, float)) or v is None else str(v) for v in r] for r in con.execute(sql).fetchall()]


def short(x, n: int = 48) -> str:
    x = "" if x is None else str(x)
    return x if len(x) <= n else x[: n - 1] + "…"


def setup(con, compiled: Path) -> list[str]:
    con.execute("CREATE SCHEMA d33")
    for s in SEEDS:
        con.execute(f"CREATE TABLE d33.{s} AS SELECT * FROM read_csv('{W}/seeds/{s}.csv', header=true, all_varchar=true)")
    con.execute("CREATE OR REPLACE TABLE d33.unit AS SELECT * REPLACE (CAST(factor AS DECIMAL(38,9)) AS factor) FROM d33.unit")
    con.execute("CREATE OR REPLACE TABLE d33.kpi_measure AS SELECT * REPLACE (CAST(priority AS INTEGER) AS priority) FROM d33.kpi_measure")
    for t in GOLD_COPIES:
        con.execute(f"CREATE TABLE d33.{t} AS SELECT * FROM r2.gold.{t}")
    con.execute(f"CREATE TABLE d33.concept_measures AS {local((compiled / 'models/gold/poc_concept_measures.sql').read_text())}")
    have = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'r2' AND table_schema = 'gold'").fetchall()}
    return sorted(k["kpi_key"] for k in csv.DictReader((W / "seeds/kpi.csv").open(encoding="utf-8"))
                  if f"metrics_{k['kpi_key']}" in have)


def units_report(con, compiled: Path) -> None:
    text = (compiled / "tests/metrics_unit_base.sql").read_text()
    inner = re.search(r"from \((.*)\)\s*where", text, re.S).group(1)
    con.execute(f"CREATE TABLE d33.measure_units AS {local(inner)}")
    allm = rows(con, "SELECT concept_key, measure_key, unit_code, source_unit_key, target_unit_key, ratio FROM d33.measure_units ORDER BY ALL")
    base = rows(con, local((compiled / "tests/metrics_unit_base.sql").read_text()))
    unm = rows(con, local((compiled / "tests/metrics_unit_unmapped.sql").read_text()))
    REPORT["units"] = {
        "measures": len(allm), "mapped": sum(r[3] is not None for r in allm),
        "converted (ratio<>1)": [[r[0], r[1], r[2], r[3], r[4], r[5]] for r in allm if r[5] not in (None, 1.0)],
        "test metrics_unit_base rows": [[r[0], short(r[1], 70), r[3], r[4], r[6]] for r in base],
        "test metrics_unit_unmapped rows": len(unm),
    }
    REPORT["unmapped"] = sorted({f"{r[2]}|{short(r[3], 40)}|{short(r[1], 60)}" for r in unm})


def metrics_diff(con, compiled: Path, kpis: list[str]) -> dict[str, pa.Table]:
    new_tables: dict[str, pa.Table] = {}
    changed, only, totals, per_kpi_err = [], [], {"kpis": 0, "rows_old": 0, "rows_new": 0, "rows_value_changed": 0, "rows_unit_label_changed": 0}, []
    for kpi in kpis:
        try:
            con.execute(f"CREATE OR REPLACE TABLE d33.new AS {local((compiled / f'models/gold/poc_metrics_{kpi}.sql').read_text())}")
            con.execute(f"CREATE OR REPLACE TABLE d33.old AS SELECT * FROM r2.gold.metrics_{kpi}")
            new_tables[kpi] = arrow(con.execute("SELECT * FROM d33.new"))
            per = rows(con, """
                WITH j AS (
                    SELECT coalesce(n.measure_key, o.measure_key) AS measure_key, o.value AS ov, n.value AS nv,
                        o.unit AS ou, n.unit AS nu, n.unit_published AS np, o.series_key IS NULL AS only_new,
                        n.series_key IS NULL AS only_old, coalesce(n.geography_key, o.geography_key) AS g,
                        coalesce(n.date_key, o.date_key) AS d
                    FROM d33.old o FULL JOIN d33.new n USING (series_key, date_key)
                )
                SELECT measure_key, count(*) FILTER (WHERE NOT only_new) AS n_old, count(*) FILTER (WHERE NOT only_old) AS n_new,
                    count(*) FILTER (WHERE only_new) AS only_new, count(*) FILTER (WHERE only_old) AS only_old,
                    count(*) FILTER (WHERE ov IS DISTINCT FROM nv AND NOT only_new AND NOT only_old) AS changed,
                    count(*) FILTER (WHERE ou IS DISTINCT FROM np AND NOT only_new AND NOT only_old) AS published_unit_differs,
                    any_value(ou), any_value(nu),
                    arg_max([g, cast(d AS varchar), cast(ov AS varchar), cast(nv AS varchar)], (g = 'PL')::int * 100000 + year(d))
                        FILTER (WHERE NOT only_new AND NOT only_old)
                FROM j GROUP BY ALL ORDER BY ALL""")
            totals["kpis"] += 1
            for m, n_old, n_new, o_new, o_old, ch, pud, ou, nu, sample in per:
                totals["rows_old"] += n_old
                totals["rows_new"] += n_new
                totals["rows_value_changed"] += ch
                if ch:
                    changed.append([kpi, m, ch, n_old, ou, nu, sample])
                if o_new or o_old or pud:
                    only.append([kpi, m, n_old, n_new, o_new, o_old, pud])
        except Exception:  # noqa: BLE001
            per_kpi_err.append(f"{kpi}: {traceback.format_exc()[-400:]}")
    REPORT["metrics diff"] = {"totals": totals, "changed [kpi, measure, rows changed, rows, published unit, target unit, PL newest old→new]": changed,
                              "rows only in one side or published unit differs [kpi, measure, old, new, only_new, only_old, unit_differs]": only}
    ERRORS.extend(per_kpi_err)
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
    summary, details = {}, []
    for kpi_key, kpi in {k["kpi_key"]: k for k in publish.seed("kpi")}.items():
        if kpi["status"] != "published":
            continue
        res: dict = {}
        try:
            old_metrics = arrow(con.execute(f"SELECT * FROM r2.gold.metrics_{kpi_key}"))
            new_metrics = new_tables[kpi_key]
            old_series = publish.kpi_series(old_metrics, main_measures, kpi_key, main_kpis[kpi_key]["unit_key"])
            new_series = publish.kpi_series(new_metrics, branch_measures, kpi_key, kpi["unit_key"])
            cols = ["source_key", "measure_key", "geography_key", "period", "value", "unit_key"]
            order = [(c, "ascending") for c in cols[:4]]
            try:
                body = client.get_object(Bucket=bucket, Key=f"{publish.PREFIX}kpi/{kpi_key}/series.parquet")["Body"].read()
                prod = pq.read_table(io.BytesIO(body))
                res["series main=prod"] = prod.num_rows == old_series.num_rows and prod.select(cols).sort_by(order).equals(old_series.select(cols).sort_by(order))
            except Exception as exc:  # noqa: BLE001
                res["series main=prod"] = f"error {short(exc, 80)}"
            con.register("os", old_series)
            con.register("ns", new_series)
            res["series rows old/new"] = [old_series.num_rows, new_series.num_rows]
            res["series changed [src, measure, rows, unmatched, unit old→new]"] = rows(con, """
                SELECT coalesce(o.source_key, n.source_key), coalesce(o.measure_key, n.measure_key),
                    count(*) FILTER (WHERE o.value IS DISTINCT FROM n.value OR o.unit_key IS DISTINCT FROM n.unit_key),
                    count(*) FILTER (WHERE o.source_key IS NULL OR n.source_key IS NULL),
                    any_value(o.unit_key) || '→' || any_value(n.unit_key)
                FROM os o FULL JOIN ns n USING (source_key, measure_key, geography_key, period, frequency)
                GROUP BY ALL HAVING count(*) FILTER (WHERE o.value IS DISTINCT FROM n.value OR o.unit_key IS DISTINCT FROM n.unit_key
                    OR o.source_key IS NULL OR n.source_key IS NULL) > 0 ORDER BY ALL""")
            con.unregister("os")
            con.unregister("ns")
            if kpi_key not in publish.SKIP:
                old_latest = publish.kpi_latest(old_metrics, main_measures, {}, kpi_key, main_kpis[kpi_key]["unit_key"], "x")
                new_latest = publish.kpi_latest(new_metrics, branch_measures, {}, kpi_key, kpi["unit_key"], "x")
                try:
                    prod_latest = json.loads(client.get_object(Bucket=bucket, Key=f"{publish.PREFIX}kpi/{kpi_key}/latest.json")["Body"].read())
                    res["latest main=prod"] = strip(prod_latest) == strip(old_latest)
                except Exception as exc:  # noqa: BLE001
                    res["latest main=prod"] = f"error {short(exc, 80)}"
                ov = {(v["source_key"], v["geography_key"]): v for v in old_latest["values"]}
                nv = {(v["source_key"], v["geography_key"]): v for v in new_latest["values"]}
                diff = [k for k in sorted(set(ov) | set(nv)) if strip(ov.get(k)) != strip(nv.get(k))]
                res["latest values old/new"] = [len(ov), len(nv)]
                by: dict[str, int] = {}
                for k in diff:
                    by[k[0]] = by.get(k[0], 0) + 1
                res["latest changed by source"] = by
                for k in diff:
                    if k[1] not in ("PL", "DE", "US", "CN", "EUROSTAT:EU27_2020", "WB:WLD", "WB:EUU"):
                        continue
                    a, b = ov.get(k) or {}, nv.get(k) or {}
                    details.append([kpi_key, k[0], k[1], a.get("period"), a.get("value"), a.get("unit_key"), b.get("value"),
                                    b.get("unit_key"), a.get("previous_value"), b.get("previous_value")])
        except Exception:  # noqa: BLE001
            ERRORS.append(f"v1 {kpi_key}: {traceback.format_exc()[-600:]}")
        summary[kpi_key] = res
    REPORT["v1 published KPIs"] = summary
    REPORT["v1 latest.json changes, sample geographies [kpi, source, geo, period, old value, old unit, new value, new unit, old prev, new prev]"] = details


def prg_and_area(con, compiled: Path, new_tables: dict[str, pa.Table]) -> None:
    out: dict = {}
    con.execute(f"CREATE TABLE d33.build_prg_new AS {(compiled / 'models/gold/poc_build_prg.sql').read_text()}")
    out["build_prg new vs prod [n, matched, new measure, old measure, new unit, old unit, rows ha/100 differ beyond float, max abs diff, sample]"] = rows(con, """
        SELECT count(*), count(o.geography_key), any_value(n.measure_key), any_value(o.measure_key), any_value(n.unit_code),
            any_value(o.unit_code), count(*) FILTER (WHERE abs(n.value / 100 - o.value) > 1e-9 * greatest(abs(o.value), 1)),
            max(abs(n.value / 100 - o.value)),
            arg_max([n.geography_key, cast(n.value AS varchar), cast(o.value AS varchar)], n.geography_key = 'TERC:1400000')
        FROM d33.build_prg_new AS n LEFT JOIN r2.gold.build_prg AS o USING (geography_key, date_key)""")
    text = (W / "macros/poc_metrics.sql").read_text()
    head = "{% macro poc_rescale(value, ratio) %}"
    body = text[text.index(head) + len(head) : text.rindex("{% endmacro %}")].replace("{{ value }}", "value").replace("{{ ratio }}", "0.01")
    out["metrics_area PRG km² from hectares vs prod km² [n, matched, exact, max abs diff, worst]"] = rows(con, f"""
        WITH r AS (SELECT geography_key, date_key, value AS ha, {body} AS km2 FROM d33.build_prg_new)
        SELECT count(*), count(m.value), count(*) FILTER (WHERE r.km2 = m.value), max(abs(r.km2 - m.value)),
            arg_max([r.geography_key, cast(r.ha AS varchar), cast(r.km2 AS varchar), cast(m.value AS varchar)], abs(r.km2 - m.value))
        FROM r LEFT JOIN r2.gold.metrics_area AS m
            ON m.source_system = 'gugik_prg' AND m.geography_key = r.geography_key AND m.date_key = r.date_key""")
    out["prod metrics_area newest [source, measure, unit, geo, date, value]"] = rows(con, """
        SELECT source_system, measure_key, unit, geography_key, max(date_key), arg_max(value, date_key)
        FROM r2.gold.metrics_area WHERE geography_key IN ('PL', 'TERC:1400000', 'TERC:0200000') GROUP BY ALL ORDER BY 4, 1""")
    out["build_bdl P1410 as published [measure, unit code, unit name, rows, min, max, PL newest, date]"] = rows(con, """
        SELECT measure_key, unit_code, unit_name, count(*), min(value), max(value),
            arg_max(value, date_key) FILTER (WHERE geography_key = 'PL'), max(date_key)
        FROM r2.gold.build_bdl WHERE block_id = 'P1410' GROUP BY ALL""")
    if "area" in new_tables:
        con.register("na", new_tables["area"])
        out["branch metrics_area newest [source, measure, geo, date, published, unit published, value, unit]"] = rows(con, """
            SELECT source_system, measure_key, geography_key, date_key, value_published, unit_published, value, unit FROM na
            WHERE geography_key IN ('PL', 'TERC:1400000', 'TERC:0200000')
            QUALIFY row_number() OVER (PARTITION BY source_system, geography_key ORDER BY date_key DESC) = 1 ORDER BY 3, 1""")
    REPORT["prg and area"] = out


def main() -> int:
    con = None
    try:
        compiled = compile_branch()
        con = connect()
        kpis = setup(con, compiled)
        guard("units", units_report, con, compiled)
        new_tables = guard("metrics", metrics_diff, con, compiled, kpis) or {}
        guard("v1", v1_compare, con, new_tables)
        guard("prg", prg_and_area, con, compiled, new_tables)
    except Exception:  # noqa: BLE001
        ERRORS.append(f"trial: {traceback.format_exc()[-1500:]}")
    flush()
    return 1 if ERRORS else 0


if __name__ == "__main__":
    sys.exit(main())
