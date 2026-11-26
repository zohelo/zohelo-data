"""The dbt part of the pse_reports trial (#2870): the real `dbt run` and `dbt test` of the source's two Silver models
and its Gold build stage with every dbt test attached to them, on a local DuckDB file attached as `r2` (the pattern of
dev/nfz_pharmacy_refunds_dbt.py), so nothing is written to the R2 Data Catalog. The file holds copies of the scratch
Bronze tables as r2.bronze.pse_reports_rce_pln / _his_wlk_cal and the source's rows of the geography map as
r2.gold.map_geography (the static list of poc_geography__pse_reports, which is what the shared map holds for this
source). Built twice. Test branch only, never on main.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import duckdb
import jinja2
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "warehouse"
SELECT = ["pse_reports_rce_pln", "pse_reports_his_wlk_cal", "poc_build_pse_reports"]
PROFILE = """zohelo:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: ":memory:"
      threads: 1
      schema: main
      attach:
        - path: {catalog}
          alias: r2
"""


def geography_rows() -> str:
    """The source's map rows, rendered from its own macro file (static SQL)."""
    module = jinja2.Environment().from_string((PROJECT / "macros/sources/pse_reports.sql").read_text()).module
    return f"with {module.poc_geography__pse_reports()} select * from pse_reports"


def dbt(command: str, flags: list[str], workdir: str) -> list[list]:
    """One dbt command in a subprocess; [node, status, failures] per node from run_results.json."""
    target = Path(workdir) / "target"
    (target / "run_results.json").unlink(missing_ok=True)
    proc = subprocess.run([sys.executable, "-m", "dbt.cli.main", command, *flags, "--select", *SELECT],
                          capture_output=True, text=True, timeout=1800, check=False)  # fmt: skip
    results = target / "run_results.json"
    if not results.exists():
        raise RuntimeError(f"dbt {command} did not run: {(proc.stdout + proc.stderr)[-1500:]}")
    out = []
    for r in json.loads(results.read_text())["results"]:
        uid = r["unique_id"]
        name = uid.split(".")[-1] if uid.startswith("model.") else uid.split(".")[2]
        out.append([name, r["status"], r["failures"], (r.get("message") or "")[:700] if r["status"] not in ("success", "pass") else ""])
    return out


def build(rce: pa.Table, his: pa.Table, workdir: str) -> tuple[str, dict[str, list]]:
    """dbt run + dbt test, twice; returns the database file and the node results of each command."""
    import yaml

    catalog = Path(workdir) / "r2.duckdb"
    with duckdb.connect(str(catalog)) as con:
        con.register("rce_arrow", rce)
        con.register("his_arrow", his)
        con.execute("create schema bronze; create schema gold")
        con.execute("create table bronze.pse_reports_rce_pln as select * from rce_arrow")
        con.execute("create table bronze.pse_reports_his_wlk_cal as select * from his_arrow")
        con.execute("create table gold.map_geography as " + geography_rows())
    (Path(workdir) / "profiles.yml").write_text(PROFILE.format(catalog=json.dumps(str(catalog))), encoding="utf-8")
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    topics = yaml.safe_load((PROJECT / "warehouse.yaml").read_text(encoding="utf-8"))["topics"]
    flags = [
        "--project-dir", str(PROJECT), "--profiles-dir", workdir, "--target", "prod",
        "--target-path", str(Path(workdir) / "target"), "--log-path", str(Path(workdir) / "logs"),
        "--no-partial-parse", "--vars", json.dumps({"topics": topics}),
    ]  # fmt: skip
    out: dict[str, list] = {}
    for n in (1, 2):
        out[f"run{n}"] = dbt("run", flags, workdir)
        if any(r[1] != "success" for r in out[f"run{n}"]):
            out[f"rows{n}"] = None  # a model failed: its message is in run<n>, the tests would only error
            out[f"test{n}"] = []
            continue
        with duckdb.connect(str(catalog), read_only=True) as con:
            out[f"rows{n}"] = [con.sql(f"select count(*) from {t}").fetchall()[0][0] for t in (
                "silver.pse_reports_rce_pln", "silver.pse_reports_his_wlk_cal", "gold.build_pse_reports")]  # fmt: skip
        out[f"test{n}"] = dbt("test", flags, workdir)
    return str(catalog), out


if __name__ == "__main__":  # local check with synthetic Bronze: python -m dev.pse_reports_dbt
    import random
    import tempfile
    from datetime import date, datetime, timedelta, timezone
    from zoneinfo import ZoneInfo

    from ingestion.to_bronze.pse_reports import load

    rng = random.Random(1)
    w = ZoneInfo("Europe/Warsaw")
    rows = {"rce_pln": [], "his_wlk_cal": []}
    days = [date(2024, 6, 14) + timedelta(days=i) for i in range((date(2025, 4, 2) - date(2024, 6, 14)).days)]
    for report in rows:
        for d in days:
            start = datetime(d.year, d.month, d.day, tzinfo=w).astimezone(timezone.utc)
            end = datetime((d + timedelta(days=1)).year, (d + timedelta(days=1)).month, (d + timedelta(days=1)).day, tzinfo=w).astimezone(timezone.utc)
            n = int((end - start).total_seconds() // 900)
            for i in range(n):
                t_utc = start + timedelta(minutes=15 * (i + 1))
                s_loc = (t_utc - timedelta(minutes=15)).astimezone(w)  # PSE labels the end with the offset at the start
                label = (t_utc.replace(tzinfo=None) + s_loc.utcoffset()).strftime("%Y-%m-%d %H:%M:%S")
                if s_loc.fold == 1:  # the repeated autumn hour: '2024-10-27 02a:15:00'
                    label = label[:13] + "a" + label[13:]
                row = {f: None for f in load.FIELDS[report]}
                row.update({"business_date": str(d), "period": "x", "period_utc": "y",
                            "dtime": label, "dtime_utc": t_utc.strftime("%Y-%m-%d %H:%M:%S"),
                            "publication_ts": "2024-10-27 02a:52:11.658", "publication_ts_utc": "2026-01-18 17:35:16.045"})
                for f in load.VALUES[report]:
                    row[f] = None if f in ("jga", "jgo") else str(round(rng.uniform(-50, 900), 3))
                if report == "his_wlk_cal" and d == date(2024, 7, 1) and i == 5:
                    row["wi"] = None  # an incomplete wind day: left out of Gold for wi only
                row.update({"entity": f"{report}_{d:%Y%m%d}", "raw_sha256": f"sha_{report}_{d}", "source_version": "20260118T173516Z",
                            "ingested_at": datetime(2026, 11, 26, tzinfo=timezone.utc), "source_row_number": i + 1})
                rows[report].append(row)
    tables = {r: pa.Table.from_pylist(v, schema=load.bulk_bronze.arrow_schema(load.columns_of(r))) for r, v in rows.items()}
    with tempfile.TemporaryDirectory() as tmp:
        path, runs = build(tables["rce_pln"], tables["his_wlk_cal"], tmp)
        for name, run in runs.items():
            print(name, json.dumps(run))
        with duckdb.connect(path) as con:
            print(con.sql("select measure_key, count(*), min(date_key), max(date_key) from gold.build_pse_reports group by 1").fetchall())
            print(con.sql("select periods_expected, count(distinct business_date) from silver.pse_reports_his_wlk_cal group by 1").fetchall())
