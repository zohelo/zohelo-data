"""The dbt part of the nfz_pharmacy_refunds trial (#2930): the real `dbt build` of the source's Silver model and Gold
block with every dbt test attached to them, on a local DuckDB file attached as `r2` (the pattern of
ops/dbt_compile.py), so nothing is written to the R2 Data Catalog. The file holds a copy of the scratch Bronze table
as r2.bronze.nfz_ra_monthly_provisions and the source's rows of the geography map as r2.gold.map_geography (the
static list of poc_geography__nfz_pharmacy_refunds, which is what the shared map holds for this source). Built
twice; the analysis queries the built tables. Deleted before merge.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import duckdb
import jinja2
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "warehouse"
SELECT = ["nfz_ra_monthly_provisions", "poc_build_nfz_pharmacy_refunds"]
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
    module = jinja2.Environment().from_string((PROJECT / "macros/sources/nfz_pharmacy_refunds.sql").read_text()).module
    return f"with {module.poc_geography__nfz_pharmacy_refunds()} select * from nfz_pharmacy_refunds"


def dbt(command: str, flags: list[str], workdir: str) -> list[list]:
    """One dbt command in a subprocess (dbt keeps the attached file open in its own process); [node, status,
    failures] per node from run_results.json."""
    target = Path(workdir) / "target"
    (target / "run_results.json").unlink(missing_ok=True)
    proc = subprocess.run([sys.executable, "-m", "dbt.cli.main", command, *flags, "--select", *SELECT],
                          capture_output=True, text=True, timeout=1200, check=False)
    results = target / "run_results.json"
    if not results.exists():
        raise RuntimeError(f"dbt {command} did not run: {(proc.stdout + proc.stderr)[-1500:]}")
    return [[r["unique_id"].split(".")[-1] if r["unique_id"].startswith("model.") else r["unique_id"].split(".")[2],
             r["status"], r["failures"]] for r in json.loads(results.read_text())["results"]]


def build(bronze: pa.Table, workdir: str) -> tuple[str, dict[str, list]]:
    """dbt run + dbt test, twice; returns the database file and the node results of each command."""
    import yaml

    catalog = Path(workdir) / "r2.duckdb"
    with duckdb.connect(str(catalog)) as con:
        con.register("bronze_arrow", bronze)
        con.execute("create schema bronze; create schema gold")
        con.execute("create table bronze.nfz_ra_monthly_provisions as select * from bronze_arrow")
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
        with duckdb.connect(str(catalog), read_only=True) as con:
            out[f"rows{n}"] = [con.sql("select count(*) from silver.nfz_ra_monthly_provisions").fetchall()[0][0],
                               con.sql("select count(*) from gold.build_nfz_pharmacy_refunds").fetchall()[0][0]]
        out[f"test{n}"] = dbt("test", flags, workdir)
    return str(catalog), out


if __name__ == "__main__":  # local check with synthetic Bronze: python -m dev.nfz_pharmacy_refunds_dbt
    import random

    from ingestion.to_bronze.nfz_pharmacy_refunds import load

    rng, rows = random.Random(1), []
    for area in ["PL"] + [f"{i:02d}" for i in range(1, 17)]:
        for atc in ["_T", "A", "C", "N"]:
            for n in range(96):
                y, m = 2017 + n // 12, n % 12 + 1
                v = [round(rng.uniform(1, 1e6), 2) for _ in range(4)]
                rows.append({"entity": f"{area}_{'T' if atc == '_T' else atc}", "area_code": area, "atc_code": atc,
                             "atc_name": None if atc == "_T" else f"GROUP {atc}", "year": str(y), "month": str(m),
                             "quantity": str(v[0]), "refund": str(v[1]), "donation": str(v[2]),
                             "contribution_of_patient": str(v[3]), "value": str(round(v[1] + v[3], 2)),
                             "meta_count": 96, "page": n // 25 + 1, "raw_sha256": f"sha{area}{atc}{n // 25}",
                             "source_version": "20241231T000000Z", "ingested_at": None, "source_row_number": n % 25 + 1})
    table = pa.Table.from_pylist(rows, schema=load.ARROW)
    with tempfile.TemporaryDirectory() as tmp:
        path, runs = build(table, tmp)
        for name, run in runs.items():
            print(name, json.dumps(run))
        with duckdb.connect(path) as con:
            print(con.sql("select count(*) from silver.nfz_ra_monthly_provisions").fetchall(),
                  con.sql("select count(*) from gold.build_nfz_pharmacy_refunds").fetchall())
