"""The dbt part of the ure_prices trial (#2870): the real `dbt run` and `dbt test` of the Silver model and the Gold
block with every dbt test attached to them, on a local DuckDB file attached as `r2` (the pattern of
dev/nfz_pharmacy_refunds_dbt.py), so nothing is written to the R2 Data Catalog. The file holds a copy of the scratch
Bronze table as r2.bronze.ure_prices_cells and the source's rows of the geography map as r2.gold.map_geography (the
static list of poc_geography__ure_prices). Built twice. Deleted before merge.
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
SELECT = ["ure_prices_indicators", "poc_build_ure_prices"]
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
    module = jinja2.Environment().from_string((PROJECT / "macros/sources/ure_prices.sql").read_text()).module
    return f"with {module.poc_geography__ure_prices()} select * from ure_prices"


def dbt(command: str, flags: list[str], workdir: str) -> list[list]:
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
        con.execute("create table bronze.ure_prices_cells as select * from bronze_arrow")
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
            out[f"rows{n}"] = [con.sql("select count(*) from silver.ure_prices_indicators").fetchall()[0][0],
                               con.sql("select count(*) from gold.build_ure_prices").fetchall()[0][0]]
        out[f"test{n}"] = dbt("test", flags, workdir)
    return str(catalog), out


if __name__ == "__main__":  # local check on saved pages: python -m dev.ure_prices_dbt <entity>=<file> ...
    import tempfile

    from ingestion.to_bronze.ure_prices import load
    from ingestion.to_landing.ure_prices import reduce_landing

    tables = []
    for arg in sys.argv[1:]:
        entity, path = arg.split("=", 1)
        body = reduce_landing(Path(path).read_bytes())
        receipt = {"entity": entity, "sha256": f"sha_{entity}", "version_token": "20261126T000000Z",
                   "ingested_at": "20261126T000000Z"}
        tables.append(load.decode(body, receipt))
    with tempfile.TemporaryDirectory() as tmp:
        path, runs = build(pa.concat_tables(tables), tmp)
        for name, run in runs.items():
            print(name, json.dumps(run))
        with duckdb.connect(path) as con:
            print(con.sql("select indicator_code, frequency, unit_code, value_status, count(*), min(period_label), max(period_label) "
                          "from silver.ure_prices_indicators group by all order by all").fetchall())
            print(con.sql("select measure_key, count(*), min(date_key), max(date_key), min(value), max(value) "
                          "from gold.build_ure_prices group by all order by all").fetchall())
