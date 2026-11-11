"""Gold trial for the statistical sources: dbt build of the seeds and the Gold models (without topics) against real
Silver, written to the scratch namespace zz_dev_gold_stat (dbt var gold_schema), then build_check rows of the given
sources, metrics row counts and samples printed to the log; the scratch tables are dropped at the end (finally).
Never publishes: catalog.publish() and gold.publish are not called. Deleted before merge.
Usage: python -m dev.gold_stat_trial [--concepts a,b] [--sources eurostat,gus_bdl] [--keep]"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import time

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "zz_dev_gold_stat"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--concepts", default="")
    parser.add_argument("--sources", default="eurostat,gus_bdl,gus_dbw,nbp,nbp_interest_rates")
    parser.add_argument("--select", default="path:seeds path:models/gold")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args(argv)
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "warehouse"), "DUCKDB_TEMP_DIRECTORY": "/mnt/duckdb_tmp"}
    subprocess.run(["sudo", "mkdir", "-p", "/mnt/duckdb_tmp"], check=False)
    subprocess.run(["sudo", "chmod", "777", "/mnt/duckdb_tmp"], check=False)
    cat = bf.retry(catalog, "catalog")
    try:
        started = time.monotonic()
        failed = []
        for verb, sel in (("seed", ["path:seeds"]), ("run", args.select.split()), ("test", args.select.split())):
            cmd = ["dbt", verb, "--target", "prod", "--select", *sel, "--exclude", "path:models/gold/topics",
                   "path:tests/topic_quality.sql", "path:tests/topic_quality_warn.sql", "path:tests/topic_seeds.sql",
                   "--threads", "2", "--vars", f"{{gold_schema: {NAMESPACE}}}"]
            run = subprocess.run(cmd, cwd=ROOT / "warehouse", env=env, capture_output=True, text=True)
            for wait in (60, 180, 300):  # HTTP 429 from the catalog: resume the failed nodes
                if run.returncode == 0 or verb == "test" or "429" not in run.stdout:
                    break
                time.sleep(wait)
                run = subprocess.run(["dbt", "retry", "--target", "prod", "--vars", f"{{gold_schema: {NAMESPACE}}}"],
                                     cwd=ROOT / "warehouse", env=env, capture_output=True, text=True)
            print(f"\n##### dbt {verb} exit {run.returncode}")
            print(run.stdout[-40000:])
            results = json.loads((ROOT / "warehouse" / "target" / "run_results.json").read_text())
            failed += [(verb, r["unique_id"].split(".")[-1], r["status"], (r.get("message") or "")[:200])
                       for r in results["results"] if r["status"] not in ("success", "pass")]
        annotate("notice" if not failed else "warning", "gold stat trial build",
                 {"minutes": round((time.monotonic() - started) / 60, 1), "failed": failed[:15]})
        import duckdb

        con = duckdb.connect()
        counts = {}
        for ident in cat.list_tables(NAMESPACE):
            snap = cat.load_table(ident).current_snapshot()
            counts[ident[-1]] = int((snap.summary or {}).get("total-records", -1)) if snap else 0
        print("\n=== ROWS", json.dumps(counts, sort_keys=True))
        try:
            check = cat.load_table((NAMESPACE, "build_check")).scan().to_arrow()
            con.register("bc", check)
            srcs = ",".join(f"'{s}'" for s in args.sources.split(","))
            print("\n=== BUILD CHECK")
            for r in con.sql(f"select * from bc where source_system in ({srcs}) order by 1, 2").fetchall():
                print("\t".join(str(v)[:200] for v in r))
        except Exception as exc:
            print("build_check not read", exc)
        facts = []
        for name in ("fact_annual", "fact_semiannual", "fact_quarterly", "fact_monthly", "fact_daily"):
            try:
                facts.append(cat.load_table((NAMESPACE, name)).scan(
                    selected_fields=("series_key", "date_key", "source_system", "block_id")).to_arrow())
            except Exception as exc:
                print("fact not read", name, exc)
        import pyarrow as pa

        con.register("allfacts", pa.concat_tables(facts, promote_options="default"))
        dups = con.sql("select source_system, block_id, count(*) keys from (select source_system, block_id, series_key, "
                       "date_key from allfacts group by all having count(*) > 1) group by all order by 3 desc").fetchall()
        print("\n=== GRAIN DUPLICATES (series_key x date_key) by block", dups[:40])
        annotate("notice" if not dups else "warning", "gold stat grain", {"duplicate_keys_by_block": dups[:30]})
        for concept in [c for c in args.concepts.split(",") if c]:
            try:
                t = cat.load_table((NAMESPACE, f"metrics_{concept}")).scan().to_arrow()
                con.register("m", t)
                print(f"\n=== METRICS {concept}: {t.num_rows} rows")
                for r in con.sql(
                    "select source_system, measure_key, frequency, count(*) n, count(distinct geography_key) geos, "
                    "min(date_key), max(date_key), min(value), max(value) from m group by all order by 1, 2"
                ).fetchall():
                    print("\t".join(str(v)[:160] for v in r))
                for r in con.sql(
                    "select source_system, measure_key, date_key, value, unit from m where geography_key = 'PL' "
                    "qualify row_number() over (partition by measure_key order by date_key desc) <= 3 order by 2, 3"
                ).fetchall():
                    print("  PL", "\t".join(str(v)[:120] for v in r))
            except Exception as exc:
                print(f"metrics {concept} not read: {exc}")
    finally:
        if not args.keep:
            dropped = 0
            for ident in cat.list_tables(NAMESPACE):
                try:
                    cat.drop_table(ident)
                    dropped += 1
                except Exception as exc:  # a scratch table left behind is reported, not fatal
                    print("drop failed", ident, exc)
            annotate("notice", "gold stat trial dropped", {"namespace": NAMESPACE, "dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
