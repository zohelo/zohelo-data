"""Gold trial for #2743: dbt seed/run/test of the Gold project into zz_dev_gold_stat (dbt var gold_schema), then
compact annotations: test results, grain by series_key x date_key, build_check of the slice blocks, the rows of the
metrics_value_range and metrics_geography_unmatched tests (their SQL executed on the scratch tables), and the
published BDL measures (P2137, P2361) against production gold.build_bdl. Scratch tables dropped at the end."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import time

import duckdb
import pyarrow as pa

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
NS = "zz_dev_gold_stat"
SLICE = ["EARN_MW_CUR", "ERT_BIL_EUR_M", "PRC_HICP_MINR", "P2656", "P2713", "P2692", "P2721", "P2837", "P4610",
         "P3788", "P3794", "1603_1339_116_4", "1618_1335_95_4", "1714_1575_243_1", "309_485_180_3", "507_870_93_1",
         "policy_rates"]


def table(cat, name, fields=None):
    t = cat.load_table((NS, name))
    return t.scan(selected_fields=fields).to_arrow() if fields else t.scan().to_arrow()


def run_test_sql(con, cat, path):
    sql = (ROOT / "warehouse/tests" / path).read_text()
    sql = re.sub(r"\{\{\s*config\([^)]*\)\s*\}\}", "", sql)
    for name in sorted(set(re.findall(r"\{\{ ref\('([a-z0-9_]+)'\) \}\}", sql))):
        alias = name.replace("poc_", "", 1) if name.startswith("poc_") else name  # prod target: table = alias
        con.register(f"t_{name}", table(cat, alias))
        sql = sql.replace(f"{{{{ ref('{name}') }}}}", f"t_{name}")
    return con.sql(sql)


def main() -> int:
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "warehouse"), "DUCKDB_TEMP_DIRECTORY": "/mnt/duckdb_tmp"}
    subprocess.run(["sudo", "mkdir", "-p", "/mnt/duckdb_tmp"], check=False)
    subprocess.run(["sudo", "chmod", "777", "/mnt/duckdb_tmp"], check=False)
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    try:
        started, failed = time.monotonic(), []
        excl = ["--exclude", "path:models/gold/topics", "path:tests/topic_quality.sql",
                "path:tests/topic_quality_warn.sql", "path:tests/topic_seeds.sql"]
        for verb, sel in (("seed", ["path:seeds"]), ("run", ["path:models/gold"]), ("test", ["path:models/gold",
                                                                                         "path:seeds", "path:tests"])):
            cmd = ["dbt", verb, "--target", "prod", "--select", *sel, *excl, "--threads", "2",
                   "--vars", f"{{gold_schema: {NS}}}"]
            run = subprocess.run(cmd, cwd=ROOT / "warehouse", env=env, capture_output=True, text=True)
            for wait in (60, 180, 300):  # HTTP 429 from the catalog
                if run.returncode == 0 or verb == "test" or "429" not in run.stdout:
                    break
                time.sleep(wait)
                run = subprocess.run(["dbt", "retry", "--target", "prod", "--vars", f"{{gold_schema: {NS}}}"],
                                     cwd=ROOT / "warehouse", env=env, capture_output=True, text=True)
            print(run.stdout[-30000:])
            res = json.loads((ROOT / "warehouse/target/run_results.json").read_text())["results"]
            failed += [[verb, r["unique_id"].split(".")[-1][:60], r["status"], (r.get("message") or "")[:90]]
                       for r in res if r["status"] not in ("success", "pass")]
        annotate("notice" if not failed else "warning", "1 dbt build and tests",
                 {"minutes": round((time.monotonic() - started) / 60, 1), "not_passed": failed[:12]})
        facts = [table(cat, n, ("series_key", "date_key", "source_system", "block_id")) for n in
                 ("fact_annual", "fact_semiannual", "fact_quarterly", "fact_monthly", "fact_daily")]
        con.register("f", pa.concat_tables(facts))
        dups = con.sql("select source_system, block_id, count(*) from (select source_system, block_id, series_key, "
                       "date_key from f group by all having count(*) > 1) group by all order by 3 desc").fetchall()
        bc = table(cat, "build_check")
        con.register("bc", bc)
        ids = ", ".join(f"'{b}'" for b in SLICE)
        rows = con.sql(f"select block_id, row_count, series, rows_geography_unmatched, rows_period_unplaced, "
                       f"strftime(first_date, '%Y-%m'), strftime(last_date, '%Y-%m') from bc where block_id in ({ids}) "
                       f"order by 1").fetchall()
        annotate("notice" if not dups else "warning", "2 grain (series_key x date_key) and build_check of the slice",
                 {"duplicate_keys": dups[:10], "build_check": [list(r) for r in rows]})
        try:
            check_tests(con, cat)
        except Exception as exc:  # reported, the other steps go on
            annotate("error", "3 test SQL failed", {"error": f"{type(exc).__name__}: {str(exc)[:600]}"})
        try:
            published(con, cat)
        except Exception as exc:  # reported
            annotate("error", "4 published check failed", {"error": f"{type(exc).__name__}: {str(exc)[:600]}"})
    finally:
        cleanup(cat)
    return 0


def check_tests(con, cat):
    vr = run_test_sql(con, cat, "metrics_value_range.sql")
    con.register("vr", vr.arrow().read_all())
    vr_sum = con.sql("select concept_key, source_system, count(*), min(value), max(value), any_value(value_min), "
                     "any_value(value_max) from vr group by all order by 3 desc").fetchall()
    gu = run_test_sql(con, cat, "metrics_geography_unmatched.sql")
    con.register("gu", gu.arrow().read_all())
    gu_rows = con.sql("select concept_key, source_system, series_key, count(*) from gu group by all").fetchall()
    annotate("notice", "3 test SQL on the scratch tables",
             {"metrics_value_range_by_concept_source": [[str(x)[:40] for x in r] for r in vr_sum[:25]],
              "metrics_geography_unmatched": [[str(x)[:70] for x in r] for r in gu_rows[:12]]})


def published(con, cat):
    from pyiceberg.expressions import In

    prod = catalog().load_table(("gold", "build_bdl")).scan(
        row_filter=In("block_id", ["P2137", "P2361"]),
        selected_fields=("block_id", "measure_key", "geography_key", "date_key", "value")).to_arrow()
    new = cat.load_table((NS, "build_bdl")).scan(
        row_filter=In("block_id", ["P2137", "P2361"]),
        selected_fields=("block_id", "measure_key", "geography_key", "date_key", "value")).to_arrow()
    con.register("p", prod)
    con.register("n", new)
    diff = con.sql("select coalesce(p.block_id, n.block_id) b, count(*) filter (where n.value is null) gone, "
                   "count(*) filter (where p.value is null) added, count(*) filter (where p.value <> n.value) "
                   "changed, string_agg(distinct case when n.value is null then p.geography_key || ' ' || "
                   "strftime(p.date_key, '%Y') || '=' || p.value end, '; ')[:600] gone_sample from p full join n "
                   "using (measure_key, geography_key, date_key) group by 1").fetchall()
    annotate("notice", "4 published BDL measures vs production build_bdl", [list(r) for r in diff])


def cleanup(cat):
    dropped, failed = 0, []
    for _ in range(4):
        left = list(cat.list_tables(NS))
        if not left:
            break
        for ident in left:
            for wait in (0, 10, 30, 60):  # HTTP 429 from the catalog
                time.sleep(wait)
                try:
                    cat.drop_table(ident)
                    dropped += 1
                    break
                except Exception as exc:  # retried, then reported
                    err = f"{ident[-1]}: {type(exc).__name__}"
            else:
                failed.append(err)
    left = [i[-1] for i in cat.list_tables(NS)]
    annotate("notice" if not left else "warning", "5 scratch dropped",
             {"namespace": NS, "tables": dropped, "left": left[:20], "failed": failed[:5]})


if __name__ == "__main__":
    raise SystemExit(main())
