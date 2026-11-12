"""Dev trial (scratch only): Gold observations of a sample of Eurostat datasets, built by
warehouse/observations.py into the namespace and prefix zz_dev_gold_coverage, checked, then removed.
Reads production Silver and gold.map_geography; writes nothing outside the scratch. Deleted before merge.

python -m dev.gold_observations_trial [random sample size, default 300] [minutes, default 120] [keep]
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
import random
import subprocess
import sys
import time

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import observations as obs

NS = "zz_dev_gold_coverage"
SOURCE = "eurostat"
ALWAYS = ["UNE_RT_M", "NAMA_10_GDP", "PRC_HICP_MINR", "DEMO_PJAN", "NAMQ_10_GDP", "DEMO_R_PJANAGGR3", "EARN_MW_CUR"]
SPOT = {  # series of the rule-based layer whose newest PL value is reported, with the curated fact's value
    "UNE_RT_M": "measure_key = 'eurostat:UNE_RT_M:PC_ACT:SA@M' and breakdown_json like '%\"age\":\"TOTAL\"%' and sex_key = '_T'",
    "NAMA_10_GDP": "measure_key = 'eurostat:NAMA_10_GDP:CP_MEUR:B1GQ@A'",
    "PRC_HICP_MINR": "measure_key = 'eurostat:PRC_HICP_MINR:RCH_A@M' and breakdown_json like '%\"TOTAL\"%'",
    "DEMO_PJAN": "is_total",
}


def connect(tmp: str):
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '9GB'; SET preserve_insertion_order = false; SET temp_directory = '{tmp}'")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    return con


def paths_of(cat, name: str) -> list[str]:
    table = cat.load_table((NS, f"{SOURCE}_{name}"))
    return [p for held in et.state(table, "block_id").values() for p in held["paths"]]


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
    for name in [*(f"{SOURCE}_{t}" for t in obs.TABLES), obs.REJECTED[1]]:
        with contextlib.suppress(Exception):
            cat.drop_table((NS, name))
            gone["tables"] += 1
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            gone["objects"] += len(keys)
    left = sum(len(p.get("Contents", [])) for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/"))
    return {**gone, "objects_left": left, "namespace_left": (NS,) in cat.list_namespaces()}


def checks(cat, con, sample: list[str], silver_paths: list[str]) -> dict:
    o, s, m = (files(paths_of(cat, n)) for n in ("observations", "series", "measures"))
    one = lambda q: con.execute(q).fetchone()  # noqa: E731
    out: dict = {}
    out["rows"], out["series_keys"], out["blocks"] = one(f"select count(*), count(distinct series_key), count(distinct block_id) from {o}")
    # the grain exactly as tests/fact_grain.sql groups it: series_key × date_key over the whole table
    out["duplicate_keys"] = one(
        f"select count(*) from (select 1 from {o} group by series_key, date_key having count(*) > 1)"
    )[0]
    out["series_rows"], out["series_points"] = one(f"select count(*), sum(points) from {s}")
    out["measures"], out["measure_points"] = one(f"select count(*), sum(points) from {m}")
    out["null_keys"] = one(
        f"select count(*) from {o} where series_key is null or measure_key is null or geography_key is null or date_key is null"
    )[0]
    out["by_frequency"] = con.execute(
        f"select frequency, count(*), min(date_key)::varchar, max(date_key)::varchar from {o} group by 1 order by 2 desc"
    ).fetchall()
    out["status"] = con.execute(f"select observation_status_key, count(*) from {o} group by 1 order by 2 desc limit 6").fetchall()
    out["geo"] = con.execute(
        f"""select case when geography_key = '_X' then '_X' when geography_key = '_Z' then '_Z'
                when geography_key like 'TERC:%' then 'TERC' when geography_key like 'NUTS:%' then 'NUTS'
                when geography_key like 'EUROSTAT:%' then 'aggregate' else 'country' end, count(*)
            from {o} group by 1 order by 2 desc"""
    ).fetchall()
    out["unmatched_series"], out["unmatched_codes"] = one(
        f"select count(*), count(distinct geography_source_code) from {s} where geography_key = '_X'"
    )
    out["unmatched_top"] = con.execute(
        f"select geography_source_code, sum(points) from {s} where geography_key = '_X' group by 1 order by 2 desc limit 25"
    ).fetchall()
    out["total_series"], out["blocks_with_total"] = one(
        f"select count(*) filter (where is_total), count(distinct block_id) filter (where is_total) from {s}"
    )
    out["measures_per_block"] = con.execute(
        f"select min(n), median(n), max(n) from (select block_id, count(*) as n from {m} group by 1)"
    ).fetchone()
    out["multi_frequency_blocks"] = one(
        f"select count(*) from (select block_id from {m} group by 1 having count(distinct frequency) > 1)"
    )[0]
    # row counts against the Silver source: rows with a value in the sampled datasets' Silver files
    out["silver_valued_rows"] = one(f"select count(*) from {files(silver_paths)} where value is not null")[0]
    out["silver_rows_not_loaded"] = out["silver_valued_rows"] - out["rows"]
    out["mb"] = {
        n: round(sum(h["bytes"] for h in et.state(cat.load_table((NS, f"{SOURCE}_{n}")), "block_id").values()) / 1e6, 1)
        for n in obs.TABLES
    }
    spot = {}
    for block, where in SPOT.items():
        row = con.execute(
            f"""select o.period_label, o.value, o.series_key from {o} as o
                where o.series_key in (select series_key from {s} where block_id = '{block}' and geography_key = 'PL' and {where})
                order by o.date_key desc, o.series_key limit 1"""
        ).fetchone()
        spot[block] = list(row) if row else None
    out["spot_pl_newest"] = spot
    return out


def curated(cat) -> dict:
    """The curated facts' newest PL values for the same series (production Gold, read-only)."""
    wanted = {
        "UNE_RT_M": ("fact_monthly", "eurostat:UNE_RT_M:PC_ACT:TOTAL"),
        "NAMA_10_GDP": ("fact_annual", "eurostat:NAMA_10_GDP:CP_MEUR:B1GQ"),
        "PRC_HICP_MINR": ("fact_monthly", "eurostat:PRC_HICP_MINR:RCH_A"),
        "DEMO_PJAN": ("fact_annual", "eurostat:DEMO_PJAN:NR"),
    }
    from pyiceberg.expressions import And, EqualTo, StartsWith

    out = {}
    for block, (table, measure) in wanted.items():
        try:
            rows = (
                cat.load_table(("gold", table))
                .scan(row_filter=And(StartsWith("measure_key", measure), EqualTo("geography_key", "PL")),
                      selected_fields=("series_key", "period_label", "value", "date_key"))
                .to_arrow()
                .to_pylist()
            )
            newest = max(r["date_key"] for r in rows)
            out[block] = sorted([r["period_label"], r["value"], r["series_key"]] for r in rows if r["date_key"] == newest)[:3]
        except Exception as exc:
            out[block] = f"{type(exc).__name__}: {str(exc)[:80]}"
    return out


def one(block: str) -> None:
    """One block built in this process (no pool), its result printed: run as a subprocess by `diagnose`."""
    import json
    import tempfile

    cat = catalog()
    tmp = os.environ["DUCKDB_TEMP_DIRECTORY"]
    with tempfile.TemporaryDirectory(dir=tmp) as directory:
        lookups, keys = obs.export_lookups(cat, SOURCE, Path(directory))
        silver = cat.load_table(obs.SOURCES[SOURCE]["silver"])
        item = obs.blocks_of(SOURCE, et.state(silver, "dataset_id"))[block]
        result = obs.work(SOURCE, {"block": block, "recipe": "diagnose", **item}, lookups, "4GB", tmp, f"{NS}/gold/")
    print("RESULT " + json.dumps(result, default=str), flush=True)


def diagnose(blocks: list[str]) -> None:
    """Each block in its own subprocess, with the exit code, the end of stderr and the kernel's kill lines."""
    out = {}
    for block in blocks:
        started = time.monotonic()
        done = subprocess.run([sys.executable, "-m", "dev.gold_observations_trial", "one", block],
                              capture_output=True, text=True, timeout=1500)
        result = [line[7:400] for line in done.stdout.splitlines() if line.startswith("RESULT ")]
        out[block] = {"exit": done.returncode, "seconds": round(time.monotonic() - started), "result": result[:1],
                      "stderr": done.stderr[-500:] if done.returncode else ""}
    kernel = subprocess.run("sudo dmesg | grep -iE 'killed process|out of memory|segfault' | tail -4",
                            shell=True, capture_output=True, text=True).stdout[-600:]
    annotate("notice", "gold observations trial diagnose", {"blocks": out, "kernel": kernel})


def main() -> None:
    if len(sys.argv) > 2 and sys.argv[1] == "one":
        os.environ.setdefault("DUCKDB_TEMP_DIRECTORY", "/mnt/duckdb_tmp")
        one(sys.argv[2])
        return
    size = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    minutes = float(sys.argv[2]) if len(sys.argv) > 2 else 120
    tmp = "/mnt/duckdb_tmp"
    try:
        subprocess.run(["sudo", "mkdir", "-p", tmp], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), tmp], check=True)
    except Exception:
        tmp = "/tmp/duckdb_tmp"
        Path(tmp).mkdir(parents=True, exist_ok=True)
    os.environ["DUCKDB_TEMP_DIRECTORY"] = tmp
    cat, client = catalog(), s3()
    annotate("notice", "trial cleanup before", cleanup(cat, client))  # leftovers of a killed earlier run
    try:
        diagnose(["TIN00092", "UNE_RT_M", "NAMA_10_GDP", "DEMO_PJAN"])
        silver = et.state(cat.load_table(obs.SOURCES[SOURCE]["silver"]), "dataset_id")
        by_size = sorted(silver, key=lambda e: -silver[e]["rows"])
        upper = {e.upper(): e for e in silver}
        picked = set(random.Random(20261112).sample(sorted(silver), min(size, len(silver))))
        picked |= {upper[b] for b in ALWAYS if b in upper}
        # large datasets: memory and time of a heavy case (the very largest are named in the plan, not built)
        picked |= set([e for e in by_size if silver[e]["rows"] <= 150_000_000][:2])
        sample = sorted(e.upper() for e in picked)
        silver_paths = [p for e in picked for p in silver[e]["paths"]]
        plan = {
            "datasets_in_silver": len(silver),
            "sample": len(sample),
            "sample_silver_rows": sum(silver[e]["rows"] for e in picked),
            "sample_silver_mb": round(sum(silver[e]["bytes"] for e in picked) / 1e6),
            "all_silver_rows": sum(h["rows"] for h in silver.values()),
            "all_silver_mb": round(sum(h["bytes"] for h in silver.values()) / 1e6),
            "largest": [[e, silver[e]["rows"], round(silver[e]["bytes"] / 1e6)] for e in by_size[:4]],
            "largest_built": [e for e in by_size if silver[e]["rows"] <= 150_000_000][:2],
        }
        args = ["--source", SOURCE, "--trial", NS, "--minutes", str(minutes), "--only", *sample]
        started = time.monotonic()
        first = obs.main(args)
        build_minutes = round((time.monotonic() - started) / 60, 1)
        started = time.monotonic()
        second = obs.main([*args[:5], "5", *args[6:]])  # the same again: nothing to build
        con = connect(tmp)
        report = {"plan": plan, "exit": [first, second], "build_minutes": build_minutes,
                  "second_run_seconds": round(time.monotonic() - started), **checks(cat, con, sample, silver_paths)}
        report["curated_pl_newest"] = curated(cat)
        head = {k: v for k, v in report.items() if k not in ("unmatched_top", "spot_pl_newest", "curated_pl_newest", "status")}
        annotate("notice", "gold observations trial", head)
        annotate("notice", "gold observations trial values", {k: report[k] for k in ("spot_pl_newest", "curated_pl_newest", "unmatched_top", "status")})
        rejected = cat.load_table((NS, obs.REJECTED[1])).scan().to_arrow().to_pylist() if (NS, obs.REJECTED[1]) in cat.list_tables(NS) else []
        annotate("notice", "gold observations trial rejected", {"count": len(rejected), "sample": [[r["entity"], r["error"][:150]] for r in rejected[:15]]})
    finally:
        if "keep" not in sys.argv:
            annotate("notice", "trial cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
