"""Dev check (reads only): are the exact zeros of the Eurostat per-block Gold load published by Eurostat?

For AVIA_TF_ACA and four other zero-heavy datasets of the #2831 trial sample, the same cells are counted
in three places: Eurostat's own bulk file as landed (TSV, every cell classified by its published lexeme:
`:` not available, empty, a zero value, another value, each with its flags), the dataset's Silver file,
and the per-block Gold output (the build stage and the observations projection of
warehouse/observations.py, run on the runner's disk and never uploaded). A handful of zero cells are also
followed by key from the bulk file to Silver and to Gold. Nothing is written to R2: no table, no object.
Deleted before merge.

python -m dev.eurostat_zeros [dataset ...]   (datasets to compare in place of the four picked ones)
"""

from __future__ import annotations

from collections import Counter
import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
import sys
import time

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import archive_key, catalog, req, s3
from ingestion.common.report import annotate
from warehouse import entity
from warehouse import observations as obs

SOURCE = "eurostat"
FIRST = "AVIA_TF_ACA"
# The sample of trial 43293303988 (dev/gold_observations_trial.py at 8dfbc607): 30 random datasets with
# this seed, the named ones, the two largest up to 150 M Silver rows, and the flagged and projection
# datasets its body names.
SEED, RANDOM = 20261112, 30
NAMED = ["UNE_RT_M", "NAMA_10_GDP", "PRC_HICP_MINR", "DEMO_PJAN", "NAMQ_10_GDP", "DEMO_R_PJANAGGR3", "EARN_MW_CUR",
         "NAIO_10_FCP_U3", "AVIA_TF_ACA", "RAIL_IF_LINE_NA", "ROAD_EC_EMPL", "PROJ_STP22", "PROJ_STP25"]
OTHERS = 4
LANDING_PREFIX = "01_landing/eurostat/snapshot/data/"
PERIOD = [(re.compile(r"^(\d{4})-?Q([1-4])$"), r"\1-Q\2"), (re.compile(r"^(\d{4})-?S([12])$"), r"\1-S\2"),
          (re.compile(r"^(\d{4})-?W(\d{2})$"), r"\1-W\2"), (re.compile(r"^(\d{4})M(\d{2})$"), r"\1-\2")]


def connect(tmp: str, remote: bool = False):
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '8GB'; SET preserve_insertion_order = false; SET temp_directory = '{tmp}'")
    if remote:
        con.execute("INSTALL httpfs; LOAD httpfs")
        endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
        con.execute(
            "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
            [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
        )
    return con


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + str(p).replace("'", "''") + "'" for p in paths) + "])"


def classify(cell: bytes) -> tuple[str, str, str]:
    """(class, flags, value lexeme) of one bulk-file cell, split as the Bronze decoder splits it: the
    first run of non-space characters is the value, the rest the flags."""
    tokens = cell.decode("utf-8").split(None, 1)
    value = tokens[0] if tokens else ""
    flags = tokens[1].strip() if len(tokens) > 1 else ""
    if value == ":":
        return "not_available", flags, value
    if value == "":
        return "empty", flags, value
    try:
        number = float(value)
    except ValueError:
        return "not_a_number", flags, value
    return ("zero" if number == 0 else "value"), flags, value


def count_bulk(path: Path, want_samples: int = 2) -> dict:
    """Every cell of the bulk file by class and flags, the zero lexemes as published, and a few zero and
    not-available cells with their series key and period (to follow into Silver and Gold)."""
    classes: Counter = Counter()
    zero_lexemes: Counter = Counter()
    known: dict[bytes, tuple[str, str, str]] = {}
    samples: dict[str, list] = {"zero": [], "zero_flagged": [], "not_available": []}
    series = cells = 0
    with gzip.open(path, "rb") as stream:
        header = stream.readline().decode("utf-8-sig").rstrip("\r\n").split("\t")
        dimensions = [d.strip().lower() for d in header[0].split("\\")[0].split(",")]
        periods = [p.strip() for p in header[1:]]
        for raw in stream:
            parts = raw.rstrip(b"\r\n").split(b"\t")
            if len(parts) < 2:
                continue
            series += 1
            cells += len(parts) - 1
            row = Counter(parts[1:])
            for cell, n in row.items():
                got = known.get(cell)
                if got is None:
                    got = classify(cell)
                    if len(known) < 500_000:
                        known[cell] = got
                kind, flags, value = got
                classes[(kind, flags)] += n
                if kind == "zero":
                    zero_lexemes[value] += n
                wanted = "zero_flagged" if kind == "zero" and flags else kind
                # a spread of series: series 4, 8, 16, … (any file size), one cell of a kind per series
                if wanted in samples and len(samples[wanted]) < want_samples and series > 2 and series & (series - 1) == 0:
                    at = parts.index(cell, 1)
                    samples[wanted].append(
                        {"key": parts[0].decode("utf-8").strip(), "period": periods[at - 1], "cell": cell.decode("utf-8")}
                    )
    by_class: dict[str, int] = Counter()
    for (kind, _), n in classes.items():
        by_class[kind] += n
    return {
        "dimensions": dimensions,
        "series": series,
        "periods": len(periods),
        "cells": cells,
        "by_class": dict(by_class),
        "zero_by_flags": {f or "(none)": n for (k, f), n in sorted(classes.items(), key=lambda i: -i[1]) if k == "zero"},
        "not_available_by_flags": dict(
            Counter({f or "(none)": n for (k, f), n in classes.items() if k == "not_available"}).most_common(6)
        ),
        "zero_lexemes": dict(zero_lexemes.most_common(6)),
        "samples": samples,
    }


def fetch_bulk(client, digest: str, dataset_ids: list[str], target: Path) -> str:
    """The landed bulk file with this SHA-256: the Archive copy, else the file still in Landing."""
    import botocore.exceptions

    lake, landing = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    places = [(lake, archive_key(SOURCE, digest, "x.tsv.gz"))]
    for dataset_id in dataset_ids:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing, Prefix=f"{LANDING_PREFIX}{dataset_id}/"):
            places += [(landing, o["Key"]) for o in page.get("Contents", []) if o["Key"].endswith(".tsv.gz")]
    for bucket, key in places:
        try:
            client.download_file(bucket, key, str(target))
        except botocore.exceptions.ClientError:
            continue
        held = sha256()
        with target.open("rb") as handle:
            for block in iter(lambda: handle.read(8 << 20), b""):
                held.update(block)
        if held.hexdigest() == digest:
            return key.split("/")[0] + "/…/" + key.rsplit("/", 1)[-1][:24]
        target.unlink()
    raise RuntimeError(f"no landed file with SHA-256 {digest[:12]} in Archive or Landing")


def silver_identity(dimensions: list[str], key: str, period: str) -> dict:
    """The Silver columns of one bulk-file cell (the rule of warehouse/entity/eurostat_observations.sql)."""
    codes = dict(zip(dimensions, [c.strip() for c in key.split(",")], strict=True))
    for pattern, shape in PERIOD:
        if pattern.match(period):
            period = pattern.sub(shape, period)
            break
    return {
        "freq": codes.get("freq"),
        "unit": codes.get("unit"),
        "geo": codes.get("geo"),
        "codes": ",".join(c for d, c in codes.items() if d not in ("freq", "unit", "geo")),
        "period": period,
    }


def follow(con, silver: str, gold: str, dimensions: list[str], samples: dict) -> list:
    """Each sampled bulk-file cell as Silver and Gold hold it."""
    out = []
    for kind, cells in samples.items():
        for cell in cells:
            who = silver_identity(dimensions, cell["key"], cell["period"])
            held = con.execute(
                f"""select value, status_code from {silver} where freq_code is not distinct from ? and
                    unit_code is not distinct from ? and geo_code is not distinct from ? and breakdown_codes = ?
                    and time_period = ?""",
                [who["freq"], who["unit"], who["geo"], who["codes"], who["period"]],
            ).fetchall()
            wanted = sorted(c for c in who["codes"].split(",") if c)
            narrow = " and ".join(["true", *(f"contains(measure_key || breakdown_json, '{c}')" for c in wanted if "'" not in c)])
            rows = con.execute(
                f"""select value, observation_status_key, measure_codes, breakdown_json from {gold}
                    where frequency is not distinct from ? and unit_code = coalesce(?, '_Z')
                    and geography_source_code is not distinct from ? and period_label = ? and {narrow}""",
                [who["freq"], who["unit"], who["geo"], who["period"]],
            ).fetchall()
            exact = [
                [value, status]
                for value, status, measure_codes, breakdown in rows
                if sorted([c for c in measure_codes.split(":") if c] + list(json.loads(breakdown).values())) == wanted
            ]
            out.append({"bulk": [kind, cell["key"], cell["period"], cell["cell"]], "silver": held, "gold": exact})
    return out


def one(cat_lookups: dict, client, dataset: str, held: dict, tmp: Path) -> dict:
    """One dataset: its Silver file and its landed bulk file are downloaded, then compared."""
    work = tmp / dataset
    work.mkdir(parents=True)
    try:
        silver_files = []
        for uri in held["paths"]:
            bucket, key = uri.removeprefix("s3://").split("/", 1)
            silver_files.append(work / f"silver{len(silver_files)}.parquet")
            client.download_file(bucket, key, str(silver_files[-1]))
        con = connect(str(work))
        digests = [r[0] for r in con.execute(f"select distinct raw_sha256 from {files(silver_files)}").fetchall()]
        con.close()
        if len(digests) != 1:
            raise RuntimeError(f"{dataset}: Silver holds {len(digests)} bulk files")
        bulk_file = work / "bulk.tsv.gz"
        place = fetch_bulk(client, digests[0], sorted({str(e) for e in held["entities"]}), bulk_file)
        return {"bulk_from": place, **compare(cat_lookups, dataset, silver_files, bulk_file, held["rows"], work)}
    finally:
        shutil.rmtree(work, ignore_errors=True)


def compare(lookups: dict, dataset: str, silver_files: list[Path], bulk_file: Path, silver_rows: int, work: Path) -> dict:
    """Bulk file, Silver file and the per-block Gold output of one dataset, counted the same way."""
    started = time.monotonic()
    con = connect(str(work))
    silver = files(silver_files)
    rows, valued, zeros, version = con.execute(
        f"select count(*), count(value), count(*) filter (where value = 0), any_value(source_version) from {silver}"
    ).fetchone()
    out: dict = {
        "dataset": dataset,
        "source_version": version,
        "silver": {
            "rows": rows,
            "with_value": valued,
            "no_value": rows - valued,
            "zeros": zeros,
            "zeros_by_flags": dict(con.execute(
                f"""select coalesce(nullif(trim(status_code), ''), '(none)'), count(*) from {silver}
                    where value = 0 group by 1 order by 2 desc"""
            ).fetchall()),
            "mb": round(sum(f.stat().st_size for f in silver_files) / 1e6, 1),
        },
    }
    out["bulk_mb"] = round(bulk_file.stat().st_size / 1e6, 1)
    bulk = count_bulk(bulk_file)
    bulk_file.unlink()
    out["bulk"] = {k: v for k, v in bulk.items() if k not in ("samples", "dimensions")}
    out["bulk_seconds"] = round(time.monotonic() - started)

    # Gold as warehouse/observations.py `work` makes it, without the upload
    memory = "10GB" if silver_rows > obs.BIG_ROWS else "4GB"
    build, gold_file = work / "build.parquet", work / "observations.parquet"
    spec = obs.SOURCES[SOURCE]
    entity.apply(
        spec["build"], [str(f) for f in silver_files], build,
        not_null=("measure_key", "geography_key", "date_key", "frequency", "breakdown_json", "is_total"),
        same_rows=False, lookups=lookups, memory_limit=memory, temp_directory=str(work),
        source_view="silver", row_group_size=obs.ROW_GROUP, prepare=spec.get("prepare"),
    )
    table = obs.TABLES["observations"]
    report = entity.apply(
        table["model"], str(build), gold_file, not_null=table["not_null"], same_rows=True,
        unique_key=table["unique_key"], memory_limit=memory, temp_directory=str(work), source_view="build",
        row_group_size=obs.ROW_GROUP,
    )
    gold, built = files([gold_file]), files([build])
    g_rows, g_zeros, g_series, g_zero_series = con.execute(
        f"""select sum(n), sum(z), count(*), count(*) filter (where z = n) from
            (select series_key, count(*) as n, count(*) filter (where value = 0) as z from {gold} group by 1)"""
    ).fetchone()
    lean = work / "without_zeros.parquet"
    con.execute(
        f"COPY (select * from {gold} where value <> 0) TO '{lean}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {obs.ROW_GROUP})"
    )
    out["gold"] = {
        "rows": g_rows,
        "zeros": g_zeros,
        "zeros_by_status": dict(con.execute(
            f"select observation_status_key, count(*) from {gold} where value = 0 group by 1 order by 2 desc"
        ).fetchall()),
        "series": g_series,
        "series_only_zeros": g_zero_series,
        "observations_mb": round(report["bytes"] / 1e6, 1),
        "observations_mb_without_zeros": round(lean.stat().st_size / 1e6, 1),
    }
    out["followed"] = follow(con, silver, built, bulk["dimensions"], bulk["samples"])
    b = bulk["by_class"]
    out["equal"] = {
        "cells = silver rows": bulk["cells"] == rows,
        "bulk zeros = silver zeros": b.get("zero", 0) == zeros,
        "bulk no value = silver no value": b.get("not_available", 0) + b.get("empty", 0) == rows - valued,
        # rows with a value that Gold does not load have a period the calendar cannot place
        "silver with value - gold rows": valued - g_rows,
        "silver zeros - gold zeros": zeros - g_zeros,
        "bulk not a number": b.get("not_a_number", 0),
    }
    con.close()
    out["seconds"] = round(time.monotonic() - started)
    return out


def sample_zeros(tmp: str, silver: dict) -> list:
    """Rows, rows with a value and exact zeros of every dataset of the trial sample, from Silver (two
    columns read)."""
    upper = {e.upper(): e for e in silver}
    picked = set(random.Random(SEED).sample(sorted(silver), min(RANDOM, len(silver))))
    picked |= {upper[b] for b in NAMED if b in upper}
    picked |= set([e for e in sorted(silver, key=lambda e: -silver[e]["rows"]) if silver[e]["rows"] <= 150_000_000][:2])
    con = connect(tmp, remote=True)
    out = []
    for e in sorted(picked):
        valued, zeros = con.execute(
            f"select count(value), count(*) filter (where value = 0) from {files(silver[e]['paths'])}"
        ).fetchone()
        out.append([e.upper(), silver[e]["rows"], valued, zeros])
    con.close()
    return sorted(out, key=lambda r: -r[3])


def main() -> None:
    tmp = "/mnt/duckdb_tmp"
    try:
        subprocess.run(["sudo", "mkdir", "-p", tmp], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), tmp], check=True)
    except Exception:
        tmp = "/tmp/duckdb_tmp"
        Path(tmp).mkdir(parents=True, exist_ok=True)
    cat, client = catalog(), s3()
    silver_table = cat.load_table(obs.SOURCES[SOURCE]["silver"])
    silver = et.state(silver_table, "dataset_id")
    table = sample_zeros(tmp, silver)
    total = [sum(r[i] for r in table) for i in (1, 2, 3)]
    annotate("notice", "eurostat zeros: trial sample by dataset", {
        "columns": ["dataset", "silver_rows", "with_value", "zeros"],
        "datasets": len(table), "total": total, "trial_43293303988": {"rows": 708_671_317, "zeros": 559_352_315},
        "top": table[:22],
    })
    blocks: dict[str, dict] = {}
    for e, held in silver.items():
        if e is None:
            continue
        item = blocks.setdefault(str(e).upper(), {"paths": [], "rows": 0, "entities": []})
        item["paths"] += held["paths"]
        item["rows"] += held["rows"]
        item["entities"].append(e)
    if len(sys.argv) > 1:
        others = [a.upper() for a in sys.argv[1:]]
    else:
        others, families = [], {FIRST.split("_")[0]}
        for dataset, _, _, zeros in table:  # most zeros first, one dataset per family (AVIA, NAIO, …)
            family = dataset.split("_")[0]
            if dataset != FIRST and zeros and family not in families and len(others) < OTHERS:
                others.append(dataset)
                families.add(family)
    lookup_dir = Path(tmp) / "lookups"
    lookup_dir.mkdir(parents=True, exist_ok=True)
    lookups, _ = obs.export_lookups(cat, SOURCE, lookup_dir)
    results = []
    # small ones first: their evidence is out before the largest dataset starts
    for dataset in [*sorted(others, key=lambda d: blocks[d]["rows"]), FIRST]:
        try:
            result = one(lookups, client, dataset, blocks[dataset], Path(tmp))
        except Exception as exc:
            result = {"dataset": dataset, "error": f"{type(exc).__name__}: {str(exc)[:600]}"}
        results.append(result)
        annotate("notice" if "error" not in result and all(
            v is True or v == 0 for v in result["equal"].values()
        ) else "warning", f"eurostat zeros: {dataset}", result)
    out_dir = Path(os.environ.get("DEV_OUTPUT_DIR", tmp))
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "eurostat_zeros.json").write_text(json.dumps({"sample": table, "datasets": results}, default=str, indent=1))
    annotate("notice", "eurostat zeros: summary", {
        "datasets": [
            [r["dataset"], r.get("bulk", {}).get("by_class"), r.get("silver", {}).get("zeros"), r.get("gold", {}).get("zeros"),
             r.get("gold", {}).get("observations_mb"), r.get("gold", {}).get("observations_mb_without_zeros"),
             r.get("equal") or r.get("error")]
            for r in results
        ],
        "writes_to_r2": 0,
    })
    shutil.rmtree(lookup_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
