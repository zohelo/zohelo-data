"""Dev trial of kprm_work_list (PLG-034, #491) on the branch's code. Writes only to zz_dev_kprm_work_list (prefix in both
buckets, catalog namespace) and removes it at the end (finally). Deleted before merge; never on the PR branch.

1. Landing: the shared bulk_file pattern into zz_dev_kprm_work_list/01_landing, twice (second: unchanged), then
   inspect and verify. 2. The landed bytes parsed independently (Python csv) for the reference numbers. 3. Bronze
   (ingestion/to_bronze/kprm_work_list) into the scratch namespace, twice (second: nothing appended), and a profile.
4. dbt, target with schema zz_dev_kprm_work_list (runtime profile), the Bronze source pointed at the scratch
   namespace in this checkout only, unselected refs deferred to a production manifest (read-only): run
   poc_map_geography (with the source's geography macro) into scratch, then build kprm_work_list_drafts,
   government_drafts and poc_build_kprm_work_list with all their tests. 5. Checks and spot values as annotations.
"""

from __future__ import annotations

from collections import Counter
import csv
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

import duckdb
import requests

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.kprm_work_list import load as bronze
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "kprm_work_list"
SCRATCH = "zz_dev_kprm_work_list"  # dbt models (Silver and Gold aliases)
SCRATCH_B = "zz_dev_kprm_work_list_bronze"  # Bronze tables (bronze.kprm_work_list_drafts shares the Silver name)
LAND = f"{SCRATCH}/01_landing"
SPOTS = ["UC2", "UC31", "UD505"]
REUSE = "https://www.gov.pl/web/premier/ponowne-wykorzystywanie"


def sh(cmd: list[str], cwd: Path, env: dict) -> int:
    print("$", " ".join(cmd), flush=True)
    done = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    out = done.stdout + done.stderr
    print(out[-20000:], flush=True)
    summary = [line for line in out.splitlines() if re.search(r"(PASS|FAIL|ERROR|WARN)=|Completed|Done\.", line)]
    annotate("notice" if done.returncode == 0 else "error", f"dbt {cmd[1]} rc={done.returncode}", summary[-6:])
    return done.returncode


def main(argv) -> int:
    client, cat = s3(), bf.retry(catalog, "catalog")
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    status = 0
    try:
        # 1. Landing twice, inspect, verify
        spec = bulk_file.declaration(SOURCE)[0]
        runs = []
        for attempt in (1, 2):
            if attempt == 2:
                time.sleep(5)
            runs.append(bulk_file.load_file(client, landing_bucket, SOURCE, spec, LAND))
        held = bulk_file.landed(client, landing_bucket, bulk_file.prefix(SOURCE, spec["entity"], LAND))
        inspect = bulk_file.main(SOURCE, "inspect", landing_root=LAND)
        verify = bulk_file.main(SOURCE, "verify", landing_root=LAND)
        receipt = held[-1]
        annotate(
            "notice",
            "trial landing",
            {
                "runs": runs,
                "receipts": len(held),
                "url": receipt["source_url"],
                "bytes": receipt["size_bytes"],
                "version": receipt["version_token"],
                "basis": receipt["version_basis"],
                "inspect_rc": inspect,
                "verify_rc": verify,
                "http": receipt["detail"]["http"],
            },
        )

        # 2. Independent parse of the landed bytes (Python csv), reference numbers
        from ingestion.common.bulk_bronze import read_raw

        raw = read_raw(client, landing_bucket, SOURCE, receipt)
        rows = list(csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""), delimiter=";", strict=True))
        header, data = rows[0], rows[1:]
        h = {name: i for i, name in enumerate(header)}
        ref = {
            "bom": raw[:3] == b"\xef\xbb\xbf",
            "rows": len(data),
            "widths": dict(Counter(len(r) for r in data)),
            "header_ok": header == bronze.HEADER,
            "distinct_numbers": len({r[0] for r in data}),
            "per_kind": dict(Counter(r[h["Rodzaj dokumentu"]] for r in data)),
            "per_status": dict(Counter(r[h["Status realizacji"]] or "(empty)" for r in data)),
            "published_min": min(r[h["Data publikacji"]] for r in data),
            "published_max": max(r[h["Data publikacji"]] for r in data),
            "spots": {
                r[0]: [
                    r[h["Rodzaj dokumentu"]],
                    r[h["Status realizacji"]] or "(empty)",
                    r[h["Data publikacji"]],
                    r[h["Tytuł"]][:70],
                ]
                for r in data
                if r[0] in SPOTS
            },
        }
        ref["per_year_status"] = sorted(
            Counter((r[h["Data publikacji"]][:4], r[h["Status realizacji"]] or "(empty)") for r in data).items()
        )
        per_year = Counter(r[h["Data publikacji"]][:4] for r in data)
        ref["per_year"] = dict(sorted(per_year.items()))
        annotate("notice", "trial file (Python csv)", ref)

        # licence statements, read again (one request each, polite)
        for url in (REUSE, "https://www.gov.pl/web/premier/wplip-rm"):
            time.sleep(3)
            page = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
            plain = (
                re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", page.text)).replace("&oacute;", "ó").replace("&nbsp;", " ")
            )
            found = {
                k: (plain[plain.find(k) : plain.find(k) + 330] if k in plain else None)
                for k in ("Informacje sektora publicznego udostępniane na stronie", "Treści tekstowe")
            }
            annotate("notice", f"trial licence {url.rsplit('/', 1)[-1]}", {"status": page.status_code, **found})

        # 3. Bronze twice and a profile
        for _ in (1, 2):
            bronze.load(0, landing_root=LAND, namespace=SCRATCH_B)
        table = cat.load_table((SCRATCH_B, bronze.TABLE_ID[1]))
        files = len(bf.loaded_hashes(table))
        con = duckdb.connect()
        con.register("b", table.scan().to_arrow())
        try:
            rej = cat.load_table((SCRATCH_B, bronze.REJECTED_ID[1])).scan().to_arrow().to_pylist()
        except Exception:
            rej = []
        q = lambda s: con.sql(s).fetchall()  # noqa: E731
        prof = {
            "bronze_files": files,
            "rows": q("select count(*) from b")[0][0],
            "distinct_numbers": q("select count(distinct numer_projektu) from b")[0][0],
            "null_numbers": q("select count(*) from b where numer_projektu is null")[0][0],
            "date_range": q("select min(data_publikacji), max(data_publikacji) from b")[0],
            "nulls": q("select " + ", ".join(f"count(*) - count({c})" for _, c in bronze.KEPT.items()) + " from b")[0],
            "status_values": q("select status_realizacji, count(*) from b group by 1 order by 2 desc"),
            "date_not_pattern": q(
                "select count(*) from b where not regexp_full_match(data_publikacji,"
                " '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}')"
            )[0][0],
            "rejected": [(r.get("entity"), (r.get("error") or "")[:200]) for r in rej],
            "columns": [f.name for f in table.schema().fields],
        }
        annotate("notice", "trial bronze", prof)

        # 4. dbt into scratch, refs deferred to production (read-only)
        wh = ROOT / "warehouse"
        yml = wh / "models/bronze/kprm_work_list.yml"
        yml.write_text(yml.read_text().replace("  schema: bronze\n", f"  schema: {SCRATCH_B}\n"))
        tmp = Path(tempfile.mkdtemp(prefix="kprm-dbt-"))
        prof_text = (wh / "profiles.yml").read_text().replace("schema: zz_dbt_trial", f"schema: {SCRATCH}")
        (tmp / "profiles.yml").write_text(prof_text)
        env = {**os.environ, "DUCKDB_TEMP_DIRECTORY": str(tmp / "duck")}
        dbt = ["dbt"]
        common = ["--profiles-dir", str(tmp)]
        status |= sh(
            [*dbt, "parse", *common, "--target", "prod", "--no-partial-parse", "--target-path", str(tmp / "prod")],
            wh,
            env,
        )
        defer = ["--defer", "--state", str(tmp / "prod")]
        status |= sh(
            [
                *dbt,
                "run",
                *common,
                "--target",
                "trial",
                "--target-path",
                str(tmp / "t1"),
                "--select",
                "poc_map_geography",
                *defer,
            ],
            wh,
            env,
        )
        models = "kprm_work_list_drafts government_drafts poc_build_kprm_work_list"
        trial = [*common, "--target", "trial"]
        status |= sh([*dbt, "run", *trial, "--target-path", str(tmp / "t2"), "--select", models, *defer], wh, env)
        # every test of the three models (incl. tests/kprm_work_list_drafts_complete.sql), after all are built
        status |= sh([*dbt, "test", *trial, "--target-path", str(tmp / "t3"), "--select", models, *defer], wh, env)
        names = {
            "kprm_work_list_drafts": "kprm_work_list_drafts",
            "government_drafts": "government_drafts",
            "poc_build_kprm_work_list": "build_kprm_work_list",
        }  # trial target: tables keep model names
        counts = []
        for attempt in (1, 2):
            if attempt == 2:  # a second build: the same row counts
                status |= sh(
                    [*dbt, "run", *trial, "--target-path", str(tmp / "t4"), "--select", models, *defer], wh, env
                )
            counts.append({t: len(cat.load_table((SCRATCH, t)).scan().to_arrow()) for t in names})
        for t, alias in names.items():
            con.register(alias, cat.load_table((SCRATCH, t)).scan().to_arrow())
        mg = cat.load_table((SCRATCH, "poc_map_geography")).scan().to_arrow()
        con.register("mg", mg)

        # 5. Checks
        s, g = "kprm_work_list_drafts", "build_kprm_work_list"
        silver = {
            "rebuild_counts": counts,
            "dup_numbers": q(f"select count(*) from (select draft_number from {s} group by 1 having count(*) > 1)")[0][
                0
            ],
            "null_keys": q(f"select count(*) from {s} where draft_number is null or raw_sha256 is null")[0][0],
            "published_range": [str(v) for v in q(f"select min(published_at), max(published_at) from {s}")[0]],
            "per_kind": q(f"select kind, count(*) from {s} group by 1 order by 2 desc"),
            "per_status": q(
                f"select status, coalesce(status_published, '(empty)'), count(*) from {s} group by all order by 3 desc"
            ),
            "per_type": q(f"select document_type, count(*) from {s} group by 1 order by 2 desc"),
            "responsible_bodies": q(
                f"select count(distinct responsible_body), count(*) - count(responsible_body) from {s}"
            )[0],
            "spots": q(
                f"select draft_number, kind, status, coalesce(status_published, '(empty)'), "
                f"strftime(published_at, '%Y-%m-%d %H:%M') from {s} where draft_number in "
                f"({', '.join(repr(x) for x in SPOTS)}) order by 1"
            ),
            "entity_vs_silver": q(
                f"select (select count(*) from government_drafts), (select count(distinct draft_number) from government_drafts)"
            )[0],
        }
        annotate("notice", "trial silver", silver)
        annotate(
            "notice",
            "trial number vs kind/type",
            q(
                "select draft_number, kind, document_type, status, strftime(published_at, '%Y-%m-%d') "
                "from kprm_work_list_drafts where kind <> case left(draft_number_prefix, 1) when 'U' then 'Projekty ustaw' "
                "when 'R' then 'Projekty rozporządzeń' when 'I' then 'Projekty innych dokumentów rządowych' end "
                "or split_part(replace(document_type, '–', '-'), ' - ', 1) <> substr(draft_number_prefix, 2)"
            ),
        )
        series = "measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json"
        gold = {
            "rows": q(f"select count(*) from {g}")[0][0],
            "rows_by_measure": q(
                f"select measure_key, frequency, count(*), min(date_key)::varchar, max(date_key)::varchar from {g} group by all order by 1"
            ),
            "dup_series_date": q(
                f"select count(*) from (select {series} k, date_key from {g} group by all having count(*) > 1)"
            )[0][0],
            "unmatched_geo": q(f"select count(*) from {g} where geography_key = '_X'")[0][0],
            "map_rows": q("select source_code, geography_key from mg where source_system = 'kprm_work_list'"),
            "statuses": q(f"select observation_status_key, count(*) from {g} group by 1"),
            "series": q(f"select count(distinct {series}) from {g}")[0][0],
            "zero_or_null": q(f"select count(*) from {g} where value is null or value <= 0")[0][0],
            "annual_total_PL": q(
                f"select period_label, value::int, observation_status_key from {g} where frequency = 'A' and is_total order by 1"
            ),
            "annual_kind_sum_vs_file": q(
                f"select sum(value)::int from {g} where frequency = 'A' and json_extract_string(breakdown_json, '$.kind') <> '_T'"
            )[0][0],
            "monthly_total_sum": q(
                f"select sum(value)::int, max(value)::int, count(*) from {g} where frequency = 'M' and is_total"
            )[0],
            "monthly_body_sum": q(
                f"select sum(value)::int from {g} where frequency = 'M' and json_extract_string(breakdown_json, '$.responsible_body') <> '_T'"
            )[0][0],
            "metrics_rows_per_period_max": q(
                f"select max(n) from (select date_key, count(*) n from {g} where frequency = 'M' and is_total group by 1)"
            )[0][0],
            "status_measure_sum_vs_entries": q(
                f"select frequency, sum(value)::int filter (where measure_key like '%by_current_status'), "
                f"sum(value)::int filter (where measure_key not like '%by_current_status' and is_total) from {g} group by 1"
            ),
            "annual_by_current_status": q(
                f"select period_label, json_extract_string(breakdown_json, '$.current_status'), value::int from {g} "
                f"where measure_key like '%annual_by_current_status' order by 1, 2"
            ),
            "metrics_rows_per_source_area_period_max": q(
                f"select max(n) from (select geography_key, date_key, count(*) n from {g} "
                f"where measure_key = 'kprm_work_list:entries:list_entries:monthly' and is_total and geography_key <> '_X' group by all)"
            )[0][0],
            "value_bound_0_1000_breaches": q(
                f"select count(*) from {g} where frequency = 'M' and is_total and (value < 0 or value > 1000)"
            )[0][0],
        }
        annotate("notice", "trial gold", gold)
        kinds = q(
            f"select json_extract_string(breakdown_json, '$.kind'), sum(value)::int from {g} where frequency = 'A' and json_extract_string(breakdown_json, '$.kind') <> '_T' group by 1 order by 2 desc"
        )
        newest = q(
            f"select period_label, value::int, observation_status_key from {g} where frequency = 'M' and is_total order by date_key desc limit 3"
        )
        annotate(
            "notice",
            "trial spot values",
            {
                "file_rows": ref["rows"],
                "silver_rows": counts[0]["kprm_work_list_drafts"],
                "file_per_kind": ref["per_kind"],
                "gold_annual_per_kind_sum": kinds,
                "file_newest": ref["published_max"],
                "gold_newest_months": newest,
                "file_spots": ref["spots"],
                "silver_spots": silver["spots"],
            },
        )
        # licence catalogue row (seeds of this head)
        con.sql(f"create table src as select * from read_csv('{wh}/seeds/source.csv', all_varchar = true)")
        con.sql(f"create table lic as select * from read_csv('{wh}/seeds/licence.csv', all_varchar = true)")
        annotate(
            "notice",
            "trial licence row",
            q(
                "select s.source_key, s.licence_key, l.name, l.share_alike, l.commercial_ok, l.attribution_text "
                "from src s left join lic l using (licence_key) where s.source_key = 'kprm_work_list'"
            ),
        )
    except Exception as exc:
        annotate("error", "trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
        raise
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for prefix in (SCRATCH, SCRATCH_B):
                for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/"):
                    for obj in page.get("Contents", []):
                        client.delete_object(Bucket=bucket, Key=obj["Key"])
                        removed += 1
        dropped = []
        try:
            for ns in (SCRATCH, SCRATCH_B):
                if (ns,) not in cat.list_namespaces():
                    continue
                for ident in cat.list_tables(ns):
                    cat.drop_table(ident)
                    dropped.append(".".join(ident))
                cat.drop_namespace(ns)
        except Exception as exc:
            print("drop failed", exc)
        left = sum(
            len(p.get("Contents", []))
            for b in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
            for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}")
        )
        annotate(
            "notice", "trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left}
        )
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
