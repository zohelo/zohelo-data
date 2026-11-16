"""Dev trial for #2976 / PR #3128 rework (TED notices of Polish buyers; test branch only, never on main).

Landing -> Bronze -> Silver -> Gold into scratch, Silver and Gold through real dbt. Landing: the whole window
(2025-01-01 .. yesterday UTC) into the Landing prefix zz_dev_ted_notices/ (pass 1), pass 2 must land nothing; then
a multi-page day loses its page 1 in scratch (as if the run had stopped before writing it) and pass 3 must fetch
that day again, writing page 1 only. Bronze: Iceberg namespace zz_dev_ted_notices, lake prefix zz_dev_ted_notices/,
twice (the second appends nothing). dbt: `dbt build` of silver ted_notices and the Gold models it feeds
(poc_map_geography and its Gold-folder parents, poc_build_ted_notices, ted_notice, the facts, dim_series and every
metrics model tests/metrics_value_range.sql reads) with target prod, var gold_schema = zz_dev_ted_notices, the other
Gold blocks declared waiting (var waiting_blocks) and, in this runner's checkout only, the Bronze source and the
Silver model's namespace pointed at zz_dev_ted_notices; a guard refuses to run when a selected node would land
outside zz_dev_. Built twice. Other sources' Silver tables are read from production, never written.
Everything written is removed in `finally`.
"""

from __future__ import annotations

from collections import Counter
from datetime import date
import json
import os
from pathlib import Path
import subprocess
import time
import traceback

import duckdb
import yaml

from ingestion.common import report as report_module

SCRATCH = "zz_dev_ted_notices"
ROOT = Path(__file__).resolve().parents[1]
WH = ROOT / "warehouse"
COLLECTED: dict[str, list] = {}
OURS = ("ted_notices", "poc_build_ted_notices", "ted_notice", "poc_metrics_procurement_notices", "poc_map_geography")


def collect(level: str, title: str, data) -> None:
    """Annotations of the activities, gathered into the trial's own few annotations."""
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


ANNOTATE = report_module.annotate
report_module.annotate = collect


def note(title: str, data, level: str = "notice") -> None:
    ANNOTATE(level, title, data)


def q(con, sql: str, limit: int = 60) -> list:
    rows = con.execute(sql).fetchall()[:limit]
    return [[v if isinstance(v, int | float) or v is None else str(v) for v in r] for r in rows]


# ---------------------------------------------------------------- Landing


def precheck(landing) -> bool:
    import requests

    session = requests.Session()
    session.headers.update({"User-Agent": landing.USER_AGENT, "Accept": "application/json"})
    body = {**landing.request_body(date(2026, 11, 10), 1), "limit": 2}
    try:
        answer = json.loads(landing.post(session, body))
    except RuntimeError as exc:
        note("ted trial precheck failed", {"fields": landing.FIELDS, "error": str(exc)[:1500]}, "error")
        return False
    keys = sorted({k for n in answer["notices"] for k in n})
    note(
        "ted trial precheck",
        {
            "fields_asked": len(landing.FIELDS),
            "keys_answered": keys,
            "buyer_name_answered": "buyer-name" in keys,
            "total_2026_11_10": answer.get("totalNoticeCount"),
        },
    )
    return "buyer-name" not in keys


def landing_steps(result: dict) -> dict:
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.ted_notices import extract as landing

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    last = landing.last_day()
    started = time.monotonic()
    result["exit_pass1_pass2"] = [landing.extract(300, landing_root=SCRATCH, last=last) for _ in (1, 2)]
    result["minutes_pass1_pass2"] = round((time.monotonic() - started) / 60, 1)
    loads = [
        {k: v for k, v in r.items() if k not in ("level", "failed_sample")}
        for r in COLLECTED.get("ted_notices landing load", [])
    ]
    result["pass1"], result["pass2"] = loads[0], loads[1]
    # partial day: remove page 1 (data and receipt, scratch only) of the first day with 2+ pages
    receipts, data = landing.listing(client, bucket, SCRATCH)
    multi = sorted(e for e in receipts if e.endswith("_p2"))
    partial: dict = {}
    if multi:
        day_entity = multi[0].replace("_p2", "_p1")
        keys = [k for _, k in receipts[day_entity]]
        keys += [k[: -len(".receipt.json")] + ".json" for k in keys]
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]})
        landing.inspect(landing_root=SCRATCH, last=last)
        partial["entity_removed"] = day_entity
        partial["inspect_after_removal"] = {
            k: v
            for k, v in COLLECTED["ted_notices landing inspect"][-1].items()
            if k in ("days_missing", "days_partial", "days_current")
        }
        partial["exit_pass3"] = landing.extract(300, landing_root=SCRATCH, last=last)
        p3 = COLLECTED["ted_notices landing load"][-1]
        partial["pass3"] = {
            k: p3.get(k)
            for k in (
                "days_missing_before",
                "days_partial_before",
                "days_done",
                "pages_landed_new",
                "pages_landed_changed",
                "pages_unchanged",
                "failed_days",
            )
        }
        landing.inspect(landing_root=SCRATCH, last=last)
        partial["inspect_after_pass3"] = {
            k: v
            for k, v in COLLECTED["ted_notices landing inspect"][-1].items()
            if k in ("days_missing", "days_partial", "days_current", "days_in_window")
        }
    result["partial_day_test"] = partial
    result["verify_exit"] = landing.verify(landing_root=SCRATCH)
    v = COLLECTED["ted_notices landing verify"][-1]
    result["verify"] = {k: v.get(k) for k in ("receipts", "entities", "days", "first_last_day", "missing_data")}
    receipts, data = landing.listing(client, bucket, SCRATCH)
    held = landing.read_receipts(client, bucket, [rows[-1][1] for rows in receipts.values()])
    result["held"] = {
        "entities": len(receipts),
        "receipts": sum(len(v) for v in receipts.values()),
        "data_files": len(data),
        "bytes_newest": sum(r["size_bytes"] for r in held),
        "entities_with_two_copies": sorted(e for e, k in receipts.items() if len(k) > 1)[:6],
        "days": len({r["publication_day"] for r in held}),
        "days_empty": len({r["publication_day"] for r in held if r["day_total_notices"] == 0}),
        "pages_per_day": dict(Counter(r["part"]["total"] for r in held if r["part"]["index"] == 1)),
        "notices_by_receipts": sum(r["day_total_notices"] for r in held if r["part"]["index"] == 1),
        "first_last_day": [min(r["publication_day"] for r in held), max(r["publication_day"] for r in held)],
        "fields_in_request": len(held[0]["request"]["fields"]),
    }
    return {"client": client, "bucket": bucket, "receipts": receipts}


# ---------------------------------------------------------------- dbt


def patch_checkout() -> None:
    """This runner's checkout only: Bronze source and the Silver model in the scratch namespace."""
    src = WH / "models/bronze/ted_notices.yml"
    text = src.read_text()
    assert text.count("  schema: bronze\n") == 1
    src.write_text(text.replace("  schema: bronze\n", f"  schema: {SCRATCH}\n"))
    project = yaml.safe_load((WH / "dbt_project.yml").read_text())
    project["models"]["zohelo"]["silver"]["ted_notices"] = {"+schema": SCRATCH}
    (WH / "dbt_project.yml").write_text(yaml.safe_dump(project, sort_keys=False))


def dbt(args: list[str], variables: dict) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(WH),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DUCKDB_TEMP_DIRECTORY": "/tmp/duckdb_tmp",
    }
    cmd = ["dbt", *args, "--target", "prod", "--vars", json.dumps(variables)]
    return subprocess.run(cmd, cwd=WH, env=env, capture_output=True, text=True)


def selection(variables: dict) -> tuple[list[str], list[str]]:
    run = dbt(["parse", "--no-partial-parse"], variables)
    if run.returncode:
        raise RuntimeError("dbt parse: " + run.stdout[-1500:])
    manifest = json.loads((WH / "target/manifest.json").read_text())
    nodes = manifest["nodes"]
    by_name = {n["name"]: uid for uid, n in nodes.items() if n["resource_type"] in ("model", "seed")}
    blocks = sorted(
        n["name"] for n in nodes.values() if "gold_block" in n["tags"] and n["name"] != "poc_build_ted_notices"
    )

    def inside(uid: str) -> bool:
        n = nodes[uid]
        if n["resource_type"] == "seed" or n["name"] == "ted_notices":
            return True
        if not n["original_file_path"].startswith("models/gold/"):
            return False
        return not ("gold_block" in n["tags"] and n["name"] != "poc_build_ted_notices")

    mvr = next(n for n in nodes.values() if n["resource_type"] == "test" and n["name"] == "metrics_value_range")
    todo = [by_name[t] for t in OURS] + [p for p in mvr["depends_on"]["nodes"] if p in nodes]
    chosen: set[str] = set()
    while todo:
        uid = todo.pop()
        if uid in chosen:
            continue
        chosen.add(uid)
        todo += [p for p in nodes[uid]["depends_on"]["nodes"] if p in nodes and inside(p)]
    outside = [
        nodes[u]["name"] + "@" + nodes[u]["schema"] for u in chosen if not nodes[u]["schema"].startswith("zz_dev_")
    ]
    if outside:
        raise RuntimeError(f"guard: selected nodes outside zz_dev_: {outside}")
    return sorted(nodes[u]["name"] for u in chosen), blocks


def build(names: list[str], variables: dict, label: str) -> dict:
    started = time.monotonic()
    args = ["build", "--select", *names, "--indirect-selection", "cautious", "--threads", "2"]
    run = dbt(args, variables)
    for wait in (90, 240, 480):  # catalog HTTP 429: retry the failed and skipped nodes
        if run.returncode == 0 or "429" not in run.stdout:
            break
        time.sleep(wait)
        run = dbt(["retry", "--threads", "1"], variables)
    results = json.loads((WH / "target/run_results.json").read_text())["results"]
    kinds = Counter((r["unique_id"].split(".")[0], r["status"]) for r in results)
    ours = [
        f"{r['unique_id'].split('.')[2][:55]}:{r['status']}:{r.get('failures')}"
        for r in results
        if r["unique_id"].startswith("test.")
        and any(o in r["unique_id"] for o in ("ted_notice", "metrics_value_range", "fact_grain", "map_geography"))
    ]
    bad = [
        (r["unique_id"].split(".")[2][:60], r["status"], (r.get("message") or "")[:120])
        for r in results
        if r["status"] not in ("success", "pass")
    ]
    out = {
        "exit": run.returncode,
        "minutes": round((time.monotonic() - started) / 60, 1),
        "nodes": len(results),
        "by_kind_status": {f"{k}:{s}": n for (k, s), n in sorted(kinds.items())},
        "not_ok": bad[:8],
    }
    if label == "build1":
        out["ted_and_shared_tests"] = ours
    if run.returncode and not bad:
        out["stdout_tail"] = [ln[:250] for ln in run.stdout.splitlines() if "rror" in ln][:8]
    return out


def table_rows(cat, names: list[str]) -> dict:
    out = {}
    for name in names:
        try:
            snap = cat.load_table((SCRATCH, name)).current_snapshot()
            out[name] = int((snap.summary or {}).get("total-records", -1)) if snap else 0
        except Exception as exc:
            out[name] = f"{type(exc).__name__}"
    return out


# ---------------------------------------------------------------- checks


def scan(con, cat, name: str, alias: str | None = None) -> None:
    con.register(alias or name, cat.load_table((SCRATCH, name)).scan().to_arrow())


def silver_checks(con) -> dict:
    t = "silver"
    out = {
        "rows_distinct_keys_days": q(
            con,
            f"select count(*), count(distinct publication_number), "
            f"count(distinct publication_date), min(publication_date), max(publication_date) from {t}",
        )[0],
        "dup_keys": q(con, f"select count(*) from (select publication_number from {t} group by 1 having count(*) > 1)")[
            0
        ][0],
        "null_keys": q(
            con,
            f"select count(*) filter (where publication_number is null), count(*) filter (where raw_sha256 is null), "
            f"count(*) filter (where publication_date is null), count(*) filter (where notice_type is null) from {t}",
        )[0],
        "per_year": q(con, f"select year(publication_date), count(*) from {t} group by 1 order by 1"),
        "days_total_mismatch": q(
            con,
            f"select count(*) from (select publication_date, count(*) n, max(day_total_notices) t "
            f"from {t} group by 1 having n <> t)",
        )[0][0],
        "columns": [r[0] for r in con.execute(f"describe {t}").fetchall()],
        "form_type": q(con, f"select form_type, count(*) from {t} group by 1 order by 2 desc", 12),
        "legal_types": q(
            con, f"select u, count(*) from (select unnest(buyer_legal_types) u from {t}) group by 1 order by 2 desc", 14
        ),
        "estimated_value": q(
            con,
            f"select count(*) filter (where estimated_value is null), count(*) filter (where estimated_value = 0), "
            f"count(*) filter (where estimated_value is not null and estimated_value_currency is null) from {t}",
        )[0],
    }
    return out


def gold_checks(con, landing_ctx: dict) -> dict:
    out = {
        "build_rows_series_x": q(
            con,
            "select count(*), count(distinct measure_key || '|' || geography_key || '|' || breakdown_json), "
            "count(*) filter (where geography_key = '_X'), min(date_key), max(date_key), "
            "count(distinct date_key) from build",
        )[0],
        "fact_monthly_rows_dup_series_date": q(
            con,
            "select (select count(*) from fm), (select count(*) from (select series_key, date_key "
            "from fm group by all having count(*) > 1))",
        )[0],
        "dim_series_ted": q(
            con, "select count(*), count(*) filter (where is_total) from ds where series_key like 'ted_notices:%'"
        )[0],
        "metrics_rows_areas_months_min_max": q(
            con,
            "select count(*), count(distinct geography_key), count(distinct date_key), "
            "min(value), max(value), any_value(unit) from met",
        )[0],
        "metrics_pl": q(
            con, "select strftime(date_key, '%Y-%m'), value from met where geography_key = 'PL' order by date_key", 30
        ),
        "metrics_top_voivodeship": q(
            con,
            "select geography_key, strftime(date_key, '%Y-%m'), value from met "
            "where geography_key like 'TERC:%' order by value desc limit 3",
        ),
        "entity_rows_unique_x_empty_multi": q(
            con,
            "select count(*), count(distinct publication_number), "
            "count(*) filter (where list_contains(buyer_geography_keys, '_X')), "
            "count(*) filter (where len(buyer_geography_keys) = 0), "
            "count(*) filter (where len(buyer_geography_keys) > 1) from ent",
        )[0],
        "entity_columns_with_name": [r[0] for r in con.execute("describe ent").fetchall() if "name" in r[0]],
        "map_geography_ted": q(
            con, "select count(*), count(*) filter (where not matched) from gmap where source_system = 'ted_notices'"
        )[0],
        "other_metrics_nonempty": q(con, "select count(*) from met_other where n > 0")[0][0],
    }
    month = "2025-06"
    out["pl_" + month + "_metrics_vs_python_recount"] = [
        q(con, f"select value from met where geography_key = 'PL' and strftime(date_key, '%Y-%m') = '{month}'")[0][0],
        recount(landing_ctx, month),
    ]
    return out


def recount(ctx: dict, month: str) -> list:
    """Notices of one month counted in Python straight from the landed answers (independent of the SQL)."""
    from ingestion.common import bulk_bronze
    from ingestion.to_landing.ted_notices import extract as landing

    receipts, client, bucket = ctx["receipts"], ctx["client"], ctx["bucket"]
    keys = [rows[-1][1] for e, rows in receipts.items() if e.startswith("day_" + month.replace("-", ""))]
    numbers, files = set(), 0
    for r in landing.read_receipts(client, bucket, keys):
        numbers |= {
            n["publication-number"]
            for n in json.loads(bulk_bronze.read_raw(client, bucket, "ted_notices", r))["notices"]
        }
        files += 1
    return [files, len(numbers)]


# ---------------------------------------------------------------- main


def trial() -> int:
    from ingestion.common import bronze_files as bf
    from ingestion.common.r2 import catalog
    from ingestion.to_bronze.ted_notices import load as bronze
    from ingestion.to_landing.ted_notices import extract as landing

    bronze.annotate = collect
    landing.annotate = collect
    if not precheck(landing):
        return 1
    res_l: dict = {}
    ctx = landing_steps(res_l)
    note("ted trial Landing (scratch zz_dev_ted_notices)", res_l)

    t0 = time.monotonic()
    res_b: dict = {"exit": [bronze.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]}
    res_b["minutes"] = round((time.monotonic() - t0) / 60, 1)
    res_b["loads"] = [
        {
            k: v
            for k, v in r.items()
            if k in ("candidates_in_landing", "new", "appended_files", "rows_added", "empty_files", "rejected")
        }
        for r in COLLECTED.get("ted_notices Bronze from Landing", [])
    ]
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    scan(con, cat, "ted_notices", "bronze_ted")
    try:
        rejected = cat.load_table((SCRATCH, "ted_notices_rejected_files")).scan().to_arrow().num_rows
    except Exception:
        rejected = 0
    res_b["rejected_rows"] = rejected
    res_b["profile"] = {
        "rows_files_entities_keys": q(
            con,
            "select count(*), count(distinct raw_sha256), count(distinct entity), "
            "count(distinct publication_number) from bronze_ted",
        )[0],
        "days": q(
            con, "select min(source_version), max(source_version), count(distinct source_version) from bronze_ted"
        )[0],
        "null_counts": {
            c: n
            for c in [r[0] for r in con.execute("describe bronze_ted").fetchall()]
            if (n := q(con, f"select count(*) filter (where {c} is null) from bronze_ted")[0][0])
        },
        "columns": len(con.execute("describe bronze_ted").fetchall()),
        "value_non_numeric": q(
            con,
            "select estimated_value_proc, count(*) from bronze_ted where estimated_value_proc is not null "
            "and (case when left(estimated_value_proc, 1) = '[' and json_valid(estimated_value_proc) "
            "then try_cast(json_extract_string(estimated_value_proc, '$[0]') as double) "
            "else try_cast(estimated_value_proc as double) end) is null group by 1 order by 2 desc",
            5,
        ),
    }
    note("ted trial Bronze (scratch)", res_b)

    patch_checkout()
    variables = {"gold_schema": SCRATCH, "topics": yaml.safe_load((WH / "warehouse.yaml").read_text())["topics"]}
    names, blocks = selection(variables)
    variables["waiting_blocks"] = blocks
    res_d: dict = {"selected_nodes": len(names), "waiting_blocks": len(blocks)}
    res_d["build1"] = build(names, variables, "build1")
    watch = [
        "ted_notices",
        "build_ted_notices",
        "ted_notice",
        "metrics_procurement_notices",
        "map_geography",
        "fact_monthly",
        "dim_series",
    ]
    res_d["rows_after_build1"] = table_rows(cat, watch)
    res_d["build2"] = build(names, variables, "build2")
    res_d["rows_after_build2"] = table_rows(cat, watch)
    note("ted trial dbt build (real dbt, scratch)", res_d, "notice" if res_d["build1"]["exit"] == 0 else "warning")

    for name, alias in (
        ("ted_notices", "silver"),
        ("build_ted_notices", "build"),
        ("fact_monthly", "fm"),
        ("dim_series", "ds"),
        ("metrics_procurement_notices", "met"),
        ("ted_notice", "ent"),
        ("map_geography", "gmap"),
    ):
        scan(con, cat, name, alias)
    note("ted trial Silver (dbt, scratch)", silver_checks(con))
    others = [
        n.replace("poc_", "", 1)
        for n in names
        if n.startswith("poc_metrics_") and n != "poc_metrics_procurement_notices"
    ]
    counts = table_rows(cat, others)
    con.execute("create table met_other (n bigint)")
    for v in counts.values():
        con.execute("insert into met_other values (?)", [v if isinstance(v, int) else -1])
    note("ted trial Gold (dbt, scratch)", gold_checks(con, ctx))
    return 0 if res_d["build1"]["exit"] == 0 and res_d["build2"]["exit"] == 0 else 1


def cleanup() -> None:
    from ingestion.common import bronze_files as bf
    from ingestion.common.r2 import catalog, req, s3

    removed: dict = {"tables": 0, "landing_objects": 0, "lake_objects": 0, "failed": {}}
    cat = bf.retry(catalog, "catalog")
    try:
        tables = list(cat.list_tables(SCRATCH))
    except Exception as exc:
        tables, removed["failed"]["list"] = [], str(exc)[:120]
    for table in tables:
        for attempt in range(6):
            try:
                try:
                    cat.purge_table(table)
                except Exception:
                    cat.drop_table(table)
                removed["tables"] += 1
                removed["failed"].pop(table[-1], None)
                break
            except Exception as exc:
                removed["failed"][table[-1]] = str(exc)[:80]
                time.sleep(10 * (attempt + 1))
    for attempt in range(6):
        try:
            cat.drop_namespace(SCRATCH)
            removed["failed"].pop("namespace", None)
            break
        except Exception as exc:
            removed["failed"]["namespace"] = str(exc)[:120]
            time.sleep(10 * (attempt + 1))
    client = s3()
    for bucket_var, label in (
        ("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
        ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects"),
    ):
        bucket = req(bucket_var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
    try:
        removed["namespace_left"] = (SCRATCH,) in cat.list_namespaces()
    except Exception:
        removed["namespace_left"] = "unknown"
    note(
        "ted trial scratch removed",
        removed,
        "notice" if not removed["failed"] and not removed["objects_left"] else "warning",
    )


def main() -> int:
    status = 1
    try:
        status = trial()
    except Exception as exc:
        note(
            "ted trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1800:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
