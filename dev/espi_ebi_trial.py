"""Dev trial of KNF ESPI (CD-006, #1486) and GPW EBI (CD-007, #1488): Landing → Bronze → Silver → Gold into scratch.

Landing prefix, lake prefix and Iceberg namespace zz_dev_espi_ebi; everything written is removed in `finally`.
Landing and Bronze each run twice (the second run must write nothing). Silver and Gold are rendered with Jinja from
warehouse/ and run in local DuckDB over the scratch Bronze tables, each built twice, with the YAML schema tests and
the singular completeness tests. Read-only extras: three KNF peak days profiled for form codes (not landed), the
producers' terms pages (KNF legal note, GPW disclaimer) for the licence quotes. Deleted before merge.

Window (declared): KNF days of receipt 2026-09-30 to 2026-11-01 (today fixed at 2026-11-01, backfill 32 days: one
whole month, October 2026, for Gold); GPW from 2025-01-01 (the production floor) to today.
"""

from __future__ import annotations

from collections import Counter
import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import sys
import time

import duckdb
from jinja2 import Environment
import yaml

from ingestion.common import bulk_bronze
from ingestion.common import report_lists as lists
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

SCRATCH = "zz_dev_espi_ebi"
CAPTURED: list = []
KNF_TODAY, KNF_BACKFILL = date(2026, 11, 1), 32
PEAK_DAYS = (date(2026, 4, 30), date(2026, 5, 29), date(2026, 9, 30))
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SPOT = {"PKN Orlen": "ORLEN", "PKO BP": "PKO BANK POLSKI", "KGHM": "KGHM", "CD Projekt": "CD PROJEKT"}
SOURCES = {
    "knf_espi": {"table": "knf_espi_reports", "silver": "knf_espi_reports", "day": "received_date"},
    "gpw_ebi": {"table": "gpw_ebi_reports", "silver": "gpw_ebi_reports", "day": "published_date"},
}


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:2500]}", flush=True)


lists.annotate = _capture
bulk_bronze.annotate = _capture


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, json.loads(text) if len(text) <= 3900 else {"truncated": text[:3800]})


def one(con, q):
    return con.execute(q).fetchone()[0]


def rows(con, q, limit=40):
    return [[str(v) for v in r] for r in con.execute(q).fetchall()][:limit]


def landing_objects(source: str) -> tuple[int, int, int]:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = size = receipts = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/{source}/"):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                receipts += 1
            else:
                files += 1
                size += o["Size"]
    return files, size, receipts


def summary_of(kind: str) -> dict:
    found = next((d for t, d in CAPTURED if kind in t), {})
    return {k: v for k, v in found.items() if k != "days"}


def landing(source: str) -> dict:
    out: dict = {"runs": []}
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        if source == "knf_espi":
            code = lists.extract_by_day(source, lists.read_knf_day, budget_minutes=60, landing_root=SCRATCH,
                                        today=KNF_TODAY, backfill=KNF_BACKFILL)
        else:
            code = lists.extract_newest_first(source, lists.read_gpw_since, lists.GPW_PAGE, landing_root=SCRATCH)
        files, size, receipts = landing_objects(source)
        out["runs"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                            **summary_of("landing load"), "files_after": files, "bytes_after": size,
                            "receipts_after": receipts})
    CAPTURED.clear()
    if source == "knf_espi":
        code = lists.inspect(source, lambda r, d: lists.read_knf_day(r, d)[:2], landing_root=SCRATCH, today=KNF_TODAY)
    else:
        code = lists.inspect(source, lambda r, d: (None, lists.read_gpw_since(r, d)[0].get(d, [])), landing_root=SCRATCH)
    out["inspect"] = {"exit": code, **summary_of("landing inspect")}
    CAPTURED.clear()
    out["verify_exit"] = lists.verify(source, landing_root=SCRATCH)
    out["verify"] = {k: v for k, v in summary_of("").items() if k in ("result", "missing", "checked", "receipts")}
    return out


def peak_types() -> dict:
    """Form codes on three KNF peak days (annual, Q1 and half-year deadlines): profiled, not landed."""
    reader, out = lists.Reader(), {}
    for day in PEAK_DAYS:
        try:
            listed, found, _ = lists.read_knf_day(reader, day)
            out[day.isoformat()] = {"listed": listed, "rows": len(found),
                                    "types": Counter(r["report_type"] or "''" for r in found).most_common()}
        except Exception as exc:  # noqa: BLE001
            out[day.isoformat()] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    out["requests"] = reader.requests
    return out


def bronze(source: str):
    table = SOURCES[source]["table"]
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = lists.bronze_load(source, table, landing_root=SCRATCH, namespace=SCRATCH)
        d = summary_of("Bronze from Landing")
        out["loads"].append({"run": n, "exit": code, **{k: d.get(k) for k in (
            "entities_landed", "candidates", "appended_files", "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, table))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, f"{source}_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected_files"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:200]} for r in rej]
    return out, arrow


def bronze_profile(con, source: str) -> dict:
    b = f"bronze_{source}"
    day = "received_date" if source == "knf_espi" else "published_text"
    kind = "report_type" if source == "knf_espi" else "category"
    p = {
        "rows": one(con, f"select count(*) from {b}"),
        "distinct_report_ids": one(con, f"select count(distinct report_id) from {b}"),
        "days": one(con, f"select count(distinct entity) from {b}"),
        "day_range": rows(con, f"select min(entity), max(entity) from {b}"),
        "dup_report_ids": one(con, f"select count(*) from (select report_id from {b} group by 1 having count(*) > 1)"),
        "null_or_empty": {c: one(con, f"select count(*) from {b} where {c} is null or {c} = ''")
                          for c in ("report_id", "issuer_name", day, kind, "link", "raw_sha256")},
        "kinds": rows(con, f"select {kind}, count(*) from {b} group by 1 order by 2 desc"),
        "issuers": one(con, f"select count(distinct issuer_name) from {b}"),
        "listed_null_days": one(con, f"select count(distinct entity) from {b} where listed is null"),
    }
    if source == "gpw_ebi":
        p["isin_empty"] = one(con, f"select count(*) from {b} where isin = ''")
        p["report_number_shapes"] = rows(con, f"select regexp_replace(report_number, '[0-9]', '9', 'g'), count(*) "
                                              f"from {b} group by 1 order by 2 desc", 10)
    return p


def env() -> Environment:
    return Environment(extensions=["jinja2.ext.do"])


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return env().from_string(Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda name: refs.get(name, name), source=lambda _s, t: sources[t])


def schema_tests(con, yml: str, model: str, table: str) -> dict:
    spec = yaml.safe_load(Path(yml).read_text())
    m = next(x for x in spec["models"] if x["name"] == model)
    results = {}
    for col in m.get("columns", []):
        for t in col.get("tests", []):
            if t == "not_null":
                results[f"not_null {col['name']}"] = one(con, f"select count(*) from {table} where {col['name']} is null")
            elif t == "unique":
                results[f"unique {col['name']}"] = one(
                    con, f"select count(*) from (select {col['name']} from {table} group by 1 having count(*) > 1)")
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lst = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                sev = (t["accepted_values"].get("config") or {}).get("severity", "error")
                results[f"accepted_values {col['name']} ({sev})"] = rows(
                    con, f"select {col['name']}, count(*) from {table} where {col['name']} is not null "
                         f"and cast({col['name']} as varchar) not in ({lst}) group by 1 order by 2 desc", 30)
    return results


def silver(con, source: str) -> dict:
    model = SOURCES[source]["silver"]
    srcmap = {SOURCES[source]["table"]: f"bronze_{source}"}
    sql = render(f"warehouse/models/silver/{source}/{model}.sql", {}, srcmap)
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table {model} as {sql}")
        counts.append(one(con, f"select count(*) from {model}"))
    s: dict = {"rows_two_builds": counts,
               "tests": schema_tests(con, f"warehouse/models/silver/{source}/_{source}.yml", model, model)}
    test = render(f"warehouse/tests/{model}_complete.sql", {}, srcmap)
    s[f"test {model}_complete"] = rows(con, f"select * from ({test})", 10)
    d = SOURCES[source]["day"]
    s["range"] = rows(con, f"select min({d}), max({d}), count(distinct {d}) from {model}")
    s["per_month"] = rows(con, f"select strftime({d}, '%Y-%m'), count(*) from {model} group by 1 order by 1")
    s["issuers"] = one(con, f"select count(distinct issuer_name) from {model}")
    if source == "gpw_ebi":
        s["isin_null"] = one(con, f"select count(*) from {model} where isin is null")
        s["report_number_null"] = one(con, f"select count(*) from {model} where report_number is null")
    spots = {}
    for label, needle in SPOT.items():
        spots[label] = rows(con, f"select issuer_name, count(*), min({d}), max({d}), "
                                 f"string_agg(distinct {'report_type' if source == 'knf_espi' else 'category'}, ' ') "
                                 f"from {model} where upper(issuer_name) like '%{needle}%' group by 1 order by 2 desc", 3)
    s["spot_issuers"] = spots
    s["sample"] = rows(con, f"select * exclude (raw_sha256, source_version, ingested_at) from {model} "
                            f"where upper(issuer_name) like '%ORLEN%' or upper(issuer_name) like '%KGHM%' "
                            f"order by report_id limit 2", 2)
    return s


def gold(con, source: str) -> dict:
    model = SOURCES[source]["silver"]
    refs = {model: model}
    out: dict = {}
    sql = render(f"warehouse/models/gold/filing_{source}.sql", refs, {})
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table filing_{source} as {sql}")
        counts.append(one(con, f"select count(*) from filing_{source}"))
    out["filing_rows_two_builds"] = counts
    out["filing_tests"] = schema_tests(con, f"warehouse/models/gold/_{source}.yml", f"filing_{source}", f"filing_{source}")
    sql = render(f"warehouse/models/gold/poc_build_{source}.sql", refs, {})
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table build_{source} as {sql}")
        counts.append(one(con, f"select count(*) from build_{source}"))
    b = f"build_{source}"
    out["build_rows_two_builds"] = counts
    out["columns_in_contract_order"] = [d[0] for d in con.execute(f"select * from {b} limit 0").description] == CONTRACT
    out["build_tests"] = schema_tests(con, f"warehouse/models/gold/_{source}.yml", f"poc_build_{source}", b)
    out["dup_series_date"] = one(con, f"select count(*) from (select measure_key || '|' || geography_key || '|' || "
                                      f"breakdown_json as series_key, date_key from {b} group by all having count(*) > 1)")
    out["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 "
                                                    f"having count(distinct frequency) > 1)")
    out["null_values"] = one(con, f"select count(*) from {b} where value is null")
    out["months"] = rows(con, f"select min(date_key), max(date_key), count(distinct date_key) from {b}")
    out["totals"] = rows(con, f"select period_label, value from {b} where is_total order by 1", 30)
    out["breakdowns"] = rows(con, f"select breakdown_json, sum(value) from {b} where not is_total group by 1 order by 2 desc", 15)
    out["totals_equal_breakdown_sums"] = one(con, f"select count(*) from (select date_key, sum(value) filter (where is_total) t, "
                                                  f"sum(value) filter (where not is_total) s from {b} group by 1) where t <> coalesce(s, 0)")
    d = SOURCES[source]["day"]
    out["silver_rows_in_gold_months"] = rows(con, f"select strftime({d}, '%Y-%m'), count(*) from {model} "
                                                  f"where strftime({d}, '%Y-%m') in (select period_label from {b}) group by 1 order by 1", 30)
    return out


def terms_pages() -> dict:
    """The producers' own terms pages (read-only, robots.txt first): text around copyright and reuse words."""
    reader, out = lists.Reader(), {}
    words = re.compile(r"(?i)(prawa autorskie|prawo autorskie|autorsk|copyright|wykorzyst|kopiowa|rozpowszechnia|"
                       r"reproduc|reuse|liability|odpowiedzialno|©)")
    for name, robots, url in (
        ("knf_legal_note", "https://www.knf.gov.pl/robots.txt", "https://www.knf.gov.pl/nota_prawna/index.html"),
        ("knf_oam_list_footer", None, "https://moam.knf.gov.pl/moam.nsf"),
        ("gpw_disclaimer", None, "https://www.gpw.pl/disclaimer"),
    ):
        try:
            if robots:
                body = reader.fetch("GET", robots)
                out[f"{name}_robots"] = " | ".join(line for line in body.splitlines() if line.strip())[:300]
                if re.search(r"(?im)^disallow:\s*/nota_prawna|^disallow:\s*/\s*$", body):
                    out[name] = "robots.txt disallows: not read"
                    continue
            page = reader.fetch("GET", url)
            text = lists.text(re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", page))
            hits = []
            for m in words.finditer(text):
                hits.append(text[max(0, m.start() - 160): m.start() + 260])
                if len(hits) >= 4:
                    break
            out[name] = {"url": url, "chars": len(text), "snippets": hits}
        except Exception as exc:  # noqa: BLE001
            out[name] = {"url": url, "error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    return out


def licence() -> dict:
    from warehouse.publish import licensed_sources

    def seed(name):
        with open(f"warehouse/seeds/{name}.csv", newline="", encoding="utf-8") as h:
            return list(csv.DictReader(h))

    out = {}
    licensed = licensed_sources(seed("source"), seed("licence"))
    for source in SOURCES:
        src = [r for r in seed("source") if r["source_key"] == source]
        lic = [r for r in seed("licence") if src and r["licence_key"] == src[0]["licence_key"]]
        out[source] = {"source_row": src, "licence_row": lic, "licensed_by_gate": source in licensed}
    return out


def cleanup() -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/"))
    try:
        removed["namespace_left"] = SCRATCH in [n[0] for n in cat.list_namespaces()]
    except Exception:  # noqa: BLE001
        pass
    note("espi_ebi scratch removed", removed)


def main(argv: list[str]) -> int:
    status = 0
    started = datetime.now()
    try:
        note("espi_ebi trial setup", {"duckdb": duckdb.__version__, "scratch": SCRATCH, "knf_today": KNF_TODAY,
                                      "knf_backfill_days": KNF_BACKFILL, "gpw_floor": "2025-01-01",
                                      "started": started.isoformat(timespec="seconds")})
        con = duckdb.connect()
        for source in SOURCES:
            land = landing(source)
            b, arrow = bronze(source)
            con.register(f"bronze_{source}", arrow)
            note(f"{source} landing (twice), inspect, verify; bronze (twice)", {"landing": land, "bronze": b})
            note(f"{source} bronze profile", bronze_profile(con, source))
            note(f"{source} silver", silver(con, source))
            note(f"{source} gold", gold(con, source))
        note("knf_espi peak days, form codes (not landed)", peak_types())
        note("espi_ebi licence gate and terms pages", {"gate": licence(), "terms": terms_pages()})
    except Exception as exc:  # noqa: BLE001
        status = 1
        import traceback

        traceback.print_exc()
        annotate("error", "espi_ebi trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
