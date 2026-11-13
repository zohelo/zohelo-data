"""Dev trial (PLG-016 ESMA registers, #2994): Landing → Bronze → Silver → Gold into scratch, on the build code.

Landing (shared bulk_file pattern) into the Landing prefix zz_dev_esma_registers/ (inspect, load twice, verify),
Bronze load into Iceberg namespace zz_dev_esma_registers and lake prefix zz_dev_esma_registers/ (twice), a Bronze
profile, then the Silver and Gold SQL of the branch rendered with Jinja and run in local DuckDB (each built twice)
with every test of their yml files and the singular test of the source; the Gold geography map is the source's own
macro (static), PL checked against production gold.dim_geography (read-only). Also: the registers site's robots.txt
and the Legal Notice linked from its footer (quoted), the field names the slice holds without the fl list (counted
in memory, never stored), and the licence gate of warehouse/publish.py on the seeds. Everything written is removed
in `finally`. At most ten annotations. Never on the build branch.
"""

from __future__ import annotations

from collections import Counter
import csv
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urljoin

import duckdb
from jinja2 import Environment
import requests
import yaml

from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.esma_registers import load as bronze_load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "esma_registers"
SCRATCH = "zz_dev_esma_registers"
REAL = report.annotate
CAPTURED: list = []
SITE = "https://registers.esma.europa.eu/"


def captured(level, title, data):  # activity annotations go to the log; the trial keeps them for its own summary
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


def q(con, sql):
    return [
        [None if v is None else (v if isinstance(v, (int, float)) else str(v)) for v in r]
        for r in con.execute(sql).fetchall()
    ]


def one(con, sql):
    return con.execute(sql).fetchall()[0][0]


def get(url, **kw):
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 120), **kw)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(30 * (attempt + 1))
                continue
            return r
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


class Links(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links, self._href, self._text = [], None, []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._href, self._text = dict(attrs).get("href"), []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            self.links.append((" ".join("".join(self._text).split()), self._href))
            self._href = None


def plain(html):
    html = re.sub(r"(?is)<(script|style|noscript)\b.*?</\1>", " ", html)
    return " ".join(re.sub(r"(?s)<[^>]+>", " ", html).split())


def legal_notice() -> None:
    out: dict = {}
    r = get(SITE + "robots.txt")
    out["robots"] = {"http": r.status_code, "type": r.headers.get("Content-Type"), "head": r.text[:120]}
    targets: list = []
    for page in (SITE, SITE + "publication/"):
        time.sleep(3)
        r = get(page)
        parser = Links()
        parser.feed(r.text)
        legal = [
            [t[:50], urljoin(r.url, h)]
            for t, h in parser.links
            if re.search(r"(?i)legal|notice|copyright|disclaimer", t + " " + h)
        ]
        out[page] = {"http": r.status_code, "final": r.url, "links": len(parser.links), "legal_links": legal[:6]}
        print(
            "PAGE",
            page,
            r.status_code,
            r.url,
            json.dumps(parser.links[:150], ensure_ascii=False),
            "\nTEXT",
            plain(r.text)[:2500],
            flush=True,
        )
        targets += [u for t, u in legal if re.search(r"(?i)legal", t + u) and u not in targets]
    out["read"] = {}
    for u in targets[:2]:
        time.sleep(3)
        r = get(u)
        text = plain(r.text)
        print("LEGAL", u, r.status_code, r.url, "\n", text[:8000], flush=True)
        hits = [
            text[max(0, m.start() - 120) : m.start() + 600]
            for m in re.finditer(r"(?i)reproduc|re-?use|copyright", text)
        ]
        out["read"][u] = {"http": r.status_code, "final": r.url, "title": text[:100], "hits": hits[:3]}
    REAL("notice", "esma_registers licence: registers site robots.txt and footer Legal Notice", out)


def unfiltered_fields() -> dict:
    """Field names and fill counts of the slice without the fl list (in memory only, nothing printed but counts)."""
    params = {"q": "*:*", "fq": ['ae_homeMemberState:"POLAND"', "type_s:parent"], "rows": 2000, "wt": "json"}
    d = get("https://registers.esma.europa.eu/solr/esma_registers_upreg/select", params=params).json()
    docs = d["response"]["docs"]
    present, filled = Counter(), Counter()
    for doc in docs:
        for k, v in doc.items():
            present[k] += 1
            filled[k] += v not in ("", None, [])
    return {"numFound": d["response"]["numFound"], "fields": {k: [present[k], filled[k]] for k in sorted(present)}}


def render(path: Path, refs: dict, sources: dict, prefix: str = "") -> str:
    text = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", path.read_text(), flags=re.S)
    return (
        Environment()
        .from_string(prefix + text)
        .render(ref=lambda n: refs[n], source=lambda _s, t: sources[t], var=lambda n, d=None: d)
    )


def yml_tests(con, yml: Path, name: str, table: str) -> list:
    spec = next(m for m in yaml.safe_load(yml.read_text())["models"] if m["name"] == name)
    out = []
    for t in spec.get("tests", []):
        if isinstance(t, dict) and "unique" in t:
            expr = t["unique"]["column_name"]
            out.append(["unique " + expr, one(con, f"select count(*) - count(distinct {expr}) from {table}")])
    for col in spec.get("columns", []):
        c = col["name"]
        for t in col.get("tests", []):
            kind = t if isinstance(t, str) else next(iter(t))
            if kind == "not_null":
                out.append([f"not_null {c}", one(con, f"select count(*) from {table} where {c} is null")])
            elif kind == "unique":
                out.append(
                    [
                        f"unique {c}",
                        one(con, f"select count(*) - count(distinct {c}) from {table} where {c} is not null"),
                    ]
                )
            elif kind == "accepted_values":
                vals = t["accepted_values"]["arguments"]["values"]
                lit = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                out.append(
                    [
                        f"accepted_values {c}",
                        one(con, f"select count(*) from {table} where {c} is not null and {c} not in ({lit})"),
                    ]
                )
            else:
                out.append([f"{kind} {c}", "not evaluated"])
    return out


def trial() -> None:  # noqa: PLR0915
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = captured
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    spec = bulk_file.declaration(SOURCE)[0]
    landing: dict = {"exit": {}}
    for n, mode in enumerate(("inspect", "load", "load", "verify")):
        CAPTURED.clear()
        landing["exit"][f"{n + 1}_{mode}"] = bulk_file.main(SOURCE, mode, landing_root=SCRATCH)
        files = next((d["files"] for t, d in CAPTURED if t.endswith(f"landing {mode}")), [])
        landing[f"{n + 1}_{mode}"] = [
            {k: f.get(k) for k in ("outcome", "state", "http", "size_mb", "version", "missing_data")} for f in files
        ]
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], SCRATCH))
    landing["copies"] = len(held)
    landing["bytes"] = [r["size_bytes"] for r in held]
    landing["sha256"] = [r["sha256"][:12] for r in held]
    landing["content_type"] = [r["detail"]["http"]["content_type"] for r in held]
    rows, appended = [], []
    cat = catalog()
    for _ in (1, 2):
        CAPTURED.clear()
        bronze_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        appended.append(next((d.get("appended_files") for t, d in CAPTURED if "Bronze from Landing" in t), None))
        rows.append(cat.load_table((SCRATCH, "esma_registers_entries")).scan().to_arrow().num_rows)
    arrow = cat.load_table((SCRATCH, "esma_registers_entries")).scan().to_arrow()
    try:
        rejected = cat.load_table((SCRATCH, "esma_registers_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001 - no table when nothing was rejected
        rejected = []
    body = bulk_bronze.read_raw(client, bucket, SOURCE, held[-1])
    layout = {}
    doc = json.loads(body)
    first = doc["response"]["docs"][0]
    variants = {
        "as landed": body,
        "extra field (address)": json.dumps(
            {**doc, "response": {**doc["response"], "docs": [{**first, "ae_headOfficeAddress": "x"}]}}
        ).encode(),
        "truncated page": json.dumps(
            {**doc, "response": {**doc["response"], "docs": doc["response"]["docs"][:10]}}
        ).encode(),
        "status as list": json.dumps(
            {**doc, "response": {**doc["response"], "numFound": 1, "docs": [{**first, "ae_status": ["Active"]}]}}
        ).encode(),
    }
    for label, b in variants.items():
        try:
            layout[label] = f"accepted, {bronze_load.decode(b, held[-1]).num_rows} rows"
        except ValueError as exc:
            layout[label] = f"rejected: {str(exc)[:110]}"
    REAL(
        "notice",
        "esma_registers landing + bronze (scratch)",
        {
            "landing": landing,
            "bronze_rows_load1_load2": rows,
            "bronze_appended_files_load1_load2": appended,
            "answer_numFound": doc["response"].get("numFound"),
            "answer_keys": sorted(doc),
            "rejected": [(r.get("entity"), (r.get("error") or "")[:200]) for r in rejected],
            "layout_check": layout,
        },
    )

    con = duckdb.connect()
    con.register("bronze_a", arrow)
    con.execute("create table bronze as select * from bronze_a")
    cols = [
        c
        for c in arrow.column_names
        if c not in ("entity", "raw_sha256", "source_version", "ingested_at", "source_row_number")
    ]
    fill = {
        c: one(con, f"select count(*) filter (where {c} is not null and trim({c}) <> '') from bronze") for c in cols
    }
    corp = r"(?i)(s\.?\s?a\.?\b|sp\.?\s?z\s?o\.?\s?o|spółka|tfi\b|towarzystwo|fundusz|dom maklerski|bank|\bltd|limited|gmbh|\bag\b|plc|\basi\b|alternatywn|oddział|branch|fióktelep|giełda|exchange|\bse\b|n\.v\.|b\.v\.|s\.k\.a|sp\.\s?k|sp\.\s?j|\binc\b|\bllc\b|\bs\.r\.o|\bas\b|\bab\b|uab|kft|zrt|srl|s\.r\.l|sicav|fund|capital|invest|management|asset|securities|brokers?|markets?|crowd|platform)"
    no_marker = q(
        con,
        f"select ae_entity_type_code, ae_entity_name from bronze where not regexp_matches(ae_entity_name, '{corp}') order by 1, 2",
    )
    print("NAMES_WITHOUT_LEGAL_FORM_MARKER", json.dumps(no_marker, ensure_ascii=False), flush=True)
    try:
        unfiltered = unfiltered_fields()
    except Exception as exc:  # noqa: BLE001
        unfiltered = {"error": str(exc)[:200]}
    REAL(
        "notice",
        "esma_registers bronze profile",
        {
            "rows": one(con, "select count(*) from bronze"),
            "distinct_id": one(con, "select count(distinct id) from bronze"),
            "null_id": one(con, "select count(*) from bronze where id is null"),
            "files": one(con, "select count(distinct raw_sha256) from bronze"),
            "filled_per_column": fill,
            "type_label_status": q(
                con,
                "select ae_entity_type_code, ae_entity_type_label, ae_status, count(*) from bronze group by all order by 1, 3",
            ),
            "office_type": q(con, "select ae_office_type, count(*) from bronze group by 1 order by 2 desc"),
            "authority": q(con, "select ae_competent_authority, count(*) from bronze group by 1"),
            "legalform": q(con, "select ae_legalform, count(*) from bronze group by 1 order by 2 desc limit 8"),
            "date_shapes": q(
                con,
                """select 'auth', count(*) filter (where regexp_full_match(ae_authorisation_notification_date, '[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00Z')),
            count(*) filter (where nullif(ae_authorisation_notification_date, '') is null), min(ae_authorisation_notification_date), max(ae_authorisation_notification_date) from bronze
            union all select 'withdrawal_iso', count(*) filter (where regexp_full_match(ae_authorisation_withdrawal_end_date, '[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00Z')),
            count(*) filter (where nullif(ae_authorisation_withdrawal_end_date, '') is null), min(ae_authorisation_withdrawal_end_date), max(ae_authorisation_withdrawal_end_date) from bronze
            union all select 'withdrawal_str', count(*) filter (where regexp_full_match(ae_authorisation_withdrawal_end_date_str, '[0-9]{2}/[0-9]{2}/[0-9]{4}')),
            count(*) filter (where nullif(ae_authorisation_withdrawal_end_date_str, '') is null), min(ae_authorisation_withdrawal_end_date_str), max(ae_authorisation_withdrawal_end_date_str) from bronze
            union all select 'last_update', count(*) filter (where regexp_full_match(ae_last_update, '[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00Z')),
            count(*) filter (where nullif(ae_last_update, '') is null), min(ae_last_update), max(ae_last_update) from bronze""",
            ),
            "lei": q(
                con,
                """select ae_office_type, count(*) filter (where nullif(ae_lei, '') is not null), count(*) filter (where nullif(ae_head_office_lei, '') is not null),
            count(*) filter (where nullif(ae_lei, '') is not null and not regexp_full_match(ae_lei, '[A-Z0-9]{18}[0-9]{2}')), count(*) from bronze group by 1""",
            ),
            "lei_repeated": one(
                con,
                "select count(*) from (select ae_lei from bronze where nullif(ae_lei, '') is not null group by 1 having count(*) > 1)",
            ),
            "names_without_legal_form_marker": len(no_marker),
            "unfiltered_slice_fields_present_filled": unfiltered,
        },
    )

    sources = {"esma_registers_entries": "bronze"}
    silver_dir = ROOT / "warehouse/models/silver/esma_registers"
    silver_sql = render(silver_dir / "esma_registers_entities.sql", {}, sources)
    silver_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table silver as {silver_sql}")
        silver_rows.append(one(con, "select count(*) from silver"))
    tests = yml_tests(con, silver_dir / "_esma_registers.yml", "esma_registers_entities", "silver")
    complete = render(
        ROOT / "warehouse/tests/esma_registers_entities_complete.sql", {"esma_registers_entities": "silver"}, sources
    )
    tests.append(["tests/esma_registers_entities_complete.sql", one(con, f"select count(*) from ({complete})")])
    REAL(
        "notice",
        "esma_registers silver esma_registers_entities",
        {
            "rows_two_builds": silver_rows,
            "tests_failing_rows": tests,
            "tests_passed": sum(1 for _, n in tests if n == 0),
            "tests_failed": sum(1 for _, n in tests if n != 0),
            "null_keys": q(
                con,
                "select count(*) filter (where entry_id is null), count(*) filter (where raw_sha256 is null), count(*) filter (where entity_name is null) from silver",
            ),
            "per_type_status": q(
                con,
                "select entity_type_code, status, count(*), count(lei), count(head_office_lei) from silver group by all order by 1, 2",
            ),
            "office_status": q(
                con, "select office_type, status, count(*), count(lei) from silver group by all order by 1, 2"
            ),
            "dates": q(
                con,
                "select min(authorisation_date), max(authorisation_date), count(authorisation_date), min(withdrawal_date), max(withdrawal_date), count(withdrawal_date), min(last_update), max(last_update) from silver",
            ),
            "withdrawn_but_active": one(
                con, "select count(*) from silver where withdrawal_date is not null and status = 'Active'"
            ),
            "inactive_without_withdrawal": one(
                con, "select count(*) from silver where withdrawal_date is null and status = 'Inactive'"
            ),
            "host_states": one(con, "select count(*) from silver where host_member_states is not null"),
        },
    )

    macros = (ROOT / "warehouse/macros/sources/esma_registers.sql").read_text()
    geo_cte = Environment().from_string(macros + "{{ poc_geography__esma_registers() }}").render().strip()
    con.execute(f"create table geo_map as with {geo_cte} select * from esma_registers")
    geo = cat.load_table(("gold", "dim_geography")).scan(selected_fields=("geography_key",)).to_arrow()
    con.register("dim_geo", geo)
    refs = {"esma_registers_entities": "silver", "poc_map_geography": "geo_map"}
    gold_dir = ROOT / "warehouse/models/gold"
    build_sql = render(gold_dir / "poc_build_esma_registers.sql", refs, {})
    entity_sql = render(gold_dir / "supervised_entity_esma_registers.sql", refs, {})
    counts = {"build": [], "entity": []}
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build_sql}")
        con.execute(f"create or replace table entity as {entity_sql}")
        counts["build"].append(one(con, "select count(*) from build"))
        counts["entity"].append(one(con, "select count(*) from entity"))
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    REAL(
        "notice",
        "esma_registers gold build_esma_registers",
        {
            "rows_two_builds": counts["build"],
            "dup_series_date": one(
                con, f"select count(*) from (select {series}, date_key from build group by all having count(*) > 1)"
            ),
            "series": one(con, f"select count(distinct {series}) from build"),
            "null_keys": q(
                con,
                "select count(*) filter (where measure_key is null), count(*) filter (where geography_key is null), count(*) filter (where date_key is null), count(*) filter (where value is null), count(*) filter (where breakdown_json is null) from build",
            ),
            "unmatched_geography": one(con, "select count(*) from build where geography_key = '_X'"),
            "pl_in_dim_geography": one(con, "select count(*) from dim_geo where geography_key = 'PL'"),
            "frequency_dates": q(
                con,
                "select frequency, min(date_key), max(date_key), count(distinct date_key), min(period_label) from build group by 1",
            ),
            "measures": q(
                con, "select measure_key, count(*), sum(value) filter (where is_total) from build group by 1 order by 1"
            ),
            "zero_values": one(con, "select count(*) from build where value = 0"),
            "spot_pl": q(
                con,
                """select measure_codes, json_extract_string(breakdown_json, '$.entity_type') t, json_extract_string(breakdown_json, '$.office_type') o, value
            from build where (t = '_T' and o = '_T') or (t in ('MIF', 'AIF', 'UCI') and o = '_T') order by 1, 2""",
            ),
            "yml_tests": yml_tests(con, gold_dir / "_esma_registers.yml", "poc_build_esma_registers", "build"),
        },
    )

    seeds = ROOT / "warehouse/seeds"

    def rows_of(name):
        with open(seeds / f"{name}.csv", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    from warehouse import publish

    src, lic, ms = rows_of("source"), rows_of("licence"), rows_of("kpi_measure")
    row = next(x for x in src if x["source_key"] == SOURCE)
    licensed = publish.licensed_sources(src, lic)
    kpi_rows = [m for m in ms if m["source_system"] == SOURCE]
    pattern = kpi_rows[0]["measure_key_pattern"]
    bound = re.search(
        r"\('capital_market_entities', 'esma_registers', '%', ([0-9.e]+), ([0-9.e]+)\)",
        (ROOT / "warehouse/tests/metrics_value_range.sql").read_text(),
    )
    lo, hi = float(bound.group(1)), float(bound.group(2))
    metrics = f"select source_system, geography_key, date_key, value from build where is_total and geography_key <> '_X' and measure_key like '{pattern}'"
    REAL(
        "notice",
        "esma_registers gold entity table, KPI rows, licence gate",
        {
            "entity_rows_two_builds": counts["entity"],
            "entity_yml_tests": yml_tests(
                con, gold_dir / "_esma_registers.yml", "supervised_entity_esma_registers", "entity"
            ),
            "entity_per_status_lei": q(
                con,
                "select status, count(*), count(lei), count(head_office_lei), count(valid_from), count(valid_to) from entity group by 1 order by 1",
            ),
            "entity_sample": q(
                con,
                "select entry_id, entity_type_code, office_type, status, lei is not null, valid_from, valid_to from entity order by entry_id limit 3",
            ),
            "metrics_rows_per_source_area_period_max": one(
                con, f"select coalesce(max(n), 0) from (select count(*) n from ({metrics}) group by all)"
            ),
            "metrics_values": q(con, metrics),
            "bound": [lo, hi],
            "metrics_out_of_bound": one(con, f"select count(*) from ({metrics}) where value < {lo} or value > {hi}"),
            "licence_gate": {
                "source_row": row,
                "licence_row": [x for x in lic if x["licence_key"] == row["licence_key"]],
                "licensed": SOURCE in licensed,
                "kpi_blocked": {
                    k: publish.licence_blocked(k, ms, licensed) for k in sorted({m["kpi_key"] for m in kpi_rows})
                },
            },
            "head": os.environ.get("GITHUB_SHA", "")[:12],
        },
    )


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
    for var, label in (
        ("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
        ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects"),
    ):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
    REAL("notice", "esma_registers scratch removed", removed)


def main() -> int:
    failed = 0
    try:
        try:
            legal_notice()
        except Exception as exc:  # noqa: BLE001
            REAL("warning", "esma_registers licence page failed", {"error": f"{type(exc).__name__}: {str(exc)[:500]}"})
        try:
            trial()
        except Exception as exc:  # noqa: BLE001
            import traceback

            traceback.print_exc()
            failed = 1
            REAL("error", "esma_registers trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
