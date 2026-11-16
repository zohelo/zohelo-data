"""Dev trial of ulc_airports (#2967): Landing and Bronze into zz_dev_ulc_airports/ (both buckets) and namespace
zz_dev_ulc_airports, each twice, then the branch's Silver, test and Gold SQL applied in memory (Jinja-rendered with
the source's macro file), checks and Poland spot values printed and annotated. Reads silver.teryt_terc (read-only)
for the voivodeship keys. Everything written is removed at the end (finally). Deleted before merge.

The live site first: the first Landing run asks ulc.gov.pl. Since 16 Nov ~05:00 UTC the site answers the runner
with an Incapsula challenge (trial runs 43807334129, 43808333414); when the first run is refused, Landing runs again
twice in replay: the branch's Landing code gets, through a stand-in for requests.get, the exact bytes the site served
to Dev probe 43797214327 (listing and 64 PDFs; dev/ulc_replay/, SHA-256 checked against the probe's log); no further
request reaches ulc.gov.pl. Everything after the HTTP answer (discovery, plan, PDF check, receipts, idempotence,
Bronze, Silver, Gold) runs as in production."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
import re
import sys

import duckdb
import jinja2
import requests

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.ulc_airports import load
from ingestion.to_landing import ulc_airports as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_ulc_airports"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/ulc_airports.sql").read_text()


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=80):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:100] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def receipts(client, bucket) -> int:
    n = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/ulc_airports/"):
        n += sum(o["Key"].endswith(".receipt.json") for o in page.get("Contents", []))
    return n


class Answer:
    """A requests.Response stand-in for one replayed body."""

    def __init__(self, url: str, body: bytes | None) -> None:
        self.url, self.body = url, body or b""
        self.status_code = 200 if body is not None else 404
        self.content, self.text = self.body, self.body.decode("utf-8", "replace")
        self.headers = {"Content-Length": str(len(self.body)), "Content-Type": "application/pdf"}

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def iter_content(self, size):
        for i in range(0, len(self.body), size):
            yield self.body[i : i + size]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


def replay() -> dict[str, bytes]:
    """{URL: bytes} from dev/ulc_replay/ (manifest.json {url: sha256}; each body checked against its SHA-256)."""
    folder = ROOT / "dev/ulc_replay"
    out = {}
    for url, digest in json.loads((folder / "manifest.json").read_text())["bodies"].items():
        body = (folder / f"{digest}.bin").read_bytes()
        if sha256(body).hexdigest() != digest:
            raise RuntimeError(f"replay body does not match its SHA-256: {url}")
        out[url] = body
    return out


def main(argv) -> int:
    mode = "live"
    client = s3()
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    one = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    try:
        idem: dict[str, object] = {}
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt} ({mode})", flush=True)
            idem[f"landing_exit_{attempt}"] = landing.extract(60, landing_root=LAND)
            idem[f"receipts_after_{attempt}"] = receipts(client, landing_bucket)
            if attempt == 1 and idem["landing_exit_1"] and not idem["receipts_after_1"]:
                idem["live_landing"] = "refused (annotation 'ulc_airports landing extract')"
                mode, served = "replay of probe 43797214327", replay()
                requests.get = lambda url, *a, **k: Answer(url, served.get(url))
                for again in (1, 2):
                    print(f"\n##### landing extract {again} ({mode})", flush=True)
                    idem[f"landing_exit_{again}"] = landing.extract(60, landing_root=LAND)
                    idem[f"receipts_after_{again}"] = receipts(client, landing_bucket)
                break
        held = sorted(landing.held_entities(LAND))
        idem["landing_verify_exit"] = bulk_file.main(
            "ulc_airports", "verify", landing_root=LAND, specs=[{"entity": e} for e in held]
        )
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            load.load(60, landing_root=LAND, namespace=SCRATCH)
            table = cat.load_table((SCRATCH, "ulc_airports_rows"))
            idem[f"bronze_rows_after_{attempt}"] = table.scan().to_arrow().num_rows
            idem[f"bronze_snapshots_after_{attempt}"] = len(table.metadata.snapshots)
        rows = cat.load_table((SCRATCH, "ulc_airports_rows")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "ulc_airports_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        con.register("bronze_rows", rows)

        # Bronze profile (look before modelling)
        values = "unnest([value_1, value_2, value_3])"
        profile = {
            "landing_mode": mode,
            **idem,
            "files_landed": len(held),
            "bytes_landed": None,
            "bronze_rows": rows.num_rows,
            "entities": one("select count(distinct entity) from bronze_rows"),
            "entity_range": one("select min(entity) || ' .. ' || max(entity) from bronze_rows"),
            "tables": one("select count(distinct (entity, table_number)) from bronze_rows"),
            "null_keys": one(
                "select count(*) from bronze_rows where entity is null or airport_label is null"
                " or measure_label is null or raw_sha256 is null"
            ),
            "count_cells_not_numeric": one(
                f"select string_agg(v || '=' || c, ' ') from (select v, count(*) c from (select {values} v"
                " from bronze_rows) where not regexp_full_match(v, '[0-9]{{1,3}}( [0-9]{{3}})*') group by 1)"
            ),
            "growth_cells_markers": one(
                "select string_agg(v || '=' || c, ' ') from (select v, count(*) c from (select"
                " unnest([value_4, value_5]) v from bronze_rows) where v not like '%\\%' escape '\\' group by 1)"
            ),
            "period_headers": one(
                "select string_agg(p || '=' || c, '; ') from (select period_header p, count(*) c"
                " from bronze_rows group by 1 order by 1)"
            ),
            "rejected": [(r.get("entity"), (r.get("error") or "")[:100]) for r in rejected],
        }
        size = 0
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing_bucket, Prefix=f"{LAND}/"):
            size += sum(o["Size"] for o in page.get("Contents", []) if not o["Key"].endswith(".json"))
        profile["bytes_landed"] = size
        print("BRONZE", profile, flush=True)
        annotate("notice", "ulc_airports trial landing+bronze", profile)

        # Silver
        silver = ROOT / "warehouse/models/silver/ulc_airports"
        con.sql(
            "create table traffic as "
            + render(silver / "ulc_airports_traffic.sql", {"ulc_airports_rows": "bronze_rows"})
        )
        con.sql(
            "create table airports as "
            + render(silver / "ulc_airports_airports.sql", {"ulc_airports_traffic": "traffic"})
        )
        test = render(ROOT / "warehouse/tests/ulc_airports_traffic_complete.sql", {"ulc_airports_traffic": "traffic"})
        codes = "EPWA EPKK EPGD EPKT EPWR EPPO EPMO EPRZ EPLL EPBY EPSC EPLB EPSY EPZG EPRA _T".split()
        quoted = ", ".join(f"'{c}'" for c in codes)
        stats = {
            "silver_rows": one("select count(*) from traffic"),
            "periods": one(
                "select min(period_date) || ' .. ' || max(period_date) || ' (' || count(distinct period_date) || ')' from traffic"
            ),
            "dup_keys": one(
                "select count(*) from (select 1 from traffic group by period_date, airport_code, measure_code having count(*) > 1)"
            ),
            "null_keys": one(
                "select count(*) from traffic where period_date is null or airport_code is null or measure_code is null"
                " or value is null or raw_sha256 is null or source_entity is null or airport_label is null"
            ),
            "not_accepted": one(
                f"select count(*) from traffic where airport_code not in ({quoted}) or measure_code not in"
                " ('passengers', 'passenger_operations') or unit_code not in ('passengers', 'operations')"
                " or period_quarter not in (1, 2, 3, 4) or value_variants <> 1"
            ),
            "complete_test_failures": one(f"select count(*) from ({test})"),
            "zeros": one("select count(*) from traffic where value = 0"),
            "per_measure": one(
                "select string_agg(measure_code || '=' || c, ' ') from (select measure_code, count(*) c from traffic group by 1)"
            ),
            "values_from_newer_file": one(
                "select count(*) from traffic where source_entity <> period_year || '_q' || period_quarter"
            ),
            "airports_rows": one("select count(*) from airports"),
            "airports_dup_or_null": one(
                "select count(*) - count(distinct airport_code) + count(*) filter (where terc_woj_code is null) from airports"
            ),
        }
        print("SILVER", stats, flush=True)
        annotate("notice", "ulc_airports trial silver", stats)
        show(con, "airports", "select * from airports order by airport_code")
        show(
            con,
            "printed names",
            "select airport_code, string_agg(distinct airport_label, ' / ') from traffic group by 1 order by 1",
        )

        # Gold
        con.register("terc", cat.load_table(("silver", "teryt_terc")).scan().to_arrow())
        con.sql(
            "create table geo as select 'TERC:' || terc_code as geography_key, 'wojewodztwo' as level_name, terc_code"
            " from terc where unit_level = 'wojewodztwo'"
        )
        geo_macro = re.search(r"macro poc_geography__ulc_airports\(\) %\}(.*?)\{% endmacro", MACROS, re.S).group(1)
        geo_sql = jinja2.Environment().from_string(MACROS + geo_macro).render()
        con.sql(f"create table map_geography as with {geo_sql} select * from ulc_airports")
        gold = render(
            ROOT / "warehouse/models/gold/poc_build_ulc_airports.sql",
            {"poc_map_geography": "map_geography", "ulc_airports_traffic": "traffic"},
        )
        con.sql("create table build as " + gold)
        g = {
            "gold_rows": one("select count(*) from build"),
            "dup_series_x_date": one(
                "select count(*) from (select 1 from build group by measure_key || '|' || geography_key || '|' ||"
                " breakdown_json, date_key having count(*) > 1)"
            ),
            "unmatched_geography": one("select count(*) from build where geography_key = '_X'"),
            "geographies": one("select string_agg(distinct geography_key, ' ' order by geography_key) from build"),
            "measures": one("select string_agg(distinct measure_key, ' ') from build"),
            "null_values": one("select count(*) from build where value is null"),
            "out_of_bounds": one(
                "select count(*) from build where is_total and ((measure_codes = 'passengers' and value not between 0 and 5e7)"
                " or (measure_codes = 'passenger_operations' and value not between 0 and 1e6))"
            ),
            "max_total": one(
                "select string_agg(measure_codes || '=' || m, ' ') from (select measure_codes, max(value) m from build where is_total group by 1)"
            ),
        }
        from warehouse import seed_rows

        src = [r for r in seed_rows.rows("source") if r["source_key"] == "ulc_airports"]
        lic = [r for r in seed_rows.rows("licence") if src and r["licence_key"] == src[0]["licence_key"]]
        g["catalogue_row"] = {
            "source_licence_key": src[0]["licence_key"] if src else None,
            "licence": [(r["licence_key"], r["name"], "commercial_ok=" + r["commercial_ok"]) for r in lic],
        }
        spot = one(
            "select string_agg(period_label || ' ' || measure_codes || '=' || cast(value as bigint), '; ' order by"
            " period_label desc, measure_codes) from build where geography_key = 'PL' and is_total and date_key >= '2024-01-01'"
        )
        airports_spot = one(
            "select string_agg(geography_source_code || ' ' || period_label || ' ' || measure_codes || '=' || cast(value as bigint), '; ')"
            " from build where date_key = '2026-01-01' and geography_source_code in ('EPWA', 'EPKK', 'EPRA')"
            " or (date_key = '2019-07-01' and geography_source_code = 'EPWR')"
        )
        g.update(poland=spot, airports=airports_spot)
        print("GOLD", g, flush=True)
        annotate("notice", "ulc_airports trial gold", g)
        show(
            con,
            "gold sample",
            "select measure_key, geography_key, breakdown_json, period_label, value from build order by date_key desc, 1, 2, 3 limit 40",
        )
    finally:
        removed = 0
        for bucket in (landing_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:
            print("drop failed", exc)
        left = sum(
            len(page.get("Contents", []))
            for bucket in (landing_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/")
        )
        annotate(
            "notice",
            "ulc_airports trial cleanup",
            {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
