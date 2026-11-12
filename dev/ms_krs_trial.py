"""Dev trial (PLG-012 Open KRS API, #1362): Landing → Bronze → Silver → Gold into scratch, removed in `finally`.

Arguments: [upto KRS number, default 0000030000].
Landing (prefix zz_dev_ms_krs/): run 1 with the run date set three days back (so that the bulletin route has
days to read afterwards), run 2 the same (must write nothing), then run 3 and run 4 with the real date (run 3 asks
again for held numbers listed in the bulletins of the last three days; run 4 must write nothing). Bronze into
Iceberg namespace zz_dev_ms_krs after run 2 (twice) and after run 4 (twice). Silver and Gold rendered with Jinja
from warehouse/models in local DuckDB, built twice, with the dbt tests' checks. gold.dim_geography and
silver.gleif_legal_entities are read from production (read-only). At most 10 compact annotations.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import sys

import duckdb
from jinja2 import Environment
import requests

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.ms_krs import load as bronze_load
from ingestion.to_landing import ms_krs as landing
from ingestion.to_landing.ms_krs import extract
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_ms_krs"
SPOT = {"0000028860": "ORLEN", "0000026438": "PKO BP", "0000023302": "KGHM", "0000006865": "CD PROJEKT",
        "0000009831": "PZU", "0000635012": "ALLEGRO"}
captured: list = []


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return Environment().from_string(Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs[n], source=lambda _s, t: sources[t]
    )


def terms() -> None:
    out = {}
    for url in ("https://dane.gov.pl/pl/dataset/27606", "https://api.dane.gov.pl/1.4/datasets/27606"):
        try:
            r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(15, 40))
            text = re.sub(r"\s+", " ", re.sub(r"(?s)<script.*?</script>|<[^>]+>", " ", r.text))
            hits = [text[max(0, m.start() - 150): m.start() + 250] for m in re.finditer(r"(?i)licen[cs]j|warunki ponown|CC BY|CC0|license_", text)][:3]
            out[url] = {"status": r.status_code, "bytes": len(r.content), "hits": hits}
        except requests.RequestException as exc:
            out[url] = {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    annotate("notice", "ms_krs terms pages (dane.gov.pl)", out)


def run(upto: str, today=None) -> dict:
    extract.extract(60.0, landing_root=SCRATCH, upto=upto, today=today)
    keep = ("selection", "held_before", "held_after", "bulletin_days", "bulletin_days_unread", "listed_again",
            "asked_again", "bulletin_settled", "missing_before", "asked_new", "remaining", "answers", "requests",
            "parts_landed", "seconds", "error")
    return {k: captured[-1].get(k) for k in keep if k in captured[-1]}


def landing_state(client, bucket) -> dict:
    copies, size, lines, bad_keys, pesel_like, longest, through = 0, 0, 0, 0, 0, 0, set()
    for spec in landing.specs():
        held = bulk_file.landed(client, bucket, bulk_file.prefix(landing.SOURCE, spec["entity"], SCRATCH))
        copies += len(held)
        if not held:
            continue
        size += held[-1]["size_bytes"]
        through.add(held[-1]["detail"]["bulletin_through"])
        body = bulk_bronze.read_raw(client, bucket, landing.SOURCE, held[-1])
        for line in body.decode("utf-8").splitlines():
            lines += 1
            longest = max(longest, len(line))
            record = json.loads(line)
            bad_keys += bool(set(record) - set(extract.FIELDS))
            pesel_like += sum(1 for k, v in record.items() if k != "krs_number" and isinstance(v, str) and re.fullmatch(r"\d{11}", v))
    return {"copies": copies, "newest_bytes": size, "newest_lines": lines, "lines_with_keys_outside_allowlist": bad_keys,
            "values_of_11_digits": pesel_like, "longest_line_chars": longest, "bulletin_through": sorted(map(str, through))}


def bronze(cat) -> dict:
    exits = [bronze_load.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    table = cat.load_table((SCRATCH, "ms_krs_extracts"))
    return {"exit_two_loads": exits, "files": len(list(table.scan().plan_files())), "rows": table.scan(selected_fields=("krs_number",)).to_arrow().num_rows}


def trial(upto: str) -> None:  # noqa: PLR0915
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    real = datetime.now(extract.WARSAW).date()
    back = real - timedelta(days=3)
    runs = {"run1_date_3_days_back": run(upto, back), "run2_same_date": run(upto, back)}
    state2 = landing_state(client, bucket)
    cat = catalog()
    bronze_a = bronze(cat)
    runs["run3_real_date"] = run(upto)
    runs["run4_real_date"] = run(upto)
    verify = bulk_file.main(landing.SOURCE, "verify", landing_root=SCRATCH, specs=landing.specs())
    state4 = landing_state(client, bucket)
    annotate("notice", "ms_krs landing (scratch)", {"upto": upto, "real_date": str(real), **runs, "verify_exit": verify,
             "after_run2": state2, "after_run4": state4})
    bronze_b = bronze(cat)
    table = cat.load_table((SCRATCH, "ms_krs_extracts"))
    try:
        rejected = cat.load_table((SCRATCH, "ms_krs_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    con = duckdb.connect()
    con.register("bronze_arrow", table.scan().to_arrow())
    con.execute("create table bronze_b as select * from bronze_arrow")
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan().to_arrow())
    con.execute("create table geo as select * from geo_a")
    gleif = cat.load_table(("silver", "gleif_legal_entities")).scan(
        row_filter=f"registration_authority_id == '{extract.KRS_AUTHORITY}'",
        selected_fields=("lei", "registration_authority_entity_id", "legal_name")).to_arrow()
    con.register("gleif_a", gleif)
    con.execute("create table gleif as select * from gleif_a")
    annotate("notice", "ms_krs bronze (scratch)", {
        "after_run2": bronze_a, "after_run4": bronze_b, "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3],
        "rows_per_entity_version": q(con, "select entity, count(distinct raw_sha256), count(*) from bronze_b group by 1 order by 1"),
        "http_status": q(con, "select http_status, count(*) from bronze_b group by 1 order by 1"),
        "null_krs": q(con, "select count(*) from bronze_b where krs_number is null"),
        "columns": len(con.execute("select * from bronze_b limit 0").description)})

    macros = Environment().from_string(Path("warehouse/macros/sources/ms_krs.sql").read_text()).module
    con.execute(f"create table map as with {macros.poc_geography__ms_krs()} select * from ms_krs")
    silver = render("warehouse/models/silver/ms_krs/ms_krs_entities.sql", {}, {"ms_krs_extracts": "bronze_b"})
    refs = {"ms_krs_entities": "silver_e", "poc_map_geography": "map"}
    entity = render("warehouse/models/gold/krs_entity.sql", refs, {})
    build = render("warehouse/models/gold/poc_build_ms_krs.sql", refs, {})
    counts: dict = {"silver_e": [], "gold_e": [], "gold_b": []}
    for _ in (1, 2):
        con.execute(f"create or replace table silver_e as {silver}")
        con.execute(f"create or replace table gold_e as {entity}")
        con.execute(f"create or replace table gold_b as {build}")
        for name, rows in counts.items():
            rows.append(con.execute(f"select count(*) from {name}").fetchone()[0])
    newest = "(select * from bronze_b qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1)"
    annotate("notice", "ms_krs silver ms_krs_entities", {
        "rows_two_builds": counts["silver_e"], "bronze_rows_all_versions": q(con, "select count(*) from bronze_b"),
        "dup_krs_number": q(con, "select count(*) from (select krs_number from silver_e group by 1 having count(*) > 1)"),
        "null_keys[krs,has_extract,requested_on,raw_sha256]": q(con, "select count(*) filter (where krs_number is null), count(*) filter (where has_current_extract is null), count(*) filter (where requested_on is null), count(*) filter (where raw_sha256 is null) from silver_e"),
        "has_current_extract": q(con, "select has_current_extract, count(*) from silver_e group by 1 order by 1"),
        "register": q(con, "select register, count(*) from silver_e group by 1 order by 1"),
        "requested_on": q(con, "select requested_on, count(*) from silver_e group by 1 order by 1"),
        "with_extract_nulls[name,legal_form,nip,regon,voivodeship,registered_on,last_entry_on,share_capital,pkd]": q(con, "select count(*) filter (where name is null), count(*) filter (where legal_form is null), count(*) filter (where nip is null), count(*) filter (where regon is null), count(*) filter (where seat_voivodeship is null), count(*) filter (where registered_on is null), count(*) filter (where last_entry_on is null), count(*) filter (where share_capital is null), count(*) filter (where main_pkd_code is null) from silver_e where has_current_extract"),
        "unparsed[registered_on,last_entry_on,state_as_of,share_capital,last_entry_number]": q(con, f"select count(*) filter (where b.registered_on is not null and s.registered_on is null), count(*) filter (where b.last_entry_on is not null and s.last_entry_on is null), count(*) filter (where b.state_as_of is not null and s.state_as_of is null), count(*) filter (where b.share_capital is not null and s.share_capital is null), count(*) filter (where b.last_entry_number is not null and s.last_entry_number is null) from {newest} as b join silver_e as s using (krs_number)"),
        "dates[min,max registered; min,max last entry]": q(con, "select min(registered_on), max(registered_on), min(last_entry_on), max(last_entry_on) from silver_e"),
        "regon_lengths": q(con, "select length(regon), count(*) from silver_e group by 1 order by 1"),
        "nip_lengths": q(con, "select length(nip), count(*) from silver_e group by 1 order by 1"),
        "currency": q(con, "select share_capital_currency, count(*) from silver_e group by 1 order by 2 desc limit 5"),
        "main_pkd_count": q(con, "select main_pkd_count, count(*) from silver_e group by 1 order by 1"),
        "flags[liquidation,bankruptcy,restructuring,dissolution,opp]": q(con, "select count(*) filter (where in_liquidation), count(*) filter (where in_bankruptcy), count(*) filter (where has_restructuring_entry), count(*) filter (where has_dissolution_entry), count(*) filter (where has_opp_status) from silver_e"),
        "position_state": q(con, f"select position_state, count(*) from {newest} group by 1 order by 1"),
        "extract_kind": q(con, f"select extract_kind, count(*) from {newest} group by 1 order by 1")})
    annotate("notice", "ms_krs silver closed lists", {
        "legal_form": q(con, "select legal_form, count(*) from silver_e where has_current_extract group by 1 order by 2 desc"),
        "seat_country": q(con, "select seat_country, count(*) from silver_e where has_current_extract group by 1 order by 2 desc limit 6"),
        "seat_voivodeship": q(con, "select seat_voivodeship, count(*) from silver_e where has_current_extract group by 1 order by 2 desc")})
    annotate("notice", "ms_krs gold krs_entity", {
        "rows_two_builds": counts["gold_e"],
        "dup_krs_number": q(con, "select count(*) from (select krs_number from gold_e group by 1 having count(*) > 1)"),
        "null[krs,register,name]": q(con, "select count(*) filter (where krs_number is null), count(*) filter (where register is null), count(*) filter (where name is null) from gold_e"),
        "map_rows_and_dup_codes": q(con, "select count(*), count(*) - count(distinct source_code) from map"),
        "map_keys": q(con, "select source_code, geography_key from map order by 2"),
        "seat_key_null_by_voivodeship": q(con, "select seat_voivodeship, count(*) from gold_e where seat_geography_key is null group by 1 order by 2 desc limit 8"),
        "seat_keys_not_in_dim_geography": q(con, "select count(*) from gold_e where seat_geography_key is not null and seat_geography_key not in (select geography_key from geo)"),
        "gleif_join[entities with an LEI, without, max LEIs per number]": q(con, "select count(*) filter (where n > 0), count(*) filter (where n = 0), max(n) from (select e.krs_number, count(g.lei) as n from gold_e as e left join gleif as g on g.registration_authority_entity_id = e.krs_number group by 1)"),
        "columns": [d[0] for d in con.execute("select * from gold_e limit 0").description]})
    annotate("notice", "ms_krs gold spot records", {
        "spot": q(con, f"select krs_number, name, legal_form, nip, regon, seat_voivodeship, seat_geography_key, seat_town, registered_on, last_entry_on, share_capital, share_capital_currency, main_pkd_code, in_liquidation, in_bankruptcy from gold_e where krs_number in ({','.join(repr(k) for k in SPOT)}) order by 1"),
        "expected": SPOT,
        "spot_without_extract": q(con, f"select krs_number, has_current_extract from silver_e where krs_number in ({','.join(repr(k) for k in SPOT)}) and not has_current_extract")})
    from warehouse import publish

    licensed = publish.licensed_sources(publish.seed("source"), publish.seed("licence"))
    annotate("notice", "ms_krs gold build_ms_krs", {
        "rows_two_builds": counts["gold_b"],
        "dup_series_x_date": q(con, "select count(*) from (select measure_key, geography_key, breakdown_json, date_key from gold_b group by all having count(*) > 1)"),
        "null[measure,geography,value,date]": q(con, "select count(*) filter (where measure_key is null), count(*) filter (where geography_key is null), count(*) filter (where value is null), count(*) filter (where date_key is null) from gold_b"),
        "dates_frequency_units": q(con, "select date_key, frequency, unit_code, count(*), min(value), max(value) from gold_b group by all"),
        "pl_total_vs_entity_rows": q(con, "select (select value from gold_b where geography_key = 'PL' and is_total), (select count(*) from gold_e)"),
        "voivodeship_totals": q(con, "select geography_source_code, geography_key, value from gold_b where is_total order by 3 desc"),
        "sum_voivodeships_vs_pl": q(con, "select sum(value) filter (where geography_key <> 'PL'), sum(value) filter (where geography_key = 'PL') from gold_b where is_total"),
        "sum_forms_vs_total_pl": q(con, "select sum(value) filter (where not is_total), sum(value) filter (where is_total) from gold_b where geography_key = 'PL'"),
        "geography_keys_not_in_dim": q(con, "select distinct geography_key from gold_b where geography_key not in (select geography_key from geo)"),
        "sample": q(con, "select geography_key, breakdown_json, value from gold_b where geography_key = 'PL' order by value desc limit 6"),
        "licence_gate": {"source_csv": [s for s in publish.seed("source") if s["source_key"] == "ms_krs"],
                         "ms_krs_in_licensed_sources": "ms_krs" in licensed}})


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
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
    annotate("notice", "scratch removed", removed)


def main() -> int:
    upto = sys.argv[1] if len(sys.argv) > 1 else "0000030000"
    bulk_file.annotate = bulk_bronze.annotate = lambda *_a, **_k: None  # summaries go to the log only (cap 10)
    extract.annotate = lambda _level, _title, data: captured.append(data)
    failed = 0
    try:
        try:
            terms()
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"terms_error": str(exc)[:300]}))
        try:
            trial(upto)
        except Exception as exc:  # noqa: BLE001
            import traceback

            traceback.print_exc()
            failed = 1
            annotate("error", "ms_krs trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
