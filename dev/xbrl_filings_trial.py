"""Dev trial (CD-013 filings.xbrl.org, #1500): Landing → Bronze → Silver → Gold into scratch.

Landing extract into the Landing prefix zz_dev_xbrl_filings/ (twice: the second run must write nothing), verify,
Bronze load into Iceberg namespace zz_dev_xbrl_filings and lake prefix zz_dev_xbrl_filings/ (twice: the second
appends nothing), then the Silver and Gold models rendered with Jinja from warehouse/models in local DuckDB, each
built twice, with the dbt tests' checks. Everything written is removed in `finally`. At most 10 annotations.
Optional argument: the number of facts documents to read per extract run (a short shake-down; the proof has none).
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
import re
import sys
import time

import duckdb
from jinja2 import Environment
import requests

from ingestion.common import bulk_bronze
from ingestion.common import report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.xbrl_filings import load as bronze_load
from ingestion.to_landing import xbrl_filings as landing
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_xbrl_filings"
_annotate = report.annotate
captured: list = []


def annotate(level, title, data):
    print(json.dumps({"annotation": title, "data": data}, default=str), flush=True)
    _annotate(level, title, data)


def capture(level, title, data):
    captured.append([level, title, data])
    print(json.dumps({"captured": [level, title, data]}, default=str)[:6000], flush=True)


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return Environment().from_string(Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t]
    )


def terms() -> None:
    out = {}
    for name, url, pattern in (("about", f"{landing.BASE}/docs/about", r"Terms of use.{0,200}|.{0,10}XBRL All Rights Reserved\."),
                               ("api", f"{landing.BASE}/docs/api", r"Access to the API is provided.{0,420}")):
        time.sleep(landing.PAUSE)
        r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
        text = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", r.text))
        out[name] = {"status": r.status_code, "final_url": r.url, "quotes": re.findall(pattern, text)[:3]}
    out["user_agent_sent"] = bulk_file.USER_AGENT
    src = [r for r in csv.DictReader(open("warehouse/seeds/source.csv", encoding="utf-8")) if r["source_key"] == "xbrl_filings"]
    lic = {r["licence_key"]: r for r in csv.DictReader(open("warehouse/seeds/licence.csv", encoding="utf-8"))}
    out["licence_gate"] = {"source_row": src, "licence_row": lic.get(src[0]["licence_key"]) if src else None}
    annotate("notice", "xbrl_filings terms and licence row", out)


def objects(bucket_var: str) -> list:
    client, bucket = s3(), req(bucket_var)
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
        found += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return found


def trial(limit: int) -> None:  # noqa: PLR0915
    report.annotate = bulk_file.annotate = bulk_bronze.annotate = capture
    started = time.monotonic()
    exits = [landing.extract(300, landing_root=SCRATCH, max_items=limit)]
    first_seconds = round(time.monotonic() - started)
    before = objects("STORAGE_LANDING_BUCKET_NAME")
    exits.append(landing.extract(300, landing_root=SCRATCH, max_items=limit))
    after = objects("STORAGE_LANDING_BUCKET_NAME")
    extract_summaries = [c[2] for c in captured if c[1] == "xbrl_filings landing extract"]
    held = landing.held_versions(s3(), req("STORAGE_LANDING_BUCKET_NAME"), SCRATCH)
    verify = bulk_file.main(landing.SOURCE, "verify", landing_root=SCRATCH, specs=[{"entity": e} for e in sorted(held)])
    data = [o for o in after if not o[0].endswith(".receipt.json")]
    facts_sizes = sorted(s for k, s in data if "/facts_" in k)
    annotate("notice", "xbrl_filings landing (scratch)", {
        "limit_per_run": limit, "extract_exit_two_runs": exits, "first_run_seconds": first_seconds,
        "run1": extract_summaries[0] if extract_summaries else None, "run2": extract_summaries[1] if len(extract_summaries) > 1 else None,
        "objects_after_run1": len(before), "objects_after_run2": len(after), "second_run_new_objects": len(set(after) - set(before)),
        "data_files": len(data), "data_bytes": sum(s for _, s in data), "list_bytes": [s for k, s in data if "/filings_pl/" in k],
        "facts_files": len(facts_sizes), "facts_bytes": sum(facts_sizes),
        "facts_size_min_median_max": [facts_sizes[0], facts_sizes[len(facts_sizes) // 2], facts_sizes[-1]] if facts_sizes else None,
        "entities_held": len(held), "versions_per_entity_max": max(len(v) for v in held.values()), "verify_exit": verify})
    captured.clear()
    bronze_exit = [bronze_load.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    loads = [c[2] for c in captured if "Bronze from Landing" in c[1]]
    cat = catalog()
    tf = cat.load_table((SCRATCH, "xbrl_filings_filings"))
    tx = cat.load_table((SCRATCH, "xbrl_filings_facts"))
    a_f, a_x = tf.scan().to_arrow(), tx.scan().to_arrow()
    try:
        rejected = cat.load_table((SCRATCH, "xbrl_filings_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    con = duckdb.connect()
    con.register("bronze_filings", a_f)
    con.register("bronze_facts", a_x)
    reasons: dict = {}
    for r in rejected:
        reasons[r.get("error", "")[:80]] = reasons.get(r.get("error", "")[:80], 0) + 1
    annotate("notice", "xbrl_filings bronze (scratch)", {
        "bronze_exit": bronze_exit, "loads": loads, "filings_files": len(list(tf.scan().plan_files())), "facts_files": len(list(tx.scan().plan_files())),
        "filings_rows": a_f.num_rows, "facts_rows": a_x.num_rows, "rejected_files": len(rejected), "rejected_reasons": reasons,
        "rejected_sample": [r.get("entity") for r in rejected][:4],
        "filings": q(con, "select count(*), count(distinct fxo_id), count(distinct entity_identifier), count(*) filter (where entity_identifier is null), count(*) filter (where entity_name is null), count(*) filter (where json_url is null), min(period_end), max(period_end), min(date_added), max(date_added), count(*) filter (where published is not null) from bronze_filings"),
        "names_per_lei_max": q(con, "select max(n) from (select count(distinct entity_name) n from bronze_filings group by entity_identifier)"),
        "facts_entities": q(con, "select count(distinct entity), count(distinct fxo_id), count(*) filter (where 'facts_' || fxo_id <> entity) from bronze_facts"),
        "facts_by_concept_dim": q(con, "select concept, extra_dimensions = '{}' as plain, count(*), count(distinct fxo_id) from bronze_facts group by all order by all"),
        "units": q(con, "select unit, count(*), count(distinct fxo_id) from bronze_facts group by 1 order by 2 desc limit 8"),
        "units_plain": q(con, "select unit, count(*), count(distinct fxo_id) from bronze_facts where extra_dimensions = '{}' group by 1 order by 2 desc limit 8"),
        "period_shape": q(con, "select regexp_replace(period, '[0-9]', '9', 'g'), count(*) from bronze_facts group by 1 order by 2 desc limit 5"),
        "value_not_numeric": q(con, "select count(*) filter (where value is null), count(*) filter (where value is not null and try_cast(value as double) is null), (list(distinct value) filter (where value is not null and try_cast(value as double) is null))[1:5] from bronze_facts"),
        "decimals": q(con, "select decimals, count(*) from bronze_facts group by 1 order by 2 desc limit 8"),
        "language_not_null": q(con, "select count(*) from bronze_facts where language is not null"),
        "entity_scheme": q(con, "select regexp_replace(entity_scheme_id, '[A-Z0-9]{20}$', 'LEI'), count(*), count(*) filter (where right(entity_scheme_id, 20) <> left(fxo_id, 20)) from bronze_facts group by 1 order by 2 desc limit 4"),
        "extra_dim_keys": q(con, "select k, count(*) from (select unnest(json_keys(extra_dimensions::json)) k from bronze_facts) group by 1 order by 2 desc limit 6")})

    s_fil = render("warehouse/models/silver/xbrl_filings/xbrl_filings_filings.sql", {}, {"xbrl_filings_filings": "bronze_filings"})
    s_fac = render("warehouse/models/silver/xbrl_filings/xbrl_filings_facts.sql", {"xbrl_filings_filings": "s_filings"}, {"xbrl_filings_facts": "bronze_facts"})
    gold = {
        "esef_filing": render("warehouse/models/gold/esef_filing.sql", {"xbrl_filings_filings": "s_filings"}, {}),
        "esef_issuer": render("warehouse/models/gold/esef_issuer.sql", {"xbrl_filings_filings": "s_filings"}, {}),
        "esef_financial_fact": render("warehouse/models/gold/esef_financial_fact.sql", {"xbrl_filings_facts": "s_facts"}, {}),
        "build": render("warehouse/models/gold/poc_build_xbrl_filings.sql", {"xbrl_filings_filings": "s_filings"}, {}),
    }
    counts: dict = {}
    for _ in (1, 2):
        con.execute(f"create or replace table s_filings as {s_fil}")
        con.execute(f"create or replace table s_facts as {s_fac}")
        for name, sql in gold.items():
            con.execute(f"create or replace table {name} as {sql}")
        for t in ("s_filings", "s_facts", *gold):
            counts.setdefault(t, []).append(con.execute(f"select count(*) from {t}").fetchone()[0])

    def test(path: str, refs: dict) -> list:
        return q(con, Environment().from_string(Path(path).read_text()).render(config=lambda **_k: "", ref=lambda n: refs.get(n, n)))

    annotate("notice", "xbrl_filings silver filings", {
        "rows_two_builds": counts["s_filings"],
        "dup_fxo_id": q(con, "select count(*) from (select fxo_id from s_filings group by 1 having count(*) > 1)"),
        "null_keys": q(con, "select count(*) filter (where fxo_id is null), count(*) filter (where lei is null), count(*) filter (where issuer_name is null), count(*) filter (where period_end is null), count(*) filter (where date_added is null), count(*) filter (where package_sha256 is null), count(*) filter (where raw_sha256 is null), count(*) filter (where filing_sequence is null), count(*) filter (where processed_at is null) from s_filings"),
        "country_standard": q(con, "select country_code, report_standard, count(*) from s_filings group by all"),
        "lei_test_rows": len(test("warehouse/tests/xbrl_filings_lei.sql", {"xbrl_filings_filings": "s_filings"})),
        "by_period_year": q(con, "select year(period_end), count(*), count(distinct lei), count(*) filter (where has_json) from s_filings group by 1 order by 1"),
        "period_end_month_day": q(con, "select strftime(period_end, '%m-%d'), count(*) from s_filings group by 1 order by 2 desc limit 8"),
        "sequence": q(con, "select filing_sequence, count(*) from s_filings group by 1 order by 1"),
        "filings_for_period": q(con, "select filings_for_period, count(*), count(distinct lei || period_end::varchar) from s_filings group by 1 order by 1"),
        "multi_groups_by_gap": q(con, "select gap, count(*) from (select case when max(date_added) - min(date_added) < interval 1 day then 'within a day' else 'different days' end as gap from s_filings where filings_for_period > 1 group by lei, period_end) group by 1 order by 1"),
        "date_added_range": q(con, "select min(date_added), max(date_added), min(processed_at), max(processed_at) from s_filings"),
        "validation": q(con, "select count(*) filter (where error_count > 0), count(*) filter (where warning_count > 0), count(*) filter (where inconsistency_count > 0), max(error_count), max(warning_count) from s_filings"),
        "distinct": q(con, "select count(distinct lei), count(distinct lei || '|' || period_end::varchar) from s_filings")})
    conflicts = test("warehouse/tests/xbrl_filings_fact_conflicts.sql", {"xbrl_filings_facts": "s_facts"})
    conflicts_sql = Environment().from_string(Path("warehouse/tests/xbrl_filings_fact_conflicts.sql").read_text()).render(config=lambda **_k: "", ref=lambda _n: "s_facts")
    annotate("notice", "xbrl_filings silver facts", {
        "rows_two_builds": counts["s_facts"],
        "dup_grain": q(con, "select count(*) from (select 1 from s_facts group by fxo_id, concept, period_start, period_end, unit_code having count(*) > 1)"),
        "null_keys": q(con, "select count(*) filter (where fxo_id is null), count(*) filter (where lei is null), count(*) filter (where concept is null), count(*) filter (where period_start is null), count(*) filter (where period_end is null), count(*) filter (where unit_code is null), count(*) filter (where raw_sha256 is null), count(*) filter (where value is null) from s_facts"),
        "bronze_rows_left_out": q(con, "select count(*) filter (where extra_dimensions <> '{}') as dimensional, count(*) filter (where extra_dimensions = '{}' and not contains(period, '/')) as instant, count(*) filter (where extra_dimensions = '{}' and unit is null) as no_unit, count(*) filter (where extra_dimensions = '{}' and try_cast(value as double) is null) as no_number, count(*) filter (where fxo_id not in (select fxo_id from s_filings)) as not_in_list from bronze_facts"),
        "by_concept_unit": q(con, "select concept, unit_code, count(*), count(distinct fxo_id), count(distinct lei), count(*) filter (where is_reporting_period), count(*) filter (where values_distinct > 1) from s_facts group by all order by all"),
        "facts_per_key": q(con, "select facts, count(*) from s_facts group by 1 order by 1 limit 8"),
        "period_days": q(con, "select case when period_end - period_start between 360 and 370 then 'year' when period_end - period_start < 360 then 'shorter' else 'longer' end, count(*), count(*) filter (where is_reporting_period) from s_facts group by 1 order by 1"),
        "period_end_years": q(con, "select year(period_end), count(*) from s_facts group by 1 order by 1"),
        "filings_with_facts": q(con, "select count(distinct fxo_id), count(distinct fxo_id) filter (where concept = 'ifrs-full:Revenue'), count(distinct fxo_id) filter (where concept = 'ifrs-full:ProfitLoss'), count(distinct fxo_id) filter (where is_reporting_period) from s_facts"),
        "value_range": q(con, "select concept, min(value), max(value), count(*) filter (where value = 0), count(*) filter (where value < 0) from s_facts group by 1 order by 1"),
        "decimals": q(con, "select decimals, count(*) from s_facts group by 1 order by 2 desc limit 6"),
        "candidates": q(con, "select reporting_period_candidates, count(*) from s_facts group by 1 order by 1"),
        "conflict_test_rows": len(conflicts), "conflicts_by_reason": q(con, "select reason, concept, count(*), count(distinct fxo_id) from (" + conflicts_sql + ") group by all order by all"), "conflict_sample": conflicts[:4]})
    differ = test("warehouse/tests/esef_issuer_period_values_differ.sql", {"esef_financial_fact": "esef_financial_fact"})
    annotate("notice", "xbrl_filings gold entity tables", {
        "rows_two_builds": {k: counts[k] for k in ("esef_filing", "esef_issuer", "esef_financial_fact")},
        "dup_keys": q(con, "select (select count(*) from (select fxo_id from esef_filing group by 1 having count(*) > 1)), (select count(*) from (select lei from esef_issuer group by 1 having count(*) > 1)), (select count(*) from (select 1 from esef_financial_fact group by fxo_id, concept, unit_code having count(*) > 1))"),
        "null_keys": q(con, "select (select count(*) from esef_filing where fxo_id is null or lei is null or period_end is null), (select count(*) from esef_issuer where lei is null or issuer_name is null), (select count(*) from esef_financial_fact where fxo_id is null or lei is null or value is null or measure is null or unit_code is null)"),
        "issuer_filings": q(con, "select sum(filings), min(first_period_end), max(last_period_end), max(filings), max(period_ends) from esef_issuer"),
        "facts_by_measure_currency": q(con, "select measure, currency_code, count(*), count(distinct fxo_id), count(distinct lei) from esef_financial_fact group by all order by all"),
        "facts_by_year": q(con, "select period_end_year, measure, count(*), count(distinct lei) from esef_financial_fact group by all order by all"),
        "period_days": q(con, "select case when period_end - period_start between 360 and 370 then 'year' when period_end - period_start < 360 then 'shorter' else 'longer' end, count(*) from esef_financial_fact group by 1 order by 1"),
        "filings_with_json_without_gold_value": q(con, "select count(*) filter (where fxo_id not in (select fxo_id from esef_financial_fact where measure = 'revenue')), count(*) filter (where fxo_id not in (select fxo_id from esef_financial_fact where measure = 'profit_loss')), count(*) from esef_filing where has_json"),
        "by_filings_for_period": q(con, "select filings_for_period, count(*), count(distinct fxo_id), count(distinct lei) from esef_financial_fact group by 1 order by 1"),
        "facts_lei_not_in_issuer": q(con, "select count(*) from esef_financial_fact where lei not in (select lei from esef_issuer)"),
        "issuer_period_values_differ_rows": len(differ), "differ_sample": differ[:5],
        "multi_filing_groups_with_facts_agreeing": q(con, "select count(*) from (select lei, concept, period_start, period_end, unit_code from esef_financial_fact group by all having count(*) > 1 and count(distinct value) = 1)")})
    annotate("notice", "xbrl_filings gold build stage", {
        "rows_two_builds": counts["build"],
        "dup_series_date": q(con, "select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json as series_key, date_key from build group by all having count(*) > 1)"),
        "series": q(con, "select count(distinct measure_key || '|' || geography_key || '|' || breakdown_json), count(distinct measure_key), min(date_key), max(date_key) from build"),
        "nulls": q(con, "select count(*) from build where value is null or date_key is null or measure_key is null or raw_sha256 is null"),
        "distinct": q(con, "select distinct source_system, block_id, unit_code, geography_key, frequency, observation_status_key, is_total from build order by all"),
        "rows": q(con, "select measure_key, period_label, date_key, value from build order by 1, 2"),
        "check_against_silver": q(con, "select count(*), count(distinct lei) from s_filings")})
    names = ["ORLEN", "POWSZECHNA KASA", "KGHM", "CD PROJEKT", "POWSZECHNY ZAK", "ALLEGRO", "DINO POLSKA", "LPP"]
    spot = {}
    for name in names:
        spot[name] = q(con, f"""select i.lei, f.fxo_id, f.measure, f.period_end, f.value, f.currency_code, f.decimals
            from esef_issuer i left join esef_financial_fact f on f.lei = i.lei
            where upper(i.issuer_name) like '%{name}%' order by f.period_end desc nulls last, f.measure, f.fxo_id limit 4""")
    annotate("notice", "xbrl_filings spot records", {
        "issuer_rows": q(con, "select lei, issuer_name, filings, first_period_end, last_period_end from esef_issuer where " + " or ".join(f"upper(issuer_name) like '%{n}%'" for n in names) + " order by 2"),
        "facts": spot})


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
    removed["objects_left"] = len(objects("STORAGE_LANDING_BUCKET_NAME")) + len(objects("STORAGE_LAKEHOUSE_BUCKET_NAME"))
    annotate("notice", "scratch removed", removed)


def main() -> int:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    failed = 0
    try:
        try:
            terms()
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"terms_error": str(exc)[:300]}))
        try:
            trial(limit)
        except Exception as exc:  # noqa: BLE001
            import traceback

            traceback.print_exc()
            failed = 1
            annotate("error", "xbrl_filings trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
