"""Dev trial (scratch only): topics sources Landing → Bronze → Silver in zz_dev_topics, then removed.

python -m dev.trial_topics news [keep]
Writes: Landing under zz_dev_topics_landing/, Bronze namespace zz_dev_topics (files under zz_dev_topics/).
Everything is deleted at the end (unless 'keep').
"""

from __future__ import annotations

import contextlib
from pathlib import Path
import re
import sys

import duckdb

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

NS = "zz_dev_topics"
LANDING = "zz_dev_topics_landing"
TABLES = {
    "all": [],
    "calendars": ["release_calendar_items", "release_calendars_rejected_files"],
    "news": ["news_items", "news_feeds_rejected_files"],
    "signals": ["wiki_pageviews", "wikimedia_pageviews_rejected_files", "gdelt_timeline", "gdelt_rejected_files"],
}
SEEDS = "warehouse/seeds"


def sql_of(path: str, sources: dict[str, str]) -> str:
    sql = re.sub(r"\{#.*?#\}", "", Path(path).read_text(), flags=re.S)
    for pattern, name in sources.items():
        sql = re.sub(pattern, name, sql)
    return sql


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
    for name in [t for ts in TABLES.values() for t in ts]:
        with contextlib.suppress(Exception):
            cat.drop_table((NS, name))
            gone["tables"] += 1
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    for bucket, prefix in ((req("STORAGE_LANDING_BUCKET_NAME"), LANDING + "/"), (req("STORAGE_LAKEHOUSE_BUCKET_NAME"), NS + "/")):
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
                gone["objects"] += len(keys)
    return gone


def news(cat, client) -> None:
    from ingestion.to_bronze.news_feeds import load

    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    landed = [bulk_file.load_file(client, bucket, "news_feeds", spec, LANDING) for spec in bulk_file.declaration("news_feeds")]
    annotate("notice", "trial news landing", landed)
    load.load(0, landing_root=LANDING, namespace=NS)
    items = cat.load_table((NS, "news_items")).scan().to_arrow()
    con = duckdb.connect()
    con.register("news_items_arrow", items)
    sql = sql_of("warehouse/models/silver/news_feeds/news_headlines.sql",
                 {r"\{\{ source\('news_feeds', 'news_items'\) \}\}": "news_items_arrow"})
    con.execute(f"create table h as {sql}")
    per = con.execute("select outlet_key, count(*), min(published_at)::varchar, max(published_at)::varchar, sum(published_from_ingest::int) from h group by 1 order by 1").fetchall()
    sample = con.execute("select outlet_key, published_at::varchar, title, url from h order by published_at desc limit 12").fetchall()
    dup = con.execute("select count(*) - count(distinct headline_id) from h").fetchone()[0]
    annotate("notice", "trial news silver", {"bronze_rows": items.num_rows, "headlines": con.execute("select count(*) from h").fetchone()[0],
                                            "dup_ids": dup, "per_outlet": per})
    annotate("notice", "trial news sample", sample)
    rej = cat.load_table((NS, "news_feeds_rejected_files")).scan().to_arrow().to_pylist() if (NS, "news_feeds_rejected_files") in cat.list_tables(NS) else []
    annotate("notice", "trial news rejected", rej[:5])


def seeds(con) -> None:
    for name in ("topic", "topic_kpi", "topic_keyword", "topic_signal", "news_outlet", "kpi_v2_map"):
        con.execute(f"create or replace table seed_{name} as select * from read_csv('{SEEDS}/{name}.csv', header=true, all_varchar=true)")
    con.execute("create or replace table seed_topic_keyword as select * replace (cast(weight as double) as weight) from seed_topic_keyword")


def table(cat, name):
    with contextlib.suppress(Exception):
        return cat.load_table((NS, name)).scan().to_arrow()
    return None


def signals(cat, client) -> None:
    from ingestion.to_bronze.gdelt import load as gload
    from ingestion.to_bronze.wikimedia_pageviews import load as wload
    from ingestion.to_landing import gdelt, wikimedia_pageviews

    n = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 4
    wiki_specs = wikimedia_pageviews.specs(check_held=False)
    bulk_file.main("wikimedia_pageviews", "load", landing_root=LANDING, specs=wiki_specs, pause_seconds=0.2, missing_ok=True)
    keep = {r["topic_key"] for r in __import__("ingestion.to_landing.patterns.seeded", fromlist=["x"]).rows("gdelt_query")[:n]}
    g_specs = [s for s in gdelt.specs(check_held=False) if s["topic_key"] in keep]
    gdelt.extract(LANDING, g_specs)
    wload.load(0, landing_root=LANDING, namespace=NS)
    gload.bulk_bronze.load("gdelt", (NS, "gdelt_timeline"), (NS, "gdelt_rejected_files"), gload.COLUMNS, gload.decode,
                           "timeline", landing_root=LANDING, lake_root=NS, all_versions=True, specs=g_specs)
    con = duckdb.connect()
    seeds(con)
    for name in ("wiki_pageviews", "gdelt_timeline"):
        t = table(cat, name)
        if t is not None:
            con.register(name + "_arrow", t)
    refs = {r"\{\{ ref\('topic_signal'\) \}\}": "seed_topic_signal"}
    att = sql_of("warehouse/models/silver/topics/topic_attention.sql",
                 {r"\{\{ source\('wikimedia_pageviews', 'wiki_pageviews'\) \}\}": "wiki_pageviews_arrow", **refs})
    con.execute(f"create table att as {att}")
    per = con.execute("select topic_key, count(*), min(day)::varchar, max(day)::varchar, max(articles), round(avg(views)) from att group by 1 order by 1").fetchall()
    landed = {e for (e,) in con.execute("select distinct project || ':' || article from wiki_pageviews_arrow").fetchall()}
    wanted = [s["article"] for s in wiki_specs]
    annotate("notice", "trial wiki attention", {"rows": con.execute("select count(*) from att").fetchone()[0], "per_topic": per})
    annotate("notice", "trial wiki articles", {"wanted": len(wanted), "landed_articles": len(landed), "landed": sorted(landed)[:80]})
    if "gdelt_timeline_arrow" in [r[0] for r in con.execute("show tables").fetchall()]:
        vol = sql_of("warehouse/models/silver/topics/topic_volume.sql",
                     {r"\{\{ source\('gdelt', 'gdelt_timeline'\) \}\}": "gdelt_timeline_arrow"})
        con.execute(f"create table vol as {vol}")
        per = con.execute("select topic_key, count(*), min(day)::varchar, max(day)::varchar, round(avg(volume)), round(avg(volume_share)*1e4, 2), round(avg(tone), 2) from vol group by 1 order by 1").fetchall()
        last = con.execute("select topic_key, day::varchar, volume, monitored, tone from vol qualify row_number() over (partition by topic_key order by day desc) <= 2 order by 1, 2").fetchall()
        annotate("notice", "trial gdelt volume", {"per_topic": per, "last": last})
    else:
        annotate("warning", "trial gdelt volume", "no gdelt_timeline table")


def run_all(cat, client) -> None:
    import traceback

    from ingestion.common import bulk_bronze, report

    real, collected = report.annotate, []

    def quiet(level, title, data):
        collected.append((level, title, data))
        if level == "error":
            real(level, title, data)

    globals()["annotate"] = quiet
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = quiet

    from dev.topics_render import render, topics_cfg
    from ingestion.to_bronze.release_calendars import load as cload
    from ingestion.to_landing import release_calendars

    for step in (news, signals):
        try:
            step(cat, client)
        except Exception:  # noqa: BLE001 - trial: report and go on
            annotate("error", f"trial {step.__name__} crashed", traceback.format_exc()[-1500:])
    try:
        release_calendars_specs = release_calendars.specs()
        for spec in release_calendars_specs:
            with contextlib.suppress(bulk_file.PermanentError):
                bulk_file.load_file(client, req("STORAGE_LANDING_BUCKET_NAME"), "release_calendars", spec, LANDING)
        cload.load(0, landing_root=LANDING, namespace=NS)
    except Exception:  # noqa: BLE001
        annotate("error", "trial calendars crashed", traceback.format_exc()[-1500:])
    con = duckdb.connect()
    con.execute("set timezone='UTC'")
    seeds(con)
    for name in ("release_kpi", "release_calendar"):
        con.execute(f"create or replace table seed_{name} as select * from read_csv('{SEEDS}/{name}.csv', header=true, all_varchar=true)")
    for name in ("news_items", "wiki_pageviews", "gdelt_timeline", "release_calendar_items"):
        t = table(cat, name)
        if t is None:
            t = None
            annotate("warning", f"trial: no {name}", "")
            continue
        con.register(name, t)
    srcs = {("news_feeds", "news_items"): "news_items", ("wikimedia_pageviews", "wiki_pageviews"): "wiki_pageviews",
            ("gdelt", "gdelt_timeline"): "gdelt_timeline", ("release_calendars", "release_calendar_items"): "release_calendar_items"}
    refs = {n: f"seed_{n}" for n in ("topic", "topic_kpi", "topic_keyword", "topic_signal", "news_outlet", "kpi_v2_map", "release_kpi", "release_calendar")}
    cfg = topics_cfg()
    models = [("news_headlines", "silver/news_feeds"), ("topic_attention", "silver/topics"), ("topic_volume", "silver/topics"),
              ("releases", "silver/release_calendars"), ("topic_matches", "gold/topics"), ("topic_daily", "gold/topics"),
              ("topic_rank", "gold/topics"), ("topic_quality", "gold/topics")]
    for name, folder in models:
        try:
            sql = render(f"warehouse/models/{folder}/{name}.sql", refs, srcs, cfg)
            con.execute(f"create or replace table {name} as {sql}")
            refs[name] = name
        except Exception as exc:  # noqa: BLE001
            annotate("error", f"trial model {name}", str(exc)[:1500])
            con.execute(f"create or replace table {name} as select 1 as x where false")
            refs[name] = name
    globals()["annotate"] = real
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = real
    annotate("notice", "trial steps", [(lv, t, str(d)[:160]) for lv, t, d in collected][:20])
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
    annotate("notice", "trial releases", {"per_publisher": q("select publisher, count(*), min(release_date)::varchar, max(release_date)::varchar, count(*) filter (where kpi_keys != '') from releases group by 1"),
                                          "linked": q("select publisher, release_date::varchar, title, kpi_keys from releases where kpi_keys != '' order by release_date limit 15")})
    annotate("notice", "trial matches", {"headlines": q("select count(*) from news_headlines"), "matched": q("select count(distinct headline_id) from topic_matches"),
                                         "per_topic": q("select topic_key, count(*) from topic_matches group by 1 order by 2 desc")})
    annotate("notice", "trial matched sample", q("select topic_key, outlet_key, title, pattern from topic_matches order by published_at desc limit 25"))
    annotate("notice", "trial unmatched sample", q("select outlet_key, title from news_headlines where headline_id not in (select headline_id from topic_matches) order by published_at desc limit 30"))
    annotate("notice", "trial ranking", q("select period, rank, topic_key, round(score, 2), headlines, trend, round(z_news, 2), round(z_volume, 2), round(z_attention, 2) from topic_rank where rank <= 10 order by period, rank"))
    annotate("notice", "trial quality", q("select check_key, subject, value, status from topic_quality where status != 'ok' or check_key in ('matched_share', 'unmatched_headlines') order by 1, 2"))
    annotate("notice", "trial daily today", q("select topic_key, headlines, round(news, 1), round(volume, 1), attention, round(z_volume, 2), round(z_attention, 2), release, round(heat, 2) from topic_daily where day = (select max(day) from topic_daily) and heat is not null order by heat desc limit 15"))
    try:
        from warehouse import publish

        if hasattr(publish, "topics_json"):
            doc = publish.topics_json(con, cfg)
            body = json.dumps(doc, ensure_ascii=False).encode()
            annotate("notice", "trial topics.json", {"bytes": len(body), "gzip": len(__import__("gzip").compress(body)),
                                                     "day": doc["periods"]["day"]["topics"][:2], "week_keys": [t["topic_key"] for t in doc["periods"]["week"]["topics"]]})
    except Exception:  # noqa: BLE001
        annotate("error", "trial topics.json", traceback.format_exc()[-1500:])


if __name__ == "__main__":
    import json

    cat, client = catalog(), s3()
    TABLES["all"] = [t for k, ts in TABLES.items() if k != "all" for t in ts]
    try:
        {"news": news, "signals": signals, "all": run_all}[sys.argv[1]](cat, client)
    finally:
        if "keep" not in sys.argv:
            annotate("notice", "trial cleanup", cleanup(cat, client))
