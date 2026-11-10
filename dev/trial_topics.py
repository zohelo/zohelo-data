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
    bulk_file.main("gdelt", "load", landing_root=LANDING, specs=g_specs, pause_seconds=6, missing_ok=True)
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


if __name__ == "__main__":
    cat, client = catalog(), s3()
    try:
        {"news": news, "signals": signals}[sys.argv[1]](cat, client)
    finally:
        if "keep" not in sys.argv:
            annotate("notice", "trial cleanup", cleanup(cat, client))
