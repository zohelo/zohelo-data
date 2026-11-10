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
TABLES = {"news": ["news_items", "news_feeds_rejected_files"]}


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


if __name__ == "__main__":
    cat, client = catalog(), s3()
    try:
        {"news": news}[sys.argv[1]](cat, client)
    finally:
        if "keep" not in sys.argv:
            annotate("notice", "trial cleanup", cleanup(cat, client))
