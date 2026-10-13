#!/usr/bin/env python3
"""Archive only R2 Landing objects already proven by current Iceberg provenance.

Supported bounded scopes:
- Eurostat: Landing files registered in bronze.eurostat_full_observations (one Bronze
  file per Landing SHA-256).
- World Bank WDI: the one bulk ZIP whose SHA-256 equals silver.wdi_observations.archive_sha256.

Every archive copy is server-side copied and then fully streamed back through SHA-256
before it counts. Archive keys are content-addressed
(05_archive/<source>/<kind>/<sha256>.bin). Landing is never deleted here. The result is
reported as an annotation; the Archive key itself is the record.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
from typing import Any

from ingestion.common.r2 import landing_objects, req, s3, sha_column
from ingestion.common.report import annotate

SOURCES=("eurostat","world_bank_wdi")


def proven_for(source:str,meta:dict[str,dict[str,Any]])->tuple[set[str],dict[str,Any]]:
    if source=="eurostat":
        from ingestion.common.r2 import catalog
        from ingestion.to_bronze.eurostat.batch import TABLE_ID, loaded_hashes
        bulk=loaded_hashes(catalog().load_table(TABLE_ID))
        return bulk,{"proof":"bronze_file_per_landing_sha256",
                     "bronze_tables":[{"table":"bronze.eurostat_full_observations","distinct_sha256":len(bulk)}]}
    bulk={digest for digest,item in meta.items() if "/bulk/" in item["key"]}
    modeled=sha_column(
        "SELECT DISTINCT archive_sha256 FROM silver.wdi_observations "
        "WHERE archive_sha256 IS NOT NULL;","archive_sha256")
    if len(bulk)!=1 or modeled!=bulk:
        raise RuntimeError(
            f"WDI bulk/model lineage mismatch: landing={sorted(bulk)} modeled={sorted(modeled)}")
    return bulk,{
        "proof":"silver_archive_sha256_equals_bulk_landing_sha256",
        "bronze_tables":[
            {"tables":["bronze.wdi_country","bronze.wdi_country_series","bronze.wdi_data",
                       "bronze.wdi_footnote","bronze.wdi_series","bronze.wdi_series_time"],
             "modeled_lineage_table":"silver.wdi_observations",
             "column":"archive_sha256","distinct_sha256":1},
        ],
    }


def hash_object(client,bucket:str,key:str)->tuple[str,int]:
    response=client.get_object(Bucket=bucket,Key=key)
    body=response["Body"]; digest=sha256(); size=0
    try:
        while True:
            chunk=body.read(1024*1024)
            if not chunk: break
            digest.update(chunk); size+=len(chunk)
    finally:
        body.close()
    return digest.hexdigest(),size


def archive_key(source:str,item:dict[str,Any])->str:
    return f"05_archive/{source}/{item['kind']}/{item['sha256']}.bin"


def ensure_archive(client,bucket:str,source:str,item:dict[str,Any])->dict[str,Any]:
    target=archive_key(source,item)
    copied=False
    try:
        head=client.head_object(Bucket=bucket,Key=target)
        present=int(head.get("ContentLength",-1))==int(item["size"])
    except Exception:
        present=False
    if not present:
        client.copy_object(
            Bucket=bucket,Key=target,
            CopySource={"Bucket":bucket,"Key":item["key"]},
            MetadataDirective="REPLACE",
            Metadata={"source-sha256":item["sha256"]},
            ContentType="application/octet-stream",
        )
        copied=True
    observed,size=hash_object(client,bucket,target)
    if observed!=item["sha256"] or size!=item["size"]:
        raise RuntimeError(f"Archive readback mismatch: {item['key']}")
    return {
        "source_key":item["key"],"archive_key":target,"size":size,"sha256":observed,
        "kind":item["kind"],"copied_this_run":copied,"compression":"none_exact_native_bytes",
    }



def main(argv=None)->int:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source",choices=SOURCES,required=True)
    args=parser.parse_args(argv)
    client=s3(); landing_bucket=req("R2_LANDING_BUCKET")
    meta=landing_objects(client,landing_bucket,args.source)
    proven,proof=proven_for(args.source,meta)
    if not proven<=set(meta):
        raise RuntimeError(f"Iceberg provenance lacks Landing bytes: {len(proven-set(meta))}")
    selected=[meta[digest] for digest in sorted(proven)]

    with ThreadPoolExecutor(max_workers=16) as pool:
        archived=list(pool.map(lambda item: ensure_archive(client,landing_bucket,args.source,item),selected))

    summary={
        "source":args.source,"proof":proof,
        "proven_landing_objects":len(selected),
        "proven_landing_gb":round(sum(int(x["size"]) for x in selected)/1e9,2),
        "archive_objects_verified":len(archived),
        "archive_objects_copied_this_run":sum(1 for x in archived if x["copied_this_run"]),
        "unproven_landing_objects":len(meta)-len(selected),
        "landing_deleted":False,
    }
    print(json.dumps(summary,sort_keys=True))
    annotate("notice",f"{args.source} archive",summary)
    return 0

if __name__=="__main__":
    raise SystemExit(main())
