#!/usr/bin/env python3
"""Archive only R2 Landing objects already proven by current Iceberg provenance.

Supported bounded scopes:
- Eurostat: bulk raw_sha256 in bronze.eurostat_full_observations and response_sha256
  in bronze.eurostat_observations.
- World Bank WDI: the one bulk ZIP whose SHA-256 equals silver.wdi_observations.archive_sha256.

Every archive copy is server-side copied and then fully streamed back through SHA-256
before the immutable receipt is written. Archive keys are content-addressed
(05_archive/<source>/<kind>/<sha256>.bin). Landing is never deleted here.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import PurePosixPath
from typing import Any

from ingestion.common.r2 import landing_objects, req, s3, sha_column

SOURCES=("eurostat","world_bank_wdi")


def proven_for(source:str,meta:dict[str,dict[str,Any]])->tuple[set[str],dict[str,Any]]:
    if source=="eurostat":
        bulk=sha_column(
            "SELECT DISTINCT raw_sha256 FROM bronze.eurostat_full_observations "
            "WHERE raw_sha256 IS NOT NULL;","raw_sha256")
        responses=sha_column(
            "SELECT DISTINCT response_sha256 FROM bronze.eurostat_observations "
            "WHERE response_sha256 IS NOT NULL;","response_sha256")
        proven=bulk|responses
        return proven,{
            "proof":"r2_sql_distinct_provenance_hash_membership",
            "bronze_tables":[
                {"table":"bronze.eurostat_full_observations","column":"raw_sha256","distinct_sha256":len(bulk)},
                {"table":"bronze.eurostat_observations","column":"response_sha256","distinct_sha256":len(responses)},
            ],
        }
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


def list_objects(client,bucket:str,prefix:str)->list[dict[str,Any]]:
    rows=[]
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket,Prefix=prefix):
        for item in page.get("Contents",[]):
            rows.append({"key":str(item["Key"]),"size":int(item.get("Size",0))})
    return sorted(rows,key=lambda row:row["key"])


def blockers(client,lakehouse_bucket:str,source:str)->list[str]:
    prefixes=[f"06_control/source_campaigns/{source}/",f"06_control/source_campaigns/{source}_bulk/"]
    if source=="eurostat":
        prefixes.append("06_control/source_campaigns/eurostat_bulk_bronze/")
    names={"current-ingestion-state.json","current-landing.json","publication-owner.json"}
    found=[]
    for prefix in prefixes:
        for item in list_objects(client,lakehouse_bucket,prefix):
            if PurePosixPath(item["key"]).name in names and item["size"]>0:
                found.append(item["key"])
    return sorted(found)


def immutable_json(client,bucket:str,key:str,value:dict[str,Any])->None:
    raw=(json.dumps(value,indent=2,sort_keys=True)+"\n").encode()
    try:
        existing=client.get_object(Bucket=bucket,Key=key)["Body"].read()
    except Exception:
        existing=None
    if existing is not None:
        if existing!=raw:
            raise RuntimeError(f"immutable receipt conflict: {key}")
        return
    client.put_object(
        Bucket=bucket,Key=key,Body=raw,ContentType="application/json",
        Metadata={"sha256":sha256(raw).hexdigest()})


def main(argv=None)->int:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source",choices=SOURCES,required=True)
    args=parser.parse_args(argv)
    client=s3(); landing_bucket=req("R2_LANDING_BUCKET"); lakehouse_bucket=req("R2_LAKEHOUSE_BUCKET")
    meta=landing_objects(client,landing_bucket,args.source)
    proven,proof=proven_for(args.source,meta)
    if not proven<=set(meta):
        raise RuntimeError(f"Iceberg provenance lacks Landing bytes: {len(proven-set(meta))}")
    selected=[meta[digest] for digest in sorted(proven)]

    with ThreadPoolExecutor(max_workers=16) as pool:
        archived=list(pool.map(lambda item: ensure_archive(client,landing_bucket,args.source,item),selected))

    active=blockers(client,lakehouse_bucket,args.source)
    digest=sha256(json.dumps(
        [{"key":x["source_key"],"size":x["size"],"sha256":x["sha256"]} for x in archived],
        sort_keys=True,separators=(",",":")).encode()).hexdigest()
    receipt_key=f"06_control/lineage/{args.source}/partial-{digest}.json"
    try:
        existing_receipt=json.loads(client.get_object(Bucket=lakehouse_bucket,Key=receipt_key)["Body"].read())
    except Exception:
        existing_receipt=None
    stable_archived=[
        {key:value for key,value in item.items() if key!="copied_this_run"}
        for item in archived
    ]
    observed_at=(
        existing_receipt.get("observed_at_utc")
        if isinstance(existing_receipt,dict) and isinstance(existing_receipt.get("observed_at_utc"),str)
        else datetime.now(timezone.utc).isoformat()
    )
    receipt={
        "format_version":1,
        "kind":"partial_landing_bronze_lineage_receipt",
        "source_id":args.source,
        "input_set_sha256":digest,
        "coverage_scope":"only_landing_hashes_proven_by_current_iceberg_provenance",
        "proof":proof,
        "landing_inputs":selected,
        "archive":stable_archived,
        "archive_readback_verified":True,
        "landing_deleted":False,
        "landing_delete_eligible":False,
        "blocking_control_references":active,
        "unproven_landing_objects":len(meta)-len(selected),
        "unproven_landing_bytes":sum(int(item["size"]) for d,item in meta.items() if d not in proven),
        "observed_at_utc":observed_at,
    }
    immutable_json(client,lakehouse_bucket,receipt_key,receipt)
    print(json.dumps({
        "result":"pass","operation":"archive-proven-r2-lineage","source":args.source,
        "proven_landing_objects":len(selected),
        "proven_landing_bytes":sum(int(x["size"]) for x in selected),
        "archive_objects_verified":len(archived),
        "archive_objects_copied_this_run":sum(1 for x in archived if x["copied_this_run"]),
        "unproven_landing_objects":receipt["unproven_landing_objects"],
        "unproven_landing_bytes":receipt["unproven_landing_bytes"],
        "landing_deleted":False,
        "blocking_control_references":active,
        "lineage_receipt":f"s3://{lakehouse_bucket}/{receipt_key}",
    },sort_keys=True))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
