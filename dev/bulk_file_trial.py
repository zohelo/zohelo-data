"""Trial: the shared bulk_file pattern lands WDI and PRG into a scratch prefix; compare the
SHA-256 with the newest production receipt; delete the scratch objects afterwards."""
import traceback

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file as bf

FILES = {
    "world_bank_wdi": [{"entity": "WDI_CSV", "url": "https://databank.worldbank.org/data/download/WDI_CSV.zip", "format": "zip", "version": "last_modified"}],
    "gugik_prg": [{"entity": "jednostki_administracyjne", "url": "https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip", "format": "zip", "version": "ingested"}],
}
SCRATCH = "zz_dev_landing"
try:
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    out = {}
    for source, files in FILES.items():
        bf.declaration = lambda s, files=files: files
        result = bf.load_file(client, bucket, source, files[0], SCRATCH)
        prod = bf.landed(client, bucket, bf.prefix(source, files[0]["entity"]))
        scratch = bf.landed(client, bucket, bf.prefix(source, files[0]["entity"], SCRATCH))
        out[source] = {"result": result, "prod_newest_sha": prod[-1]["sha256"] if prod else None,
                       "same_bytes_as_prod": bool(prod) and result["sha256"] == prod[-1]["sha256"],
                       "scratch_receipt_core": {k: scratch[-1][k] for k in ("schema_version", "entity", "version_token", "version_basis", "size_bytes")} if scratch else None}
    keys = [o["Key"] for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=SCRATCH + "/") for o in p.get("Contents", [])]
    for k in keys:
        client.delete_object(Bucket=bucket, Key=k)
    out["scratch_deleted"] = len(keys)
    annotate("notice", "bulk_file trial", out)
except Exception:
    annotate("error", "bulk_file trial", {"traceback": traceback.format_exc()[-2000:]})
    raise
