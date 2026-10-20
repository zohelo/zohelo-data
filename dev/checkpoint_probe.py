"""Read-only probe for the checkpoint design (#875), plus a scratch-table trial.

1. Eurostat Bronze: which table files are original per-Landing-file files (name = SHA-256)
   and which were rewritten by catalog compaction; how many latest Landing versions are
   covered by original files (those can be adopted without re-decoding).
2. GUS BDL: subgroups, parts and Bronze files.
3. Scratch namespace zz_dev_checkpoint: partition replace (delete + append in one
   transaction) is metadata-only; table rename within and across namespaces; per-table
   maintenance config (compaction disabled). The scratch tables are dropped at the end.
Writes nothing outside zz_dev_checkpoint.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import contextlib
import json
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import In
import requests

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.eurostat import batch as eb
from ingestion.to_bronze.gus_bdl import batch as bb

SCRATCH = "zz_dev_checkpoint"


def files_of(table) -> list[dict]:
    t = table.inspect.files()
    return [
        {"path": p, "rows": r, "bytes": b, "partition": part}
        for p, r, b, part in zip(
            t.column("file_path").to_pylist(),
            t.column("record_count").to_pylist(),
            t.column("file_size_in_bytes").to_pylist(),
            t.column("partition").to_pylist(),
        )
    ]


def eurostat(cat, client) -> None:
    table = cat.load_table(eb.TABLE_ID)
    files = files_of(table)
    original = [f for f in files if bf.SHA_RE.fullmatch(Path(f["path"]).stem)]
    compacted = [f for f in files if f not in original]
    latest = eb.landing_candidates(client, req("R2_LANDING_BUCKET"))
    by_sha = defaultdict(list)
    for f in original:
        by_sha[Path(f["path"]).stem].append(f)
    covered = [c for c in latest if any(f["partition"].get("dataset_id") == c["dataset_id"] for f in by_sha.get(c["raw_sha256"], []))]
    per_partition = Counter(f["partition"].get("dataset_id") for f in files)
    annotate(
        "notice",
        "eurostat bronze files",
        {
            "files": len(files),
            "rows": sum(f["rows"] for f in files),
            "gb": round(sum(f["bytes"] for f in files) / 1e9, 2),
            "original_files": len(original),
            "original_rows": sum(f["rows"] for f in original),
            "compacted_files": len(compacted),
            "compacted_rows": sum(f["rows"] for f in compacted),
            "compacted_partitions": len({f["partition"].get("dataset_id") for f in compacted}),
            "latest_landing_versions": len(latest),
            "latest_covered_by_original_file": len(covered),
            "latest_landing_gb": round(sum(c["size"] for c in latest) / 1e9, 2),
            "partitions": len(per_partition),
            "partitions_with_more_than_one_file": sum(1 for n in per_partition.values() if n > 1),
            "largest_latest": sorted(((c["size"], c["dataset_id"]) for c in latest), reverse=True)[:5],
        },
    )


def bdl(cat, client) -> None:
    parts = bb.candidates(client, req("R2_LANDING_BUCKET"))
    subgroups = Counter(p["subgroup_id"] for p in parts)
    table = cat.load_table(bb.TABLE_ID)
    files = files_of(table)
    annotate(
        "notice",
        "bdl landing and bronze",
        {
            "latest_parts": len(parts),
            "subgroups": len(subgroups),
            "subgroups_multi_part": sum(1 for n in subgroups.values() if n > 1),
            "max_parts": max(subgroups.values()) if subgroups else 0,
            "latest_bytes_gb": round(sum(int(p.get("size_bytes") or 0) for p in parts) / 1e9, 3),
            "bronze_files": len(files),
            "bronze_rows": sum(f["rows"] for f in files),
            "bronze_compacted_files": sum(1 for f in files if not bf.SHA_RE.fullmatch(Path(f["path"]).stem)),
        },
    )


def maintenance(namespace: str, name: str, body: dict | None) -> dict:
    base = (
        f"https://api.cloudflare.com/client/v4/accounts/{req('CLOUDFLARE_ACCOUNT_ID')}/r2-catalog/"
        f"{req('R2_LAKEHOUSE_BUCKET')}/namespaces/{namespace}/tables/{name}/maintenance-configs"
    )
    headers = {"Authorization": f"Bearer {req('R2_DATA_CATALOG_TOKEN')}"}
    response = requests.request("POST" if body is not None else "GET", base, headers=headers, json=body, timeout=30)
    try:
        payload = response.json()
    except ValueError:
        payload = {"text": response.text[:300]}
    return {"status": response.status_code, "result": payload.get("result"), "errors": payload.get("errors")}


def scratch(cat, client) -> None:
    out: dict = {}
    bucket = req("R2_LAKEHOUSE_BUCKET")
    columns = [("entity", "VARCHAR"), ("raw_sha256", "VARCHAR"), ("value", "BIGINT")]
    table_id = (SCRATCH, "trial")
    for tid in (table_id, (SCRATCH, "trial_renamed"), ("zz_dev_checkpoint_b", "trial_moved")):
        with contextlib.suppress(Exception):
            cat.drop_table(tid)
    table = bf.ensure_table(cat, table_id, columns, "entity")
    schema = pa.schema([("entity", pa.string()), ("raw_sha256", pa.string()), ("value", pa.int64())])
    uris = {}
    with tempfile.TemporaryDirectory() as tmp:
        for entity, version in (("A", "a1"), ("B", "b1"), ("A", "a2")):
            path = Path(tmp) / f"{entity}{version}.parquet"
            pq.write_table(pa.table({"entity": [entity] * 3, "raw_sha256": [version] * 3, "value": [1, 2, 3]}, schema=schema), path)
            key = f"zz_dev_checkpoint/{entity}/{version}.parquet"
            client.upload_file(str(path), bucket, key)
            uris[(entity, version)] = f"s3://{bucket}/{key}"
    bf.commit(cat, table_id, [uris[("A", "a1")], uris[("B", "b1")]], ["entity", "raw_sha256"], "trial")
    # Replace partition A: delete + append in one transaction.
    table = cat.load_table(table_id)
    metadata = table.metadata.model_copy(
        update={"properties": {**table.metadata.properties, **{f"write.metadata.metrics.column.{c}": "full" for c in ("entity", "raw_sha256")}}}
    )
    new_file = bf.parquet_file_to_data_file(table.io, metadata, uris[("A", "a2")])
    try:
        with table.transaction() as tx:
            tx.delete(In("entity", ["A"]))
            with tx.update_snapshot().fast_append() as append:
                append.append_data_file(new_file)
        table = cat.load_table(table_id)
        out["replace"] = {
            "files_after": sorted(Path(f["path"]).name for f in files_of(table)),
            "snapshots": [s.summary.additional_properties | {"operation": str(s.summary.operation)} for s in table.metadata.snapshots][-3:],
        }
    except Exception as exc:  # report, do not hide
        out["replace_error"] = repr(exc)[:500]
    out["maintenance_get"] = maintenance(SCRATCH, "trial", None)
    out["maintenance_set"] = maintenance(SCRATCH, "trial", {"compaction": {"state": "disabled"}, "snapshot_expiration": {"state": "enabled", "min_snapshots_to_keep": 1, "max_snapshot_age": "1h"}})
    out["maintenance_after"] = maintenance(SCRATCH, "trial", None)
    try:
        cat.rename_table(table_id, (SCRATCH, "trial_renamed"))
        out["rename_same_namespace"] = "ok"
        with contextlib.suppress(Exception):
            cat.create_namespace("zz_dev_checkpoint_b")
        cat.rename_table((SCRATCH, "trial_renamed"), ("zz_dev_checkpoint_b", "trial_moved"))
        out["rename_other_namespace"] = "ok"
        out["rows_after_rename"] = cat.load_table(("zz_dev_checkpoint_b", "trial_moved")).scan().to_arrow().num_rows
    except Exception as exc:
        out["rename_error"] = repr(exc)[:500]
    annotate("notice", "scratch trial", out)
    for tid in ((SCRATCH, "trial"), (SCRATCH, "trial_renamed"), ("zz_dev_checkpoint_b", "trial_moved")):
        with contextlib.suppress(Exception):
            cat.drop_table(tid)
    for ns in (SCRATCH, "zz_dev_checkpoint_b"):
        with contextlib.suppress(Exception):
            cat.drop_namespace(ns)
    keys = [{"Key": f"zz_dev_checkpoint/{e}/{v}.parquet"} for e, v in uris]
    client.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def main() -> int:
    cat, client = catalog(), s3()
    for step in (scratch, eurostat, bdl):
        try:
            step(cat, client)
        except Exception as exc:
            annotate("error", f"{step.__name__} failed", repr(exc)[:1500])
    print(json.dumps({"done": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
