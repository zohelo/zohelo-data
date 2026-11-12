"""Read-only (#2985): (1) what run 43323227629 left in Eurostat Landing, from listings only;
(2) what DuckDB httpfs and PyArrow's S3 reader do with 429 / 503 from a local stub (no R2 requests)."""

from collections import Counter
import io
import time

from dev.s3_stub import Stub
from ingestion.common import r2
from ingestion.common.report import annotate

PREFIX = "01_landing/eurostat/snapshot/data/"
WINDOW = ("20261112T224900Z", "20261112T230000Z")  # landing.extract job 0 ran 22:49:59 to 22:57:04 UTC


def landing_state() -> dict:
    client, bucket = r2.s3(), r2.req("STORAGE_LANDING_BUCKET_NAME")
    receipts, data, other = {}, {}, 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith(".receipt.json"):
                receipts[key[: -len(".receipt.json")]] = item["Size"]
            elif key.endswith(".tsv.gz"):
                data[key[: -len(".tsv.gz")]] = item["Size"]
            else:
                other += 1
    def stamp(base: str) -> str:
        return base.rsplit("__", 1)[-1]
    in_run = lambda base: WINDOW[0] <= stamp(base) <= WINDOW[1]  # noqa: E731
    orphans = sorted(b for b in data if b not in receipts)
    uploads, token, uploads_error = [], {}, None
    try:
        while True:
            page = client.list_multipart_uploads(Bucket=bucket, Prefix=PREFIX, **token)
            uploads += [
                {"key": u["Key"][len(PREFIX) :], "initiated": str(u["Initiated"])} for u in page.get("Uploads", [])
            ]
            if not page.get("IsTruncated"):
                break
            token = {"KeyMarker": page["NextKeyMarker"], "UploadIdMarker": page["NextUploadIdMarker"]}
    except Exception as exc:
        uploads_error = f"{type(exc).__name__}: {exc}"[:200]
    return {
        "receipts": len(receipts),
        "data_files_in_landing": len(data),
        "other_objects": other,
        "data_without_receipt": len(orphans),
        "data_without_receipt_examples": [o[len(PREFIX) :] for o in orphans[:8]],
        "data_without_receipt_in_run_window": sum(in_run(o) for o in orphans),
        "zero_byte_data": sum(1 for s in data.values() if s == 0),
        "zero_byte_receipts": sum(1 for s in receipts.values() if s == 0),
        "run_window": WINDOW,
        "receipts_written_in_run_window": sum(in_run(b) for b in receipts),
        "data_written_in_run_window": sum(in_run(b) for b in data),
        "newest_receipt_stamps": sorted({stamp(b) for b in receipts})[-3:],
        "receipts_by_day_last5": dict(sorted(Counter(stamp(b)[:8] for b in receipts).items())[-5:]),
        "unfinished_multipart_uploads": len(uploads),
        "unfinished_multipart_examples": uploads[:5],
        "unfinished_multipart_error": uploads_error,
        "client_throttling": r2.THROTTLE.report(),
    }


def other_clients() -> dict:
    import duckdb
    import pyarrow as pa
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    stub = Stub()
    buf = io.BytesIO()
    pq.write_table(pa.table({"a": [1, 2, 3]}), buf)
    stub.objects["t.parquet"] = (buf.getvalue(), {})
    host = stub.url.replace("http://", "")
    out = {"duckdb": {}, "pyarrow": {}}

    def measure(label, kinds, name, times, call):
        for kind in kinds:
            stub.script[kind] = [name] * times
        before, start = dict(stub.calls), time.time()
        try:
            call()
            result = "ok"
        except Exception as exc:
            result = "FAILED " + str(exc).replace("\n", " ")[:110]
        for kind in kinds:
            stub.script[kind] = []
        sent = {k: stub.calls[k] - before.get(k, 0) for k in stub.calls if stub.calls[k] - before.get(k, 0)}
        return {"result": result, "requests": sent, "seconds": round(time.time() - start, 1)}

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    out["duckdb"]["settings"] = dict(
        con.execute("select name, value from duckdb_settings() where name like 'http_retr%'").fetchall()
    )
    con.execute(
        f"CREATE SECRET s (TYPE s3, KEY_ID 'k', SECRET 's', ENDPOINT '{host}', REGION 'auto', URL_STYLE 'path', USE_SSL false)"
    )
    read = lambda: con.execute("select count(*) from read_parquet('s3://b/t.parquet')").fetchone()  # noqa: E731
    for name in ("429", "slowdown", "unavailable"):
        for kind in ("HEAD", "GET"):
            out["duckdb"][f"{kind} {name} x2 then ok"] = measure("duckdb", [kind], name, 2, read)
    out["duckdb"]["HEAD 429 x50 (sustained)"] = measure("duckdb", ["HEAD"], "429", 50, read)
    out["duckdb"]["PUT slowdown x2 then ok"] = measure(
        "duckdb", ["PUT", "CREATE_MPU", "PART"], "slowdown", 2, lambda: con.execute("copy (select 1 a) to 's3://b/w.parquet'")
    )

    fs = pafs.S3FileSystem(access_key="k", secret_key="s", endpoint_override=host, scheme="http", region="auto")
    read = lambda: fs.open_input_file("b/t.parquet").read()  # noqa: E731
    for name in ("429", "slowdown", "unavailable"):
        for kind in ("HEAD", "GET"):
            out["pyarrow"][f"{kind} {name} x2 then ok"] = measure("pyarrow", [kind], name, 2, read)
    out["pyarrow"]["HEAD 429 x50 (sustained)"] = measure("pyarrow", ["HEAD"], "429", 50, read)

    def write():
        with fs.open_output_stream("b/w2.bin") as handle:
            handle.write(b"x" * 100)

    out["pyarrow"]["PUT slowdown x2 then ok"] = measure("pyarrow", ["PUT", "CREATE_MPU", "PART"], "slowdown", 2, write)
    return out


def main() -> int:
    code = 0
    try:
        annotate("notice", "eurostat landing state after run 43323227629", landing_state())
    except Exception as exc:
        annotate("error", "eurostat landing state failed", {"error": f"{type(exc).__name__}: {exc}"[:500], "throttling": r2.THROTTLE.report()})
        code = 1
    try:
        result = other_clients()
        annotate("notice", "duckdb httpfs on throttle answers (stub)", result["duckdb"])
        annotate("notice", "pyarrow s3 on throttle answers (stub)", result["pyarrow"])
    except Exception as exc:
        annotate("warning", "stub measurement failed", {"error": f"{type(exc).__name__}: {exc}"[:500]})
    return code


if __name__ == "__main__":
    raise SystemExit(main())
