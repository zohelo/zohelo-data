"""Dev trial for #2349 (not for main): inspect, load and verify the mrpips_unemployment declaration into the
zz_dev_mrpips_unemployment/ Landing prefix, report the landed files, then delete every object under it."""

from __future__ import annotations

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

ROOT = "zz_dev_mrpips_unemployment"


def objects(client, bucket: str) -> list[dict]:
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{ROOT}/"):
        found += page.get("Contents", [])
    return found


def main() -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    codes = {}
    spec = bulk_file.declaration("mrpips_unemployment")[0]
    try:
        heads = []
        for link in bulk_file.page_links(spec)[:15]:
            head, http = bulk_file.probe(link)
            h = head.headers if head is not None else {}
            heads.append({"url": link, "http": http, "type": h.get("Content-Type"), "bytes": h.get("Content-Length")})
        annotate("notice", "mrpips_unemployment page links", {"links": heads})
    except Exception as exc:  # noqa: BLE001 - the trial reports, the fallback page follows
        annotate("warning", "mrpips_unemployment page links", {"error": str(exc)[:300]})
    import requests

    for page in (spec["url_from"]["page"], "https://psz.praca.gov.pl/"):
        try:
            resp = requests.get(page, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=30)
            every = bulk_file.links_matching(resp.text, resp.url, r"statyst|bezroboc|\.xlsx?$")
            annotate("notice", f"page {page}", {"status": resp.status_code, "final": resp.url, "links": every[:40]})
        except Exception as exc:  # noqa: BLE001
            annotate("warning", f"page {page}", {"error": str(exc)[:300]})
    try:
        for mode in ("inspect", "load", "verify"):
            codes[mode] = bulk_file.main("mrpips_unemployment", mode, landing_root=ROOT)
        landed = [
            {"key": o["Key"], "bytes": o["Size"]}
            for o in objects(client, bucket)
            if not o["Key"].endswith(".receipt.json")
        ]
        annotate(
            "notice", "mrpips_unemployment trial landed", {"codes": codes, "entities": len(landed), "files": landed}
        )
    finally:
        keys = [o["Key"] for o in objects(client, bucket)]
        for start in range(0, len(keys), 1000):
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[start : start + 1000]]})
        left = len(objects(client, bucket))
        annotate(
            "notice" if left == 0 else "error",
            "mrpips_unemployment trial clean-up",
            {"deleted": len(keys), "left": left},
        )
    return 0 if codes and all(c == 0 for c in codes.values()) and left == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
