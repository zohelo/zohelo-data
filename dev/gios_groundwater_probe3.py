"""Read-only probe 3 (PL-ENV-012, #2870): the GIOŚ groundwater datasets on dane.gov.pl (dataset 1405 and the dataset
of resource 2225196): attributes, licence fields, every resource, and the layout of each workbook. mjwp.gios.gov.pl
answers 403 with a maintenance page (probes 45809388367, 45809752001). No R2, no writes; requests 3 s apart."""

from __future__ import annotations

import json
import sys
import time

import requests

from dev.gios_groundwater_probe import describe

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.dane.gov.pl/1.4"
KEEP = ("title", "notes", "license_name", "license_condition_source", "license_condition_modification",
        "license_condition_responsibilities", "license_condition_db_or_copyrighted", "license_condition_personal_data",
        "license_chosen", "update_frequency", "created", "modified", "verified", "categories", "keywords", "url")


def get(url: str) -> requests.Response:
    time.sleep(3)
    for attempt in range(3):
        try:
            return requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=(30, 300))
        except requests.RequestException as exc:
            print(json.dumps({"retry": url, "error": str(exc)[:150]}), flush=True)
            time.sleep(15 * (attempt + 1))
    raise RuntimeError(url)


def main() -> int:
    datasets = ["1405"]
    r = get(f"{API}/resources/2225196")
    if r.status_code == 200:
        rel = (r.json().get("data") or {}).get("relationships") or {}
        ds = ((rel.get("dataset") or {}).get("data") or {}).get("id")
        print(json.dumps({"resource_2225196_dataset": ds}), flush=True)
        if ds and ds not in datasets:
            datasets.append(str(ds))
    describe_limit = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    described = 0
    for ds in datasets:
        r = get(f"{API}/datasets/{ds}")
        a = (r.json().get("data") or {}).get("attributes") or {}
        print("=" * 100)
        print(json.dumps({"dataset": ds, "http": r.status_code, **{k: a.get(k) for k in KEEP}}, ensure_ascii=False)[:4000],
              flush=True)
        url, items = f"{API}/datasets/{ds}/resources?per_page=100&page=1", []
        while url:
            body = get(url).json()
            items += body.get("data", [])
            url = (body.get("links") or {}).get("next")
        for it in items:
            a = it.get("attributes") or {}
            print(json.dumps({"id": it.get("id"), "title": a.get("title"), "format": a.get("format"),
                              "file_url": a.get("file_url"), "download_url": a.get("download_url"),
                              "size": a.get("file_size"), "created": a.get("created"), "modified": a.get("modified"),
                              "data_date": a.get("data_date"), "link": a.get("link"),
                              "description": (a.get("description") or "")[:200]}, ensure_ascii=False), flush=True)
        for it in items:
            a = it.get("attributes") or {}
            f = a.get("file_url") or a.get("download_url") or ""
            if (a.get("format") or "").lower() not in ("xlsx", "xls", "csv", "zip") or described >= describe_limit:
                continue
            described += 1
            resp = get(f)
            print("-" * 80)
            print(json.dumps({"resource": it.get("id"), "title": a.get("title"), "http": resp.status_code,
                              "bytes": len(resp.content), "last_modified": resp.headers.get("Last-Modified"),
                              "etag": resp.headers.get("ETag"), "type": resp.headers.get("Content-Type")},
                             ensure_ascii=False), flush=True)
            if resp.status_code != 200 or len(resp.content) > 40_000_000:
                continue
            name = f.lower().split("?")[0]
            if name.endswith(".csv"):
                print("  csv head:", resp.content[:1500].decode("utf-8", "replace"), flush=True)
            elif name.endswith(".zip"):
                import io
                import zipfile

                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    print("  zip members:", [(i.filename, i.file_size) for i in z.infolist()][:40], flush=True)
                    for i in z.infolist():
                        if i.filename.lower().endswith((".xlsx", ".xls")):
                            print("  member", i.filename, flush=True)
                            describe(z.read(i), i.filename.lower())
            else:
                describe(resp.content, name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
