"""Read-only probe for MRiRW ZSRIR (PLS-032): the dane.gov.pl copy of the ZSRIR bulletins. No R2 writes.

Lists the ZSRIR datasets on dane.gov.pl (API 1.4), their licence fields and resources, and downloads the newest
XLSX resource of each dataset (plus one older one) into DEV_OUTPUT_DIR for inspection; also reads the gov.pl
market pages. One request at a time, PAUSE seconds apart.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.dane.gov.pl/1.4"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
PAUSE = 1.5
session = requests.Session()
session.headers.update({"User-Agent": UA, "Accept": "application/json"})


def get(url, **params):
    for attempt in range(3):
        try:
            time.sleep(PAUSE)
            r = session.get(url, params=params or None, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                print("  status", r.status_code, url)
                time.sleep(10 * (attempt + 1))
                continue
            return r
        except requests.RequestException as exc:
            print("  error", type(exc).__name__, str(exc)[:200])
            time.sleep(10 * (attempt + 1))
    return None


def save(name, data):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    if isinstance(data, bytes):
        path.write_bytes(data)
    else:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=1))


def datasets():
    found = {}
    for q in ["Zintegrowany System Rolniczej Informacji Rynkowej", "ZSRIR"]:
        page = 1
        while page <= 10:
            r = get(f"{API}/datasets", q=q, per_page=100, page=page)
            if r is None or r.status_code != 200:
                print("datasets query failed", q, r.status_code if r is not None else None)
                break
            body = r.json()
            save(f"datasets_{re.sub(r'[^a-z]', '', q.lower())[:12]}_{page}.json", body)
            for d in body.get("data", []):
                found[d["id"]] = d
            if not body.get("links", {}).get("next"):
                break
            page += 1
    return found


def main(argv):
    found = datasets()
    print(f"datasets found: {len(found)}")
    zs = {k: v for k, v in found.items() if "rolniczej informacji rynkowej" in v["attributes"]["title"].lower()}
    for k, d in sorted(zs.items(), key=lambda x: x[1]["attributes"]["title"]):
        a = d["attributes"]
        lic = {x: a.get(x) for x in a if "licen" in x or "condition" in x}
        print(f"\n### {k} | {a['title']} | modified={a.get('modified')} | freq={a.get('update_frequency')}")
        print("  licence:", json.dumps(lic, ensure_ascii=False)[:1200])
        print("  institution:", (d.get("relationships", {}).get("institution", {}) or {}).get("data"))
        resources, page = [], 1
        while page <= 30:
            r = get(f"{API}/datasets/{k}/resources", per_page=100, page=page, sort="-data_date")
            if r is None or r.status_code != 200:
                print("  resources failed", r.status_code if r is not None else None)
                break
            body = r.json()
            resources += body.get("data", [])
            if not body.get("links", {}).get("next"):
                break
            page += 1
        save(f"resources_{k}.json", resources)
        fmts = {}
        for x in resources:
            fmts[x["attributes"].get("format")] = fmts.get(x["attributes"].get("format"), 0) + 1
        dates = sorted(str(x["attributes"].get("data_date")) for x in resources)
        print(f"  resources: {len(resources)} formats={fmts} data_date {dates[:1]}..{dates[-1:]}")
        for x in resources[:6]:
            a2 = x["attributes"]
            print(f"   - {x['id']} {a2.get('data_date')} {a2.get('format')} {a2.get('title')!r} {a2.get('file_url') or a2.get('download_url')}")
        xlsx = [x for x in resources if str(x["attributes"].get("format")).lower() in ("xlsx", "xls")]
        picks = xlsx[:1] + xlsx[len(xlsx) // 2 : len(xlsx) // 2 + 1] + xlsx[-1:]
        for x in picks:
            url = x["attributes"].get("download_url") or x["attributes"].get("file_url")
            r = get(url) if url else None
            if r is None:
                continue
            print(f"  download {x['id']} status={r.status_code} bytes={len(r.content)} type={r.headers.get('Content-Type')}")
            if r.status_code == 200:
                save(f"file_{k}_{x['id']}.{x['attributes'].get('format', 'bin').lower()}", r.content)
    for url in argv:
        r = get(url)
        if r is not None:
            print(f"\n===== {url} status={r.status_code} bytes={len(r.content)}")
            save("page_" + re.sub(r"[^a-z0-9]+", "_", url.lower())[-60:] + ".html", r.content)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
