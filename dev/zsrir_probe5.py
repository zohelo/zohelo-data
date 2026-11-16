"""Read-only probe 5 for MRiRW ZSRIR (PLS-032): the public report files of zsrir.minrol.gov.pl. No R2 writes.

GetReportFileList?id=<report> for the core market bulletins, then the newest and one older workbook of each,
dumped (sheet names, first rows) with ingestion/common/xlsx_cells.py. Chain verification as in probe 3.
"""

from __future__ import annotations

from collections import Counter
import functools
import json
import os
from pathlib import Path
import ssl
import sys
import tempfile
import time

import certifi
import requests

print = functools.partial(print, flush=True)  # noqa: A001
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://zsrir.minrol.gov.pl"
AIA = "http://certumdvtlsg2r39ca.repository.certum.pl/certumdvtlsg2r39ca.cer"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
PAUSE = 1.5
REPORTS = {15: "current prices", 2: "cereals", 1: "milk", 14: "pork", 13: "beef", 6: "poultry", 7: "eggs", 16: "overview"}
session = requests.Session()
session.headers.update({"User-Agent": UA, "Accept": "application/json, text/plain, */*"})
VERIFY = ""


def bundle() -> str:
    pem = ssl.DER_cert_to_PEM_cert(session.get(AIA, timeout=(10, 30)).content)
    with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as f:
        f.write(open(certifi.where()).read() + "\n" + pem)
        return f.name


def get(path: str):
    time.sleep(PAUSE)
    try:
        return session.get(BASE + path, timeout=(10, 120), verify=VERIFY)
    except requests.RequestException as exc:
        print(f"GET {path}: {type(exc).__name__} {str(exc)[:200]}")
        return None


def dump(name: str, body: bytes) -> None:
    from ingestion.common import xlsx_cells

    rows: dict = {}
    try:
        for c in xlsx_cells.cells(body):
            rows.setdefault(c["sheet_name"], {}).setdefault(c["row_number"], []).append(
                f"{c['cell_ref']}={c['value_text'][:28]}" + (f"[{c['number_format']}]" if c["number_format"] not in ("General", "") else "")
            )
    except Exception as exc:  # noqa: BLE001 - probe
        print(f"  dump {name} failed: {type(exc).__name__} {str(exc)[:200]}")
        return
    print(f"  workbook {name}: {len(rows)} sheets {list(rows)[:30]}")
    for sn, rs in list(rows.items())[:10]:
        print(f"   sheet {sn!r}: rows {len(rs)} max_row {max(rs)}")
        for rn in sorted(rs)[:22]:
            print(f"     r{rn}: " + " | ".join(rs[rn][:12])[:330])


def items_of(body):
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for v in body.values():
            if isinstance(v, list):
                return v
    return []


def main(argv: list[str]) -> int:
    global VERIFY
    VERIFY = bundle()
    OUT.mkdir(parents=True, exist_ok=True)
    for rid, label in REPORTS.items():
        r = get(f"/api/ZsrirData/GetReportFileList?id={rid}")
        if r is None or r.status_code != 200:
            print(f"report {rid} {label}: {None if r is None else r.status_code}")
            continue
        body = r.json()
        items = items_of(body)
        print(f"\n### report {rid} {label}: {len(r.content)} bytes, top keys "
              f"{list(body)[:10] if isinstance(body, dict) else 'list'}, {len(items)} items")
        print("   head: " + json.dumps(body, ensure_ascii=False)[:1200])
        if items:
            print("   last: " + json.dumps(items[-3:], ensure_ascii=False)[:900])
            exts = Counter(str(i.get("fileName") or i.get("name") or "").rsplit(".", 1)[-1].lower() for i in items if isinstance(i, dict))
            print("   extensions:", dict(exts))
            dates = sorted(str(v)[:7] for i in items if isinstance(i, dict) for k, v in i.items() if "date" in k.lower() and v)
            print("   date range:", dates[:1], dates[-1:], "per year:", dict(Counter(d[:4] for d in dates)))
            xl = [i for i in items if isinstance(i, dict) and str(i.get("fileName") or i.get("name") or "").lower().endswith((".xlsx", ".xls"))]
            picks = xl[:1] + xl[len(xl) // 2: len(xl) // 2 + 1] + xl[-1:]
            for it in picks:
                fid = it.get("id")
                d = get(f"/api/ZsrirData/DownloadReportFile?id={fid}")
                if d is None:
                    continue
                print(f"  download {fid} {it.get('fileName') or it.get('name')}: {d.status_code} {len(d.content)} bytes "
                      f"{d.headers.get('Content-Type')} {d.headers.get('Content-Disposition')} LM={d.headers.get('Last-Modified')} ETag={d.headers.get('ETag')}")
                if d.status_code == 200:
                    (OUT / f"r{rid}_{fid}.bin").write_bytes(d.content)
                    if d.content[:2] == b"PK":
                        dump(f"{rid}/{fid}", d.content)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
