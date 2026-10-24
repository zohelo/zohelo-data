"""Read-only: GLEIF stable latest URLs, repex header, old GLEIF tables. Deleted before merge."""

import csv
import io
import tempfile
import zipfile

import requests

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
out = {}
for kind in ("lei2", "rr", "repex"):
    url = f"https://goldencopy.gleif.org/api/v2/golden-copies/publishes/{kind}/latest.csv"
    try:
        h = requests.head(url, headers=UA, timeout=60, allow_redirects=True)
        out[kind] = [
            h.status_code,
            h.url,
            h.headers.get("Last-Modified"),
            h.headers.get("Content-Length"),
            h.headers.get("ETag"),
            [r.status_code for r in h.history],
        ]
    except Exception as exc:
        out[kind] = repr(exc)[:200]
annotate("notice", "gleif latest urls", out)
url = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/repex/latest.csv"
with tempfile.TemporaryFile() as tmp:
    with requests.get(url, headers=UA, stream=True, timeout=(30, 600)) as r:
        r.raise_for_status()
        for block in r.iter_content(8 << 20):
            tmp.write(block)
        size = tmp.tell()
    tmp.seek(0)
    z = zipfile.ZipFile(tmp)
    with z.open(z.infolist()[0]) as m:
        reader = csv.reader(io.TextIOWrapper(m, encoding="utf-8-sig", newline=""))
        header, row = next(reader), next(reader)
annotate(
    "notice",
    "gleif repex",
    {
        "mb": round(size / 1e6, 1),
        "members": [(i.filename, i.file_size) for i in z.infolist()],
        "header": header,
        "row": row,
    },
)
cat = catalog()
annotate(
    "notice",
    "gleif old tables",
    {
        ns: [".".join(t) for t in cat.list_tables(ns) if "gleif" in t[-1] or "biala" in t[-1] or "opendata" in t[-1]]
        for ns in ("bronze", "silver", "gold")
    },
)
