"""Read-only: GLEIF lei2/rr header → Bronze column names (compact). Deleted before merge."""

import csv
import io
import re
import tempfile
import zipfile

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}


def name(h):
    return re.sub(r"[^a-z0-9]+", "_", h.strip().lower()).strip("_")


for kind in ("lei2", "rr"):
    url = f"https://goldencopy.gleif.org/api/v2/golden-copies/publishes/{kind}/latest.csv"
    with tempfile.TemporaryFile() as tmp:
        with requests.get(url, headers=UA, stream=True, timeout=(30, 900)) as r:
            r.raise_for_status()
            for block in r.iter_content(8 << 20):
                tmp.write(block)
        tmp.seek(0)
        z = zipfile.ZipFile(tmp)
        with z.open(z.infolist()[0]) as m:
            header = next(csv.reader(io.TextIOWrapper(m, encoding="utf-8-sig", newline="")))
    names = [name(h) for h in header]
    # drop repeated groups: keep names whose numbered index is 1 (or none)
    short = [n for n in names if not re.search(r"_([2-9]|1[0-9])(_|$)", n)]
    text = " ".join(short)
    annotate("notice", f"{kind} names", {"total": len(names), "unique": len(set(names)), "kept": len(short)})
    for i in range(0, len(text), 3500):
        annotate("notice", f"{kind} names {i}", text[i : i + 3500])
