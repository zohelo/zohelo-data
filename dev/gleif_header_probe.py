"""Read-only: GLEIF golden-copy CSV headers and sizes for lei2, rr, repex. Deleted before merge."""

import csv
import io
import tempfile
import zipfile

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
latest = requests.get(
    "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest", headers=UA, timeout=60
).json()
latest = latest.get("data", latest)
for kind in ("lei2", "rr", "repex"):
    f = latest[kind]["full_file"]["csv"]
    info = {
        "records": f["record_count"],
        "mb": round(f["size"] / 1e6),
        "url": f["url"],
        "date": latest[kind]["publish_date"],
    }
    info["deltas"] = {k: v["csv"]["record_count"] for k, v in (latest[kind].get("delta_files") or {}).items()}
    with tempfile.TemporaryFile() as tmp:
        with requests.get(f["url"], headers=UA, stream=True, timeout=(30, 600)) as r:
            r.raise_for_status()
            for block in r.iter_content(8 << 20):
                tmp.write(block)
        tmp.seek(0)
        z = zipfile.ZipFile(tmp)
        info["members"] = [(i.filename, i.file_size) for i in z.infolist()]
        with z.open(z.infolist()[0]) as m:
            text = io.TextIOWrapper(m, encoding="utf-8-sig", newline="")
            reader = csv.reader(text)
            header = next(reader)
            row = next(reader)
    info["n_columns"] = len(header)
    annotate("notice", f"gleif {kind}", info)
    for i in range(0, len(header), 60):
        annotate("notice", f"gleif {kind} header {i}", header[i : i + 60])
    annotate("notice", f"gleif {kind} first row", dict(zip(header[:40], row[:40], strict=False)))
