"""Probe the PRG national package: HEAD, then download once and list ZIP members (no R2 writes)."""
import io
import zipfile

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
URLS = ["https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip",
        "https://opendata.geoportal.gov.pl/prg/granice/"]
out = {}
s = requests.Session()
s.headers["User-Agent"] = UA
for url in URLS:
    r = s.head(url, timeout=60, allow_redirects=True)
    out[url] = {"status": r.status_code, "final": r.url, "length": r.headers.get("Content-Length"),
                "modified": r.headers.get("Last-Modified"), "etag": r.headers.get("ETag"),
                "type": r.headers.get("Content-Type"), "ranges": r.headers.get("Accept-Ranges")}
listing = s.get(URLS[1], timeout=60)
out["listing"] = listing.text[:1500] if listing.ok else listing.status_code
r = s.get(URLS[0], timeout=900)
z = zipfile.ZipFile(io.BytesIO(r.content))
out["members"] = [(i.filename, i.file_size) for i in z.infolist()][:40]
out["bytes"] = len(r.content)
annotate("notice", "prg probe", out)
