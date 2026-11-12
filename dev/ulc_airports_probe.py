"""Read-only probe of the ULC airport statistics (PL-ENV-026, #2967): listing page and every linked PDF.

One request at a time, PAUSE seconds apart, the platform's user agent; no R2 writes. Stops at the first refusal
or bot challenge (HTTP 401/403/429/503, an HTML answer instead of a PDF, an Incapsula marker) and records it;
never retries around it. Files go to DEV_OUTPUT_DIR (the run's artifact, 7 days) for parser development.
"""

from __future__ import annotations

from hashlib import sha256
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import time
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

LISTING = "https://ulc.gov.pl/statystyki-analizy/statystyki-wg-portow-lotniczych"
PAUSE = 4.0
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))


class Anchors(HTMLParser):
    def __init__(self, base: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base, self.out, self.open = base, [], False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.out.append([urljoin(self.base, (dict(attrs).get("href") or "").strip()), ""])
            self.open = True

    def handle_data(self, data):
        if self.open:
            self.out[-1][1] += data

    def handle_endtag(self, tag):
        if tag == "a":
            self.open = False


def challenged(response: requests.Response) -> str | None:
    body = response.content[:4000]
    if response.status_code in (401, 403, 429, 503):
        return f"HTTP {response.status_code}"
    if b"_Incapsula_Resource" in body or b"captcha" in body.lower():
        return "bot challenge page"
    return None


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "pdf").mkdir(exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    page = session.get(LISTING, timeout=(30, 60))
    (OUT / "listing.html").write_bytes(page.content)
    keep = ("content-type", "last-modified", "server", "x-cdn", "x-iinfo")
    meta = {
        "listing_status": page.status_code,
        "listing_bytes": len(page.content),
        "listing_headers": {k: v for k, v in page.headers.items() if k.lower() in keep},
    }
    refusal = challenged(page)
    if refusal:
        meta["refused"] = {"url": LISTING, "reason": refusal}
        annotate("error", "ulc probe refused", meta)
        (OUT / "probe.json").write_text(json.dumps(meta, indent=2))
        return 1
    parser = Anchors(page.url)
    parser.feed(page.text)
    links = [(h, " ".join(t.split())) for h, t in parser.out if h.lower().split("?")[0].endswith(".pdf")]
    meta["pdf_links"] = len(links)
    files = []
    for n, (href, text) in enumerate(links):
        time.sleep(PAUSE)
        try:
            response = session.get(href, timeout=(30, 120), headers={"Accept-Encoding": "identity"})
        except requests.RequestException as exc:
            files.append({"n": n, "url": href, "text": text, "error": type(exc).__name__})
            continue
        refusal = challenged(response)
        record = {
            "n": n,
            "url": href,
            "text": text,
            "status": response.status_code,
            "bytes": len(response.content),
            "sha256": sha256(response.content).hexdigest(),
            "pdf": response.content[:5] == b"%PDF-",
            "last_modified": response.headers.get("Last-Modified"),
            "etag": response.headers.get("ETag"),
            "content_type": response.headers.get("Content-Type"),
        }
        files.append(record)
        if refusal or not record["pdf"]:
            meta["refused"] = {"url": href, "reason": refusal or "not a PDF", "after_files": n}
            break
        (OUT / "pdf" / f"{n:03d}__{href.rsplit('/', 1)[-1]}").write_bytes(response.content)
    meta["files"] = files
    (OUT / "probe.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    ok = [f for f in files if f.get("pdf")]
    summary = {
        "listing": page.status_code,
        "pdf_links": len(links),
        "pdf_ok": len(ok),
        "bytes": sum(f["bytes"] for f in ok),
        "refused": meta.get("refused"),
        "first": links[:2],
        "last": links[-2:],
    }
    annotate("warning" if "refused" in meta else "notice", "ulc probe", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
