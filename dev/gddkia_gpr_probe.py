"""Read-only probe of the GDDKiA traffic census (GPR, register PL-ENV-022) edition pages on gov.pl.

Lists every attachment link of the two edition pages, downloads the spreadsheet attachments (XLSX / XLS, not the
PDF twins, maps or the SHP ZIP), one request at a time 2 s apart, and keeps them in DEV_OUTPUT_DIR (uploaded as the
run's artifact) so the sheet layout can be read offline. Writes nothing to R2. robots.txt is read first.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import time
import urllib.robotparser

import requests

from ingestion.common.gus_publications import _links
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

PAGES = {
    "gpr_2025": "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025",
    "gpr_2020": "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-20202021",
}
OUT = pathlib.Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
SHEET = re.compile(r"\bxlsx?\b", re.IGNORECASE)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    rp = urllib.robotparser.RobotFileParser()
    r = requests.get("https://www.gov.pl/robots.txt", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=30)
    rp.parse(r.text.splitlines())
    summary: dict = {"robots_status": r.status_code}
    listing = {}
    for edition, page in PAGES.items():
        allowed = rp.can_fetch(bulk_file.USER_AGENT, page) and rp.can_fetch("*", page)
        summary[f"{edition}_robots_ok"] = allowed
        if not allowed:
            continue
        time.sleep(2)
        links = [(h, t) for h, t in _links(page) if "/attachment/" in h]
        listing[edition] = links
        summary[f"{edition}_attachments"] = len(links)
        files = []
        for n, (href, text) in enumerate(links):
            if not SHEET.search(text) or "pdf" in text.lower().split()[-3:]:
                continue
            if not rp.can_fetch(bulk_file.USER_AGENT, href):
                files.append({"text": text[:60], "err": "robots"})
                continue
            time.sleep(2)
            resp = requests.get(href, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=120)
            body = resp.content
            name = f"{edition}_{n:02d}"
            kind = "xlsx" if body[:2] == b"PK" else ("xls" if body[:4] == b"\xd0\xcf\x11\xe0" else "other")
            (OUT / f"{name}.{kind}").write_bytes(body)
            files.append(
                {
                    "file": f"{name}.{kind}",
                    "status": resp.status_code,
                    "bytes": len(body),
                    "lm": resp.headers.get("Last-Modified"),
                    "cd": (resp.headers.get("Content-Disposition") or "")[:90],
                    "sha": hashlib.sha256(body).hexdigest()[:12],
                    "text": text[:110],
                }
            )
        (OUT / f"{edition}_files.json").write_text(json.dumps(files, ensure_ascii=False, indent=1))
        summary[f"{edition}_sheets"] = len(files)
    (OUT / "links.json").write_text(json.dumps(listing, ensure_ascii=False, indent=1))
    annotate("notice", "gddkia_gpr probe", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
