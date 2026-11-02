"""Dev probe for #2394 and #2397 (not for main, writes nothing): bulk routes of UNdata and UNCTAD Data Hub."""

from __future__ import annotations

import re

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
URLS = [
    # UNdata: download handler per mart/table, SDMX REST catalogue
    "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=tableCode:22&DataMartId=POP&Format=csv",
    "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=&DataMartId=POP&Format=csv",
    "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=grID:101;currID:USD;pcFlag:0&DataMartId=SNAAMA&Format=csv",
    "https://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=cmID:EL&DataMartId=EDATA&Format=csv",
    "https://data.un.org/ws/rest/dataflow",
    "https://data.un.org/Handlers/ExplorerHandler.ashx?t=sources",
    "https://data.un.org/Explorer.aspx",
    # UNCTAD Data Hub: bulk file per table
    "https://unctadstat-api.unctad.org/bulkdownload/US.GDPTotal/US_GDPTotal",
    "https://unctadstat-api.unctad.org/bulkdownload/US.PLSCI/US_PLSCI",
    "https://unctadstat.unctad.org/datacentre/dataviewer/US.GDPTotal",
    "https://unctadstat.unctad.org/datacentre/reportInfo/US.GDPTotal",
    "https://unctadstat.unctad.org/release-calendar",
    "https://unctadstat.unctad.org/datacentre/",
]


def main() -> int:
    """GET each URL (bounded timeout) and annotate status, size, type, Last-Modified, disposition and the head."""
    out = []
    for url in URLS:
        try:
            r = requests.get(url, headers=UA, timeout=180)
            body = r.content
            text = body[:4000].decode("utf-8", "replace")
            hint = sorted(set(re.findall(r"(?:bulkdownload|DataMartId=|d=)[\w.%:;=/-]{2,60}", body.decode("utf-8", "replace"))))[:25]
            out.append({"url": url[-95:], "status": r.status_code, "bytes": len(body),
                        "type": r.headers.get("Content-Type"), "lm": r.headers.get("Last-Modified"),
                        "cd": r.headers.get("Content-Disposition"), "final": r.url[-95:],
                        "head": body[:6].hex() if body[:2] in (b"PK", b"7z") else text[:300],
                        "hints": hint})
        except requests.RequestException as exc:
            out.append({"url": url[-95:], "error": f"{type(exc).__name__}: {str(exc)[:150]}"})
    for i in range(0, len(out), 2):
        annotate("notice", f"un probe {i // 2 + 1}", out[i : i + 2])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
