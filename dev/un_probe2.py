"""Dev probe 2 for #2394 and #2397 (not for main, writes nothing): UNdata SDMX dataflows, UNCTAD API paths."""

from __future__ import annotations

import re

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}


def get(url: str, **kw) -> requests.Response:
    return requests.get(url, headers={**UA, **kw.pop("headers", {})}, timeout=180, **kw)


def main() -> int:
    """Annotate the UNdata dataflow ids and the UNCTAD data centre's API paths."""
    r = get("https://data.un.org/legacy/ws/rest/dataflow")
    flows = re.findall(r'<structure:Dataflow id="([^"]+)"[^>]*>.*?<common:Name xml:lang="en">([^<]+)', r.text, re.S)
    annotate("notice", "undata dataflows", flows)
    if flows:
        fid = flows[0][0]
        for url, hdr in [
            (f"https://data.un.org/legacy/ws/rest/data/{fid}/all?lastNObservations=1", {"Accept": "text/csv"}),
            ("https://data.un.org/", {}),
        ]:
            d = get(url, headers=hdr)
            annotate("notice", f"undata {url[-60:]}", {"status": d.status_code, "bytes": len(d.content),
                     "type": d.headers.get("Content-Type"), "head": d.text[:600]})
    page = get("https://unctadstat.unctad.org/datacentre/").text
    scripts = re.findall(r'src="([^"]+\.js)"', page)
    paths: set[str] = set()
    for s in scripts:
        js = get("https://unctadstat.unctad.org/datacentre/" + s).text
        paths |= set(re.findall(r'["\'`](/?(?:api|bulkdownload|odata)[/\w.{}$-]{0,80})', js))
        paths |= set(re.findall(r"https://[\w.-]*unctad[\w.-]*/[\w/.-]{0,60}", js))
    annotate("notice", "unctad js paths", sorted(paths)[:120])
    for url in ["https://unctadstat-api.unctad.org/api/reportInfo/US.GDPTotal",
                "https://unctadstat-api.unctad.org/api/reports",
                "https://unctadstat-api.unctad.org/bulkdownload/US.FdiFlowsStock/US_FdiFlowsStock",
                "https://unctadstat-api.unctad.org/bulkdownload/US.TradeMerchTotal/US_TradeMerchTotal"]:
        d = get(url)
        annotate("notice", f"unctad {url[-50:]}", {"status": d.status_code, "bytes": len(d.content),
                 "type": d.headers.get("Content-Type"), "cd": d.headers.get("Content-Disposition"),
                 "head": d.content[:6].hex() if d.content[:2] == b"7z" else d.text[:500]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
