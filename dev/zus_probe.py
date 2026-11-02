"""Dev probe for #2464 (not for main, writes nothing): full yearbook links (pages 1-2) and their headers."""

from __future__ import annotations

import re
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
PAGE = "https://www.zus.pl/baza-wiedzy/statystyka/rocznik-statystyczny-ubezpieczen-spolecznych"


def main() -> int:
    try:
        return run()
    except Exception as exc:  # noqa: BLE001 - probe: report any failure
        annotate("error", "zus probe failed", f"{type(exc).__name__}: {exc}"[:500])
        return 1


def run() -> int:
    """Annotate every /documents/10182/167642/ link with its HEAD status, type, size and Last-Modified."""
    html = requests.get(PAGE, headers=UA, timeout=60).text
    pages = sorted(set(re.findall(r'href="([^"]*AssetPublisherPortlet_INSTANCE_\w+_cur=\d+)"', html)))
    for p in pages:
        html += requests.get(urljoin(PAGE, p.replace("&amp;", "&")), headers=UA, timeout=60).text
    links = []
    for href in re.findall(r'href="([^"]*/documents/10182/167642/[^"]+)"', html):
        u = urljoin(PAGE, href.replace("&amp;", "&"))
        if u not in links:
            links.append(u)
    out = []
    annotate("notice", "zus pagination", {"pages": pages, "links": len(links)})
    for u in links:
        try:
            h = requests.get(u, headers={**UA, "Accept-Encoding": "identity"}, timeout=60, stream=True)
            h.close()
            hb = requests.get(u.split("?")[0], headers=UA, timeout=60, stream=True)
            hb.close()
            out.append({"url": u, "status": h.status_code, "type": h.headers.get("Content-Type"),
                        "bytes": h.headers.get("Content-Length"), "lm": h.headers.get("Last-Modified"),
                        "bare_status": hb.status_code})
        except requests.RequestException as exc:
            out.append({"url": u, "error": str(exc)[:150]})
    for i in range(0, len(out), 4):
        annotate("notice", f"zus yearbooks {i // 4 + 1}", out[i : i + 4])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
