"""Dev probe 2 for #2468 (not for main, writes nothing): headers of the KRUS attachments."""

from __future__ import annotations

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
IDS = ["c27176c2-0f4f-4329-8f09-afb707d1fc09", "657d4965-e3c5-4d36-a321-32650b7df31d",
       "e29bb753-c6d7-4e78-8e48-9fe803fdbedd", "63d13aa0-ea1"]


def main() -> int:
    out = []
    for i in IDS:
        url = f"https://www.gov.pl/attachment/{i}"
        try:
            h = requests.head(url, headers={**UA, "Accept-Encoding": "identity"}, timeout=60, allow_redirects=True)
            g = requests.get(url, headers=UA, timeout=120)
            out.append({"id": i, "head": h.status_code, "get": g.status_code, "type": g.headers.get("Content-Type"),
                        "cd": g.headers.get("Content-Disposition"), "lm": g.headers.get("Last-Modified"),
                        "bytes": len(g.content), "magic": g.content[:8].hex()})
        except requests.RequestException as exc:
            out.append({"id": i, "error": str(exc)[:150]})
    annotate("notice", "krus attachments", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
