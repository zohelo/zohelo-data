"""Dev probe for #2385 (not for main, writes nothing): which World Bank routes give ICP (source 90) in bulk."""

from __future__ import annotations

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
URLS = [
    "https://api.worldbank.org/v2/sources/90?format=json",
    "https://api.worldbank.org/v2/sources/90/series?format=json&per_page=1",
    "https://api.worldbank.org/v2/sources/90/country/all/series/all/time/all?format=json&per_page=1",
    "https://api.worldbank.org/v2/sources/90/country/all/series/all/time/all?format=json&per_page=50000&page=1",
    "https://databank.worldbank.org/data/download/ICP_CSV.zip",
    "https://databank.worldbank.org/data/download/ICP_2021_CSV.zip",
    "https://api.worldbank.org/v2/en/sources/90?downloadformat=csv",
]


def main() -> int:
    """GET each URL (bounded timeout) and annotate status, size, type, Last-Modified and the head."""
    out = []
    for url in URLS:
        try:
            r = requests.get(url, headers=UA, timeout=120)
            out.append({"url": url[-90:], "status": r.status_code, "bytes": len(r.content),
                        "type": r.headers.get("Content-Type"), "lm": r.headers.get("Last-Modified"),
                        "head": r.content[:220].decode("utf-8", "replace")})
        except requests.RequestException as exc:
            out.append({"url": url[-90:], "error": f"{type(exc).__name__}: {str(exc)[:150]}"})
    for i in range(0, len(out), 3):
        annotate("notice", f"icp probe {i // 3 + 1}", out[i : i + 3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
