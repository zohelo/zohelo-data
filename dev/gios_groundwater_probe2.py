"""Read-only probe 2 (PL-ENV-012, #2870): is mjwp.gios.gov.pl down for everyone or only refusing pages, and does
dane.gov.pl carry the groundwater result files? No R2, no writes; one request at a time, 3 s apart."""

from __future__ import annotations

import json
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
URLS = [
    ("GET", "https://mjwp.gios.gov.pl/"),
    ("GET", "https://mjwp.gios.gov.pl/robots.txt"),
    ("HEAD", "https://mjwp.gios.gov.pl/g2/oryginal/2026_03/a9590ac1beebee567d198e94179f5a37.xlsx"),
    ("HEAD", "https://mjwp.gios.gov.pl/g2/oryginal/2026_03/a11519d817605decc04b34db8e993004.xlsx"),
    ("GET", "https://www.gios.gov.pl/pl/"),
    ("GET", "https://powietrze.gios.gov.pl/pjp/archives"),
    ("GET", "https://wody.gios.gov.pl/pjwp/"),
    ("GET", "https://api.dane.gov.pl/1.4/datasets?q=w%C3%B3d%20podziemnych&per_page=50"),
    ("GET", "https://api.dane.gov.pl/1.4/datasets?q=wody%20podziemne&per_page=50"),
    ("GET", "https://api.dane.gov.pl/1.4/search?q=monitoring%20jako%C5%9Bci%20w%C3%B3d%20podziemnych&per_page=50"),
    ("GET", "https://www.psh.gov.pl/"),
]


def main() -> int:
    for method, url in URLS:
        time.sleep(3)
        try:
            r = requests.request(method, url, headers={"User-Agent": UA}, timeout=(20, 60), allow_redirects=True)
        except requests.RequestException as exc:
            print(json.dumps({"url": url, "error": f"{type(exc).__name__}: {exc}"[:200]}), flush=True)
            continue
        head = {k: v for k, v in r.headers.items() if k.lower() in (
            "server", "content-type", "content-length", "last-modified", "retry-after", "date", "x-cache", "via")}
        out = {"url": url, "method": method, "http": r.status_code, "final": r.url, "bytes": len(r.content), "headers": head}
        if "dane.gov.pl" in url and r.status_code == 200:
            try:
                data = r.json().get("data", [])
                out["hits"] = [
                    {"id": d.get("id"), "title": (d.get("attributes") or {}).get("title"),
                     "institution": ((d.get("relationships") or {}).get("institution") or {}).get("data", {}).get("id"),
                     "modified": (d.get("attributes") or {}).get("modified"), "model": (d.get("attributes") or {}).get("model")}
                    for d in data][:50]
            except ValueError:
                out["body"] = r.text[:300]
        elif method == "GET":
            out["body"] = " ".join(r.text.split())[:600]
        print(json.dumps(out, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
