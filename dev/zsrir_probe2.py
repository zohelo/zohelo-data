"""Read-only probe 2 for MRiRW ZSRIR (PLS-032): route finding. No R2 writes.

1. The certificate chain zsrir.minrol.gov.pl presents (diagnosis only, no data request without verification).
2. Every link of the gov.pl ZSRIR and bulletin pages (attachments, dane.gov.pl datasets, portal pages).
3. Reachability of dane.gov.pl and api.dane.gov.pl (one request each, 30 s).
One request at a time, PAUSE seconds apart.
"""

from __future__ import annotations

import subprocess
import sys
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAUSE = 2.0
PAGES = [
    "https://www.gov.pl/web/rolnictwo/zintegrowany-system-rolniczej-informacji-rynkowej",
    "https://www.gov.pl/web/rolnictwo/biuletyny-informacyjne?page=2&size=10",
    "https://www.gov.pl/web/rolnictwo/rynek-wieprzowiny",
    "https://www.gov.pl/web/rolnictwo/rynek-drobiu",
    "https://www.gov.pl/web/rolnictwo/notowania-cen",
    "https://www.gov.pl/web/rolnictwo/ceny-produktow-rolnych",
]
REACH = [
    "https://dane.gov.pl/pl/dataset/1022",
    "https://api.dane.gov.pl/1.4/datasets/1022",
    "https://zsrir.minrol.gov.pl/about",
]


def main(argv: list[str]) -> int:
    out = subprocess.run(
        ["bash", "-c", "echo | timeout 30 openssl s_client -connect zsrir.minrol.gov.pl:443 -servername zsrir.minrol.gov.pl -showcerts 2>&1 | grep -E 's:|i:|Verify return|verify error|depth=|Certificate chain' | head -30"],
        capture_output=True, text=True, timeout=60,
    )
    print("== openssl chain of zsrir.minrol.gov.pl\n" + out.stdout)
    out = subprocess.run(
        ["bash", "-c", "echo | timeout 30 openssl s_client -connect zsrir.minrol.gov.pl:443 -servername zsrir.minrol.gov.pl 2>/dev/null | openssl x509 -noout -text | grep -A2 -E 'Authority Information Access|Issuer:|Not After'"],
        capture_output=True, text=True, timeout=60,
    )
    print("== leaf AIA\n" + out.stdout)
    for url in REACH:
        time.sleep(PAUSE)
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
            print(f"reach {url}: {r.status_code} {len(r.content)} bytes {r.headers.get('Content-Type')}")
        except requests.RequestException as exc:
            print(f"reach {url}: {type(exc).__name__} {str(exc)[:200]}")
    from ingestion.common.gus_publications import _links

    for url in PAGES + argv:
        time.sleep(PAUSE)
        try:
            links = _links(url)
        except Exception as exc:  # noqa: BLE001 - probe
            print(f"\n===== {url}: {type(exc).__name__} {str(exc)[:200]}")
            continue
        print(f"\n===== {url}: {len(links)} links")
        for href, text in links:
            if "gov.pl/web/gov/" in href or "#" in href or href.rstrip("/").endswith("gov.pl"):
                continue
            if any(k in href for k in ("/attachment/", "dane.gov.pl", "rolnictwo/", "zsrir", "minrol", ".xls", ".pdf")):
                print(f"   {text[:100]!r} -> {href}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
