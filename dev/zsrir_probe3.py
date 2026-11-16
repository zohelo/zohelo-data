"""Read-only probe 3 for MRiRW ZSRIR (PLS-032): the new portal's own API, read from its JavaScript. No R2 writes.

zsrir.minrol.gov.pl sends only its leaf certificate; the issuing intermediate (Certum DV TLS G2 R39 CA) is the CA's
own file named in the leaf's Authority Information Access field. It is added to certifi's roots, and the chain is
verified to Certum Trusted Root CA as usual (verification stays on). One request at a time, PAUSE seconds apart.
"""

from __future__ import annotations

import functools
import re
import ssl
import sys
import tempfile
import time

import certifi
import requests

print = functools.partial(print, flush=True)  # noqa: A001 - probe output must reach the log if the job is stopped
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://zsrir.minrol.gov.pl"
AIA = "http://certumdvtlsg2r39ca.repository.certum.pl/certumdvtlsg2r39ca.cer"
PAUSE = 1.5
session = requests.Session()
session.headers.update({"User-Agent": UA})


def bundle() -> str:
    der = session.get(AIA, timeout=(10, 30)).content
    pem = ssl.DER_cert_to_PEM_cert(der)
    print("intermediate PEM:\n" + pem)
    with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as f:
        f.write(open(certifi.where()).read() + "\n" + pem)
        return f.name


def get(url: str, verify: str, **kw):
    time.sleep(PAUSE)
    try:
        return session.get(url, timeout=(10, 60), verify=verify, **kw)
    except requests.RequestException as exc:
        print(f"  {url}: {type(exc).__name__} {str(exc)[:200]}")
        return None


def main(argv: list[str]) -> int:
    verify = bundle()
    index = get(BASE + "/", verify)
    scripts = sorted(set(re.findall(r'src="([^"]+\.js)"', index.text) + re.findall(r'href="([^"]+\.js)"', index.text)))
    seen: set[str] = set()
    queue = list(scripts)
    strings: set[str] = set()
    while queue and len(seen) < 40:
        js = queue.pop(0)
        if js in seen:
            continue
        seen.add(js)
        r = get(f"{BASE}/{js.lstrip('/')}", verify)
        if r is None or r.status_code != 200:
            continue
        text = r.text
        print(f"js {js}: {len(text)} chars")
        for chunk in re.findall(r'["\'`](\./chunk-[A-Z0-9]+\.js|chunk-[A-Z0-9]+\.js)["\'`]', text):
            queue.append(chunk.lstrip("./"))
        for s in re.findall(r'["\'`]([^"\'`\s]{2,160})["\'`]', text):
            low = s.lower()
            if any(k in low for k in ("api", "http", "/raport", "xlsx", "xls", "csv", "export", "pobierz", "download",
                                      "notowan", "cen", "towar", "produkt", "rynek", "rynk", "biulet", "endpoint")):
                if not low.startswith(("mat-", "cdk-", "--")) and len(s) < 160:
                    strings.add(s)
    print(f"\n== {len(strings)} candidate strings")
    for s in sorted(strings):
        print("   ", s)
    for path in argv:
        r = get(BASE + path, verify)
        if r is not None:
            print(f"\n== {path}: {r.status_code} {len(r.content)} {r.headers.get('Content-Type')}\n{r.text[:3000]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
