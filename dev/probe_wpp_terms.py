"""Read-only probe (GL-039): the UN WPP terms of use as served to GitHub Actions. Writes nothing."""

from __future__ import annotations

import re

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
URLS = [
    "https://population.un.org/wpp/",
    "https://population.un.org/wpp/downloads",
    "https://population.un.org/wpp/about",
    "https://population.un.org/wpp/about-us",
    "https://population.un.org/wpp/terms-of-use",
    "https://population.un.org/wpp/assets/Files/WPP2024_Summary-of-Results.pdf",
    "https://population.un.org/dataportal/about/termsofuse",
    "https://population.un.org/dataportal/about/dataportal",
    "https://www.un.org/development/desa/pd/node/4327",
    "https://www.un.org/en/about-us/terms-of-use",
]
PAT = re.compile(r"(creative\s*commons|cc\s*by[\w .-]{0,20}igo|license|licence|terms of use|copyright)", re.I)


def main() -> int:
    for url in URLS:
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(20, 120))
        except Exception as exc:
            print(f"### {url} error {exc!r}"[:300], flush=True)
            continue
        print(f"### {url} status {r.status_code} type {r.headers.get('Content-Type')} bytes {len(r.content)}", flush=True)
        text = r.text if "pdf" not in (r.headers.get("Content-Type") or "") else r.content.decode("latin-1", "replace")
        plain = re.sub(r"<[^>]+>", " ", text)
        plain = re.sub(r"\s+", " ", plain)
        links = sorted(set(re.findall(r'href="([^"]*(?:term|licen|copyright|about)[^"]*)"', text, re.I)))[:30]
        print(f"### {url} links {links}", flush=True)
        for m in list(PAT.finditer(plain))[:12]:
            print(f"### {url} ctx: ...{plain[max(0, m.start() - 250): m.end() + 250]}...", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
