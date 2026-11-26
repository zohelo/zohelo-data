"""Read-only probe 3 (#3119): how the archive page offers its files (plain links or a reCAPTCHA-guarded download),
the page's scripts that define downloadFile, and the portal's Regulamin text on licence and reuse. Pages only, no
file downloads, no R2. One request at a time, 3 s apart."""

from __future__ import annotations

import re
import time

import requests

BASE = "https://powietrze.gios.gov.pl"
S = requests.Session()
S.headers["User-Agent"] = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def get(url: str) -> requests.Response:
    time.sleep(3)
    return S.get(url, timeout=(30, 120))


def text(html: str) -> str:
    html = re.sub(r"(?s)<script.*?</script>|<style.*?</style>", " ", html)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def main() -> int:
    page = get(f"{BASE}/pjp/archives").text
    for m in re.finditer(r"<a[^>]*downloadFile[^>]*>.*?</a>", page, re.S):
        print("ANCHOR_TAG", re.sub(r"\s+", " ", m.group(0))[:400])
    for m in re.finditer(r"recaptcha|captcha|downloadButton|function downloadFile", page, re.I):
        print("CAPTCHA_CONTEXT", m.group(0), "::", re.sub(r"\s+", " ", page[max(0, m.start() - 700) : m.end() + 700]))
    print("PAGE_TEXT", text(page)[:6000])
    scripts = sorted(set(re.findall(r'<script[^>]+src="([^"]+)"', page)))
    print("SCRIPTS", scripts)
    for src in scripts:
        url = src if src.startswith("http") else BASE + src if src.startswith("/") else f"{BASE}/pjp/{src}"
        if "powietrze.gios.gov.pl" not in url:
            continue
        body = get(url).text
        for m in re.finditer(r"downloadFile|captcha", body, re.I):
            print("SCRIPT", src, "::", re.sub(r"\s+", " ", body[max(0, m.start() - 600) : m.end() + 900]))
    terms = text(get(f"{BASE}/pjp/content/terms_of_service").text)
    print("TERMS_LEN", len(terms))
    for i in range(0, min(len(terms), 30000), 3000):
        print("TERMS", i, terms[i : i + 3000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
