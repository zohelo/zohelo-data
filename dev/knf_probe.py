"""Read-only probe of KNF statistics (PLG-017, PLG-018, #2871): robots.txt, the banking-sector page and the
statistics listing (links and titles), and the newest workbooks, printed as base64 to the job log so the build
can be written against the real files. Nothing is written to R2. One request at a time, PAUSE apart. Deleted
before merge."""

from __future__ import annotations

import base64
import hashlib
import html
import re
import sys
import time
import urllib.robotparser

import requests

from ingestion.to_landing.patterns import bulk_file

PAUSE = 4.0
BASE = "https://www.knf.gov.pl"
BANK = BASE + "/publikacje_i_opracowania/dane_statystyczne?articleId=56224&p_id=18"
LIST = BASE + "/publikacje_i_opracowania/dane_statystyczne"


def get(url: str) -> requests.Response:
    time.sleep(PAUSE)
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 120))
            print("GET", r.status_code, len(r.content), r.headers.get("content-type"), r.headers.get("last-modified"), url, flush=True)
            return r
        except requests.RequestException as exc:
            print("retry", url, exc, flush=True)
            time.sleep(15 * (attempt + 1))
    raise RuntimeError(url)


def anchors(text: str, page: str):
    for m in re.finditer(r'<a\s[^>]*href="([^"]+)"[^>]*>(.*?)</a>', text, re.S | re.I):
        href = html.unescape(m.group(1))
        label = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", m.group(2))).split())
        yield requests.compat.urljoin(page, href), label


def dump(name: str, body: bytes) -> None:
    b64 = base64.b64encode(body).decode()
    print(f"FILE_BEGIN {name} {len(body)} {hashlib.sha256(body).hexdigest()}", flush=True)
    for i in range(0, len(b64), 4000):
        print("B64 " + b64[i:i + 4000])
    print(f"FILE_END {name}", flush=True)


def main(argv) -> int:
    r = get(BASE + "/robots.txt")
    print("ROBOTS", r.status_code, r.text[:3000].replace("\n", " || "), flush=True)
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(r.text.splitlines() if r.status_code == 200 else [])
    for u in (BANK, LIST, BASE + "/knf/pl/komponenty/img/x.xlsx"):
        print("CAN_FETCH", rp.can_fetch(bulk_file.USER_AGENT, u), u, flush=True)
    if r.status_code == 200 and not rp.can_fetch(bulk_file.USER_AGENT, LIST):
        print("robots.txt disallows: stop", flush=True)
        return 1

    page = get(BANK)
    text = page.text
    plain = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"(?s)<(script|style).*?</\1>", " ", text))).split())
    at = plain.find("Dane miesięczne")
    print("BANK_TEXT", plain[max(0, at - 300): at + 2500], flush=True)
    for m in re.finditer(r"(Przy wykorzystywaniu[^.]{0,300}\.)", plain):
        print("NOTICE", m.group(1), flush=True)
    files = []
    for href, label in anchors(text, page.url):
        if re.search(r"\.(xlsx?|zip|csv|pdf)(\?|$)", href, re.I):
            print("BANK_LINK", label[:150], "->", href, flush=True)
            files.append(href)
    for href in [f for f in files if re.search(r"\.xlsx$", f, re.I)][:3]:
        dump("bank__" + href.rsplit("/", 1)[-1], get(href).content)

    # statistics listing: first pages, links and titles
    articles = []
    for n, url in enumerate([LIST, LIST + "?p_id=18&cur=2", LIST + "?cur=2"]):
        lp = get(url)
        for href, label in anchors(lp.text, lp.url):
            if "articleId=" in href or "cur=" in href or "delta=" in href:
                print(f"LIST{n}", label[:160], "->", href, flush=True)
                if "articleId=" in href:
                    articles.append((href, label))
    seen = set()
    wanted = re.compile(r"(?i)(OFE|fundusz.*emerytal|IKE|IKZE|PPK|ubezpiecz|PTE|bank|sektor|kas|SKOK|TFI|inwestycyjn|makler|kwartaln|miesi)")
    picked = 0
    for href, label in articles:
        if href in seen or not wanted.search(label):
            continue
        seen.add(href)
        if picked >= 10:
            break
        picked += 1
        ap = get(href)
        for f, flabel in anchors(ap.text, ap.url):
            if re.search(r"\.(xlsx?|zip|csv)(\?|$)", f, re.I):
                print("ART_LINK", label[:100], "|", flabel[:120], "->", f, flush=True)
                if re.search(r"(?i)(OFE|IKE|IKZE|PPK|ubezp|_A_|^A_|informacje|raport)", f.rsplit("/", 1)[-1] + " " + label) and f.lower().endswith(".xlsx"):
                    dump("art__" + f.rsplit("/", 1)[-1], get(f).content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
