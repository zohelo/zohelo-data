"""Read-only probe for CD-006 (KNF OAM list) and CD-007 (GPW report list). GET only, one request at a time,
5 seconds apart, identified User-Agent. Prints page structure to the job log; nothing is written."""

from collections import Counter
import re
import sys
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAUSE = 5
_n = 0


def get(url, **kw):
    global _n
    if _n:
        time.sleep(PAUSE)
    _n += 1
    t0 = time.time()
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept-Language": "pl,en"}, timeout=60, **kw)
    except requests.RequestException as exc:
        print(f"### GET {url} -> {type(exc).__name__}: {str(exc)[:200]}", flush=True)
        return None
    print(f"### GET {url} -> {r.status_code} {len(r.content)} bytes {time.time() - t0:.1f}s final={r.url} "
          f"ct={r.headers.get('content-type')} lm={r.headers.get('last-modified')} enc={r.encoding} "
          f"server={r.headers.get('server')} setcookie={'yes' if r.headers.get('set-cookie') else 'no'}", flush=True)
    return r


def norm(href):
    return re.sub(r"[0-9A-Fa-f]{16,}", "<HEX>", re.sub(r"\d{3,}", "<N>", href))


def structure(html, rows=3, around=()):
    hrefs = re.findall(r"""(?i)(?:href|src|action)\s*=\s*["']([^"']+)""", html)
    for pat, n in Counter(norm(h) for h in hrefs).most_common(70):
        print(f"  link {n:4d}  {pat[:200]}")
    for tag in re.findall(r"(?is)<(?:form|input|select|option|button)\b[^>]*>", html)[:80]:
        print("  tag ", re.sub(r"\s+", " ", tag)[:260])
    trs = re.findall(r"(?is)<tr\b.*?</tr>", html)
    print(f"  tr count {len(trs)}")
    for tr in [t for t in trs if "href" in t][:rows]:
        print("  ROW ", re.sub(r"\s+", " ", tr)[:1500])
    for word in around:
        for m in list(re.finditer(word, html))[:6]:
            print(f"  AROUND {word!r} @{m.start()}: ", re.sub(r"\s+", " ", html[max(0, m.start() - 500): m.start() + 900]))
    print("  HEAD ", re.sub(r"\s+", " ", html[:1500]))
    print("  TAIL ", re.sub(r"\s+", " ", html[-1200:]), flush=True)


def knf():
    r = get("https://moam.knf.gov.pl/robots.txt")
    if r is not None:
        print("  robots body:", r.text[:300].replace("\n", " | ") if "html" not in (r.headers.get("content-type") or "") else "(html page)")
    r = get("https://moam.knf.gov.pl/moam.nsf")
    if r is None or r.status_code != 200:
        return
    html = r.text
    structure(html, around=("Liczba raport", "(?i)regulamin|prawa autorskie|copyright|warunki korzystania"))
    # follow at most 6 same-host navigation links that are not report documents
    links = []
    for h in re.findall(r"""(?i)href\s*=\s*["']([^"']+)""", html):
        u = requests.compat.urljoin(r.url, h)
        if "moam.knf.gov.pl" in u and not re.search(r"[0-9A-Fa-f]{32}", u) and u not in links and u.rstrip("/") != r.url.rstrip("/"):
            links.append(u)
    print("  NAV candidates:", links[:25])
    for u in [x for x in links if not re.search(r"\.(css|js|ico|png|gif|jpg)(\?|$)", x)][:6]:
        rr = get(u)
        if rr is not None and rr.status_code == 200 and "html" in (rr.headers.get("content-type") or ""):
            structure(rr.text, rows=2, around=("Liczba raport",))


def gpw():
    r = get("https://www.gpw.pl/robots.txt")
    if r is not None:
        print("  robots body:", r.text[:600].replace("\n", " | "))
    r = get("https://www.gpw.pl/espi-ebi-reports")
    if r is None or r.status_code != 200:
        return
    html = r.text
    structure(html, rows=0, around=("(?i)ajaxindex", "(?i)show more|pokaż więcej|more-", "geru_id", "(?i)offset|limit"))
    for m in list(re.finditer(r"(?is)<li\b[^>]*>(?:(?!</li>).)*geru_id(?:(?!</li>).)*</li>", html))[:3]:
        print("  ITEM ", re.sub(r"\s+", " ", m.group(0))[:2500])
    ids = re.findall(r"geru_id=(\d+)", html)
    print("  geru ids:", len(ids), len(set(ids)), min(ids, default=None), max(ids, default=None))
    print("  type words:", Counter(re.findall(r">\s*(ESPI|EBI)\s*<", html)))
    print("  dates:", Counter(re.findall(r"\d{2}-\d{2}-\d{4}|\d{4}-\d{2}-\d{2}", html)).most_common(8), flush=True)


if __name__ == "__main__":
    which = sys.argv[1:] or ["knf", "gpw"]
    if "knf" in which:
        knf()
    if "gpw" in which:
        gpw()
