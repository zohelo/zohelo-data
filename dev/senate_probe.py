"""Read-only size and layout probe of www.senat.gov.pl for PLG-025 (#482).

GET only, one connection (requests.Session), one request at a time, at least 1.1 seconds apart, at most 320
requests, never /gfx/ or /pdf/. robots.txt first: compared line by line with the register quote; any difference
stops the probe. An HTTP 401/403/429 or a bot-protection page stops the probe and is not retried. Prints status,
size and link counts, and a few sample pages as gzip+base64 (``PAGE <name> <b64>``) to the job log, so the parser
can be written against the real bytes. Writes nothing to R2.
"""

from __future__ import annotations

import base64
import gzip
import re
import sys
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://www.senat.gov.pl"
EXPECTED_ROBOTS = [
    "user-agent: *",
    "disallow: /gfx/",
    "disallow: /pdf/",
    "allow: /*.pdf$",
    "allow: /*.doc$",
    "allow: /*.xls$",
    "allow: /*.rtf$",
    "user-agent: fasterfox",
    "disallow: /",
]
BUDGET = [320]
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept-Language": "pl"})
LAST = [0.0]
BOT = re.compile(r"request rejected|captcha|cf-chl|access denied|attention required", re.I)


class Stop(RuntimeError):
    pass


def get(path: str) -> requests.Response:
    if BUDGET[0] <= 0:
        raise Stop("request budget used")
    if path.startswith(("/gfx/", "/pdf/")):
        raise Stop(f"refusing a disallowed path {path}")
    wait = 1.1 - (time.monotonic() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    BUDGET[0] -= 1
    try:
        r = SESSION.get(BASE + path, timeout=(15, 60), allow_redirects=True)
    finally:
        LAST[0] = time.monotonic()
    head = r.content[:4000].decode("utf-8", "replace")
    if r.status_code in (401, 403, 429) or BOT.search(head):
        print(f"REFUSED {r.status_code} {path} {head[:300]!r}", flush=True)
        raise Stop(f"refused: {r.status_code} {path}")
    print(f"GET {r.status_code} {len(r.content)} {path} final={r.url} ct={r.headers.get('Content-Type')} "
          f"lm={r.headers.get('Last-Modified')} etag={r.headers.get('ETag')}", flush=True)
    return r


def dump(name: str, body: bytes) -> None:
    print(f"PAGE {name} {base64.b64encode(gzip.compress(body, 9)).decode()}", flush=True)


def main() -> int:
    try:
        robots = get("/robots.txt")
        print("ROBOTS-TEXT-BEGIN\n" + robots.text + "\nROBOTS-TEXT-END", flush=True)
        lines = [" ".join(x.split()).lower() for x in robots.text.splitlines()]
        lines = [x for x in lines if x and not x.startswith("#")]
        if lines != EXPECTED_ROBOTS:
            print(f"ROBOTS CHANGED: {lines}", flush=True)
            return 1
        print("ROBOTS unchanged against the register quote", flush=True)

        reuse = get("/ponowne-wykorzystywanie-informacji-sektora-publicznego/")
        dump("reuse", reuse.content)

        sittings = get("/prace/posiedzenia/")
        dump("sittings", sittings.content)
        hrefs = sorted(set(re.findall(r'href="([^"]+)"', sittings.text)))
        posie = [h for h in hrefs if "posiedzen" in h or "przebieg" in h]
        print(f"SITTINGS links={len(hrefs)} posiedzenia-links={len(posie)}", flush=True)
        for h in posie[:400]:
            print(f"  LINK {h}", flush=True)
        days = sorted(
            {(int(s), int(d)) for s, d in re.findall(r"przebieg,(\d+),(\d+)", sittings.text)}
        )
        print(f"DAYS on the list page: {len(days)} sittings={len({s for s, _ in days})} {days[:5]} … {days[-5:]}",
              flush=True)

        senators = get("/sklad/senatorowie/")
        dump("senators", senators.content)
        sen = sorted(set(re.findall(r"senator,(\d+),(\d+),", senators.text)))
        print(f"SENATORS links={len(sen)} terms={sorted({t for _, t in sen})}", flush=True)

        votes_total, first_print = 0, None
        for n, (s, d) in enumerate(days):
            page = get(f"/prace/posiedzenia/przebieg,{s},{d},glosowania.html")
            prints = sorted(set(re.findall(r"glosowanie-drukuj,(\d+)", page.text)))
            other = sorted(set(re.findall(r"glosowanie[a-z-]*,(\d+)", page.text)))
            votes_total += len(prints)
            print(f"DAY {s},{d} status={page.status_code} bytes={len(page.content)} print-links={len(prints)} "
                  f"vote-links={len(other)}", flush=True)
            if n < 2 or n == len(days) - 1 or (prints and first_print is None):
                dump(f"day_{s}_{d}", page.content)
            if prints and first_print is None:
                first_print = prints[0]
        print(f"VOTES (print links) total={votes_total} over {len(days)} days", flush=True)
        if first_print:
            dump(f"print_{first_print}", get(f"/prace/posiedzenia/glosowanie-drukuj,{first_print}.html").content)
    except Stop as exc:
        print(f"STOP {exc}", flush=True)
        return 1
    finally:
        print(f"requests left {BUDGET[0]}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
