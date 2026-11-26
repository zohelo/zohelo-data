"""Read-only probe 2 (#2871): the KNF statistics listing's category and paging links (PPK, pensions), and the
PPK article's attachment. Nothing written. Deleted before merge."""

from __future__ import annotations

import html
import re
import sys
import time

import requests

from ingestion.to_landing.patterns import bulk_file

LIST = "https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne"


def get(url):
    time.sleep(4)
    r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 120))
    print("GET", r.status_code, len(r.content), url, flush=True)
    return r


def main(argv):
    r = get(LIST)
    hrefs = sorted(set(html.unescape(h) for h in re.findall(r'href="([^"]+)"', r.text)))
    for h in hrefs:
        if "dane_statystyczne" in h or "p_id" in h or "cur" in h or "kategor" in h.lower() or "delta" in h:
            print("HREF", h, flush=True)
    for m in re.finditer(r"<(form|select|option|input)[^>]*>", r.text):
        print("FORM", m.group(0)[:300], flush=True)
    for m in re.finditer(r"(?s)PPK.{0,400}", r.text):
        print("PPKCTX", " ".join(m.group(0).split())[:400], flush=True)
        break
    at = r.text.find("Rynek emerytalny")
    print("MENU", " ".join(r.text[at - 3000: at + 3000].split())[:6000], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
