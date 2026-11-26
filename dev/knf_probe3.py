"""Read-only probe 3 (#2871): the KNF listing's category filter and paging (which URLs list older PPK releases).
Nothing written. Deleted before merge."""

from __future__ import annotations

import sys
import time

import requests

from ingestion.to_landing.knf_pensions import articles
from ingestion.to_landing.patterns import bulk_file

B = "https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne"
URLS = [
    B + "?categories=%2Fpublikacje_i_opracowania%2Fdane_statystyczne%2FPPK&apply=1",
    B + "?categories=/publikacje_i_opracowania/dane_statystyczne/PPK",
    B + "?page=2&apply=1",
    B + "?page=3&apply=1",
    B + "?categories=%2Fpublikacje_i_opracowania%2Fdane_statystyczne%2FPPK&page=2&apply=1",
    B + "?searchText=PPK&apply=1",
]


def main(argv):
    import re

    for url in URLS:
        time.sleep(4)
        r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 120))
        found = articles(r.text, r.url)
        ids = re.findall(r"articleId=(\d+)", r.text)
        print("URL", r.status_code, len(r.text), url, "| ppk:", [(a["quarter"], a["url"][-30:]) for a in found],
              "| ids:", sorted(set(ids))[:14], "| next:", sorted(set(re.findall(r'page=\d+[^"]*', r.text)))[:6], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
