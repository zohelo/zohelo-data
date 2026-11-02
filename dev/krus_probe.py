"""Dev probe for #2468 (not for main, writes nothing): KRUS statistics pages on gov.pl and their attachments."""

from __future__ import annotations

from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
START = "https://www.gov.pl/web/krus/podstawowe-dane-statystyczne-z-zakresu-swiadczeniobiorcow"


class Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[list[str]] = []
        self.inside = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.links.append([dict(attrs).get("href") or "", ""])
            self.inside = True

    def handle_data(self, data):
        if self.inside and data.strip():
            self.links[-1][1] = (self.links[-1][1] + " " + data.strip())[:70]

    def handle_endtag(self, tag):
        if tag == "a":
            self.inside = False


def links(url: str) -> list[list[str]]:
    p = Links()
    p.feed(requests.get(url, headers=UA, timeout=60).text)
    return [[urljoin(url, h), t] for h, t in p.links]


def main() -> int:
    try:
        first = links(START)
        pages = sorted({u for u, t in first if "/web/krus/" in u and any(k in (u + t).lower() for k in ("statyst", "dane", "informac"))})
        annotate("notice", "krus statistics pages", pages[:60])
        found = 0
        for page in [START] + pages[:25]:
            atts = [[u.rsplit("/", 1)[-1][:12], t] for u, t in links(page) if "/attachment/" in u]
            if atts and found < 8:
                found += 1
                annotate("notice", f"krus {page.rsplit('/', 1)[-1][:60]}", {"n": len(atts), "first": atts[:25]})
    except Exception as exc:  # noqa: BLE001 - probe: report any failure
        annotate("error", "krus probe failed", f"{type(exc).__name__}: {exc}"[:500])
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
