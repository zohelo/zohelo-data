"""Dev probe for #2464 and #2468 (not for main, writes nothing): the download links of the ZUS yearbook and KRUS pages."""

from __future__ import annotations

from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
PAGES = {
    "zus": "https://www.zus.pl/baza-wiedzy/statystyka/rocznik-statystyczny-ubezpieczen-spolecznych",
    "krus": "https://www.gov.pl/web/krus/podstawowe-dane-statystyczne-z-zakresu-swiadczeniobiorcow",
}
KEEP = (".xls", ".xlsx", ".pdf", ".zip", "/documents/", "/attachment/", "rocznik", "kwartaln", "statysty")


class Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[list[str]] = []
        self.href: str | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.href = dict(attrs).get("href")
            self.links.append([self.href or "", ""])

    def handle_data(self, data):
        if self.href is not None and self.links and data.strip():
            self.links[-1][1] = (self.links[-1][1] + " " + data.strip())[:80]

    def handle_endtag(self, tag):
        if tag == "a":
            self.href = None


def main() -> int:
    """Annotate the file-like links of each page (URL tail and text), in page order."""
    for name, url in PAGES.items():
        r = requests.get(url, headers=UA, timeout=60)
        p = Links()
        p.feed(r.text)
        files = [[urljoin(url, h), t] for h, t in p.links if any(k in h.lower() for k in KEEP)]
        annotate("notice", f"{name} page", {"status": r.status_code, "bytes": len(r.content), "links": len(p.links),
                                            "files": len(files)})
        for i in range(0, min(len(files), 160), 40):
            annotate("notice", f"{name} files {i // 40 + 1}", [[u[-110:], t] for u, t in files[i : i + 40]])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
