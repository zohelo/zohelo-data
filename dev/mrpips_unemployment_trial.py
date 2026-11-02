"""Dev trial for #2349 step (amended on #2348, not for main): annotate the links of every categoryId page linked
from the PSZ registered-unemployment statistics page, to find one that lists the monthly workbooks directly."""

from __future__ import annotations

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

PAGE = "https://psz.praca.gov.pl/web/urzad-pracy/rynek-pracy/statystyki-i-analizy/bezrobocie-rejestrowane"


def get(url: str) -> requests.Response:
    return requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=30)


def main() -> int:
    top = get(PAGE)
    categories = bulk_file.links_matching(top.text, top.url, r"categoryId=")
    annotate("notice", "psz categories", {"count": len(categories), "pages": categories})
    for url in categories[:12]:
        resp = get(url)
        docs = bulk_file.links_matching(resp.text, resp.url, r"/documents/")
        heads = []
        for link in docs[:3]:
            head, http = bulk_file.probe(link)
            h = head.headers if head is not None else {}
            heads.append(
                {"url": link, "http": http, "type": h.get("Content-Type"), "name": h.get("Content-Disposition")}
            )
        annotate(
            "notice",
            f"psz category {url.rsplit('categoryId=', 1)[-1]}",
            {"status": resp.status_code, "documents": len(docs), "first": heads},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
