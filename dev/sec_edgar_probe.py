"""Read-only probe for SEC EDGAR (CD-012): the access rules as published, robots.txt, and the candidate routes.

One request at a time, 3 s apart, GET and HEAD only, nothing stored. The user agent is the repository's
identified convention; a refusal is recorded and not retried another way.
"""
import html
import json
import re
import time
import urllib.robotparser

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

PAUSE = 3
PAGES = [
    "https://www.sec.gov/about/developer-resources",
    "https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data",
    "https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
    "https://www.sec.gov/privacy",
]
WORDS = r"requests per second|request rate|user.agent|declare|fair access|Website Dissemination|public information|bulk data|nightly|submissions\.zip|companyfacts\.zip|scripted|automated"
HEADS = [
    "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
    "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
    "https://www.sec.gov/files/company_tickers.json",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&State=R9&type=&dateb=&owner=exclude&count=40&output=atom",
]


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def get(url, method="GET", stream=False):
    time.sleep(PAUSE)
    return requests.request(method, url, headers={"User-Agent": USER_AGENT}, timeout=60, stream=stream, allow_redirects=True)


def main():
    out = {"user_agent": USER_AGENT}
    for host in ("https://www.sec.gov", "https://data.sec.gov"):
        try:
            r = get(host + "/robots.txt")
            rp = urllib.robotparser.RobotFileParser()
            rp.parse(r.text.splitlines())
            out[host + "/robots.txt"] = {
                "status": r.status_code, "chars": len(r.text),
                "allowed": {u: rp.can_fetch("zohelo-data", u) for u in HEADS + ["https://data.sec.gov/submissions/CIK0000320193.json", "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"] if u.startswith(host)},
                "text": r.text[:1500] if r.status_code == 200 and "<html" not in r.text[:300].lower() else r.text[:200],
            }
        except requests.RequestException as exc:
            out[host + "/robots.txt"] = type(exc).__name__
    annotate("notice", "sec_edgar probe robots", out)

    for url in PAGES:
        res = {"url": url}
        try:
            r = get(url)
            text = plain(r.text)
            res.update({"status": r.status_code, "final": r.url, "chars": len(text)})
            hits, last = [], -9999
            for m in re.finditer(WORDS, text, re.I):
                if m.start() - last < 700:
                    continue
                last = m.start()
                hits.append(text[max(0, m.start() - 250): m.start() + 650])
            res["hits"] = hits[:8] or text[:600]
        except requests.RequestException as exc:
            res["error"] = f"{type(exc).__name__}: {exc}"[:300]
        print(json.dumps(res, ensure_ascii=False), flush=True)
        # two annotations per page so the sentences are not cut
        hits = res.pop("hits", [])
        annotate("notice", f"sec_edgar probe page {url.rsplit('/', 1)[-1]} a", {**res, "hits": hits[:4]})
        if len(hits) > 4:
            annotate("notice", f"sec_edgar probe page {url.rsplit('/', 1)[-1]} b", {"hits": hits[4:8]})

    routes = []
    for url in HEADS:
        try:
            r = get(url, "HEAD")
            h = r.headers
            routes.append({"url": url, "status": r.status_code, "length": h.get("Content-Length"), "last_modified": h.get("Last-Modified"), "type": h.get("Content-Type")})
        except requests.RequestException as exc:
            routes.append({"url": url, "error": type(exc).__name__})
    try:  # field names only, of one large operating company (no values kept)
        r = get("https://data.sec.gov/submissions/CIK0000320193.json")
        doc = r.json() if r.status_code == 200 else {}
        routes.append({
            "url": r.url, "status": r.status_code, "bytes": len(r.content),
            "keys": sorted(doc), "address_keys": sorted((doc.get("addresses") or {}).get("business") or {}),
            "recent_keys": sorted((doc.get("filings") or {}).get("recent") or {}),
            "files_keys": sorted(((doc.get("filings") or {}).get("files") or [{}])[0]),
            "former_names_keys": sorted((doc.get("formerNames") or [{}])[0]),
            "entityType": doc.get("entityType"), "category": doc.get("category"), "lei": doc.get("lei"),
            "recent_rows": len(((doc.get("filings") or {}).get("recent") or {}).get("accessionNumber") or []),
        })
    except (requests.RequestException, ValueError) as exc:
        routes.append({"submissions": type(exc).__name__})
    print(json.dumps(routes), flush=True)
    annotate("notice", "sec_edgar probe routes", routes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
