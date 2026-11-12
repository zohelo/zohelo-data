"""Dev probe of the Crossref REST API (CD-029, #553): response shapes, sizes, facets, filters and rate-limit headers
for the Polish selection. Read-only (no R2 writes); one request at a time with a 2 s pause. Deleted before merge."""

from __future__ import annotations

import json
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.crossref.org"
PROBES = [
    ("aff_pl_years", "/works", {"query.affiliation": "Poland Polska", "rows": 0, "facet": "published:*"}),
    ("aff_poland", "/works", {"query.affiliation": "Poland", "rows": 0}),
    ("aff_polska", "/works", {"query.affiliation": "Polska", "rows": 0}),
    ("aff_pl_2023_facets", "/works", {"query.affiliation": "Poland Polska", "rows": 0,
     "filter": "from-pub-date:2023,until-pub-date:2023",
     "facet": "type-name:*,publisher-name:*,funder-name:*,license:*"}),
    ("aff_pl_2023_type_ja", "/works", {"query.affiliation": "Poland Polska", "rows": 0,
     "filter": "from-pub-date:2023,until-pub-date:2023,type:journal-article"}),
    ("bad_facet", "/works", {"rows": 0, "facet": "nosuchfacet:*"}),
    ("bad_filter", "/works", {"rows": 0, "filter": "nosuchfilter:1"}),
    ("funder_doi_facet", "/works", {"query.affiliation": "Poland Polska", "rows": 0,
     "filter": "from-pub-date:2023,until-pub-date:2023", "facet": "funder-doi:*"}),
    ("ror_facet", "/works", {"query.affiliation": "Poland Polska", "rows": 0,
     "filter": "from-pub-date:2023,until-pub-date:2023", "facet": "ror-id:20"}),
    ("aff_pl_sample", "/works", {"query.affiliation": "Poland Polska", "rows": 3,
     "filter": "from-pub-date:2023,until-pub-date:2023", "select": "DOI,type,member,publisher,author,published"}),
    ("funders_pl", "/funders", {"filter": "location:Poland", "rows": 1000}),
    ("funders_bad_filter", "/funders", {"filter": "nosuchfilter:1", "rows": 0}),
    ("funder_ncn", "/funders/501100004281", {}),
    ("funder_ncn_works", "/works", {"filter": "funder:501100004281", "rows": 0, "facet": "published:*"}),
    ("members_total", "/members", {"rows": 0}),
    ("members_bad_filter", "/members", {"filter": "nosuchfilter:1", "rows": 0}),
    ("members_query_poland", "/members", {"query": "Poland", "rows": 3}),
    ("members_cursor", "/members", {"rows": 2, "cursor": "*"}),
    ("works_cursor_rows0", "/works", {"query.affiliation": "Poland Polska", "rows": 2, "cursor": "*",
     "filter": "from-pub-date:2023-01-01,until-pub-date:2023-01-01", "select": "DOI,type,published,member"}),
]


def main() -> int:
    out = {}
    for n, (name, path, params) in enumerate(PROBES):
        if n:
            time.sleep(2)
        t0 = time.monotonic()
        try:
            r = requests.get(API + path, params=params, headers={"User-Agent": UA}, timeout=(30, 120))
        except requests.RequestException as exc:
            out[name] = {"error": type(exc).__name__}
            annotate("warning", f"probe {name}", out[name])
            continue
        secs = round(time.monotonic() - t0, 1)
        info = {"http": r.status_code, "bytes": len(r.content), "secs": secs,
                "rl": {k: v for k, v in r.headers.items() if k.lower().startswith(("x-rate", "x-api", "retry"))}}
        try:
            body = r.json()
        except ValueError:
            info["text"] = r.text[:600]
            out[name] = info
            annotate("notice", f"probe {name}", info)
            continue
        msg = body.get("message")
        info["status"] = body.get("status")
        info["message_type"] = body.get("message-type")
        info["message_version"] = body.get("message-version")
        if isinstance(msg, dict):
            info["keys"] = sorted(msg)[:30]
            info["total"] = msg.get("total-results")
            facets = msg.get("facets") or {}
            info["facets"] = {
                f: {"value_count": v.get("value-count"), "n": len(v.get("values") or {}),
                    "head": dict(list((v.get("values") or {}).items())[:12])}
                for f, v in facets.items()
            }
            items = msg.get("items") or []
            if items:
                info["item0_keys"] = sorted(items[0])[:40]
                info["item0"] = json.dumps(items[0], ensure_ascii=False)[:900]
            if "next-cursor" in msg:
                info["next_cursor_len"] = len(msg["next-cursor"] or "")
            if name.startswith("funder_ncn"):
                info["msg"] = json.dumps({k: v for k, v in msg.items() if k != "descendant-work-count"},
                                         ensure_ascii=False)[:1200]
        elif isinstance(msg, list):
            info["msg"] = json.dumps(msg, ensure_ascii=False)[:1500]
        if name == "funders_pl":
            items = msg.get("items") or []
            info["locations"] = sorted({i.get("location") for i in items})
            info["names_sample"] = [(i.get("id"), i.get("name")) for i in items[:15]]
            info["item_key_union"] = sorted({k for i in items for k in i})
        if name == "aff_pl_sample":
            # affiliation strings only (no names), to see what matched
            affs = []
            for it in msg.get("items") or []:
                for a in it.get("author") or []:
                    affs += [x.get("name", "")[:100] for x in a.get("affiliation") or []]
            info.pop("item0", None)
            info["affiliations"] = affs[:12]
        out[name] = info
        print(name, json.dumps(info, ensure_ascii=False)[:6000], flush=True)
        annotate("notice", f"probe {name}", info)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
