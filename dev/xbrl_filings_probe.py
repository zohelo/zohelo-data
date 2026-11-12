"""Read-only probe (CD-013, #1500): filings.xbrl.org API shape, Polish filing count, versions, json_url sizes.

GET and HEAD only, one request at a time with a 2 s pause, the repository's identified User-Agent. No writes.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import json
import re
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

BASE = "https://filings.xbrl.org"
H = {"User-Agent": USER_AGENT}


def get(url, **kw):
    time.sleep(2)
    started = time.monotonic()
    r = requests.get(url, headers=H, timeout=(30, 300), **kw)
    return r, round(time.monotonic() - started, 1)


def main() -> int:  # noqa: PLR0915
    out = {}
    r, s = get(f"{BASE}/robots.txt")
    out["robots"] = [r.status_code, r.text[:200] if r.status_code == 200 else ""]
    r, s = get(f"{BASE}/api/filings?filter[country]=PL&page[size]=2&include=entity")
    j = r.json()
    out["small"] = {"status": r.status_code, "ctype": r.headers.get("Content-Type"), "top": list(j), "meta": j.get("meta"),
                    "links": j.get("links"), "first": j["data"][0], "included": j.get("included", [])[:1],
                    "headers": {k: v for k, v in r.headers.items() if k.lower() in ("last-modified", "etag", "content-length", "cache-control", "server", "x-ratelimit-limit", "retry-after")}}
    annotate("notice", "probe api shape", out)
    print(json.dumps(out)[:6000])
    sorts = {}
    for sort in ("fxo_id", "date_added", "date_added,fxo_id", "-date_added", "processed"):
        r, s = get(f"{BASE}/api/filings?filter[country]=PL&page[size]=3&sort={sort}")
        try:
            sorts[sort] = [r.status_code, [(d["attributes"].get("fxo_id"), d["attributes"].get("date_added")) for d in r.json()["data"]]]
        except Exception as exc:  # noqa: BLE001
            sorts[sort] = [r.status_code, str(exc)[:100], r.text[:200]]
    annotate("notice", "probe sorts", sorts)
    rows, pages, included = [], [], {}
    number = 1
    while True:
        r, s = get(f"{BASE}/api/filings?filter[country]=PL&page[size]=1000&page[number]={number}&sort=date_added&include=entity")
        j = r.json()
        pages.append({"page": number, "status": r.status_code, "bytes": len(r.content), "seconds": s, "rows": len(j["data"]), "meta": j.get("meta"), "links": j.get("links")})
        rows += j["data"]
        for inc in j.get("included", []):
            included[(inc["type"], inc["id"])] = inc
        if len(j["data"]) < 1000 or number >= 5:
            break
        number += 1
    a = [d["attributes"] | {"id": d["id"], "rel": {k: (v.get("data") or v.get("links")) for k, v in d.get("relationships", {}).items()}} for d in rows]
    seg = Counter(x["fxo_id"].rsplit("-", 1)[-1] for x in a)
    shape = Counter(re.sub(r"[A-Z0-9]{20}", "LEI", re.sub(r"\d", "9", x["fxo_id"])) for x in a)
    groups = defaultdict(list)
    for x in a:
        groups[(x["fxo_id"][:20], x["period_end"])].append(x)
    multi = {k: v for k, v in groups.items() if len(v) > 1}
    ent_attr = Counter(k for i in included.values() for k in i.get("attributes", {}))
    annotate("notice", "probe PL filing list", {
        "pages": pages, "rows": len(a), "distinct_fxo": len({x["fxo_id"] for x in a}), "distinct_id": len({x["id"] for x in a}),
        "attr_keys": Counter(k for x in a for k in x).most_common(40),
        "null_counts": {k: sum(1 for x in a if x.get(k) in (None, "")) for k in a[0]},
        "country": Counter(x.get("country") for x in a), "period_end_year": sorted(Counter(str(x["period_end"])[:4] for x in a).items()),
        "period_end_md": Counter(str(x["period_end"])[4:] for x in a).most_common(8),
        "date_added_range": [min(x["date_added"] for x in a), max(x["date_added"] for x in a)],
        "processed_range": [min(str(x["processed"]) for x in a), max(str(x["processed"]) for x in a)],
        "last_segment": sorted(seg.items()), "fxo_shape": shape.most_common(5),
        "lei_period_groups": len(groups), "groups_multi": len(multi), "multi_sizes": Counter(len(v) for v in multi.values()),
        "distinct_lei": len({x["fxo_id"][:20] for x in a}), "included_types": Counter(t for t, _ in included), "entity_attr": ent_attr,
        "error_count_dist": Counter(min(int(x.get("error_count") or 0), 5) for x in a), "warning_gt0": sum(1 for x in a if (x.get("warning_count") or 0) > 0),
    })
    sample = []
    for k, v in list(multi.items())[:60]:
        sample.append([k[0][:6], k[1], [[x["fxo_id"].rsplit("-", 2)[-2:], x["date_added"][:10], (x.get("package_url") or "").rsplit("/", 1)[-1][-40:], x["sha256"][:6], x.get("error_count")] for x in sorted(v, key=lambda x: x["fxo_id"])]])
    annotate("notice", "probe multi filings 1", sample[:14])
    annotate("notice", "probe multi filings 2", sample[14:28])
    same_sha = sum(1 for v in multi.values() if len({x["sha256"] for x in v}) < len(v))
    lang = Counter(tuple(sorted(re.findall(r"[-_]([a-z]{2})\.(?:zip|xbri)$", (x.get("package_url") or "").lower())[0] if re.findall(r"[-_]([a-z]{2})\.(?:zip|xbri)$", (x.get("package_url") or "").lower()) else "?" for x in v)) for v in multi.values())
    print(json.dumps({"multi_all": [[k[0], k[1], [[x["fxo_id"], x["date_added"], x.get("package_url"), x["sha256"][:8]] for x in v]] for k, v in multi.items()]})[:60000])
    annotate("notice", "probe multi summary", {"groups_with_same_sha": same_sha, "lang_suffix_sets": lang.most_common(12),
                                                "json_url_null": sum(1 for x in a if not x.get("json_url")), "first3": a[:1], "entity_sample": list(included.values())[:2]})
    # well-known issuers by LEI prefix in the entity names
    names = {i["id"]: i["attributes"] for (t, _), i in included.items()}
    want = ["ORLEN", "POWSZECHNA KASA", "KGHM", "CD PROJEKT", "POWSZECHNY ZAK", "ALLEGRO", "DINO", "LPP"]
    hits = {}
    for x in a:
        ent = (x["rel"].get("entity") or {})
        eid = ent.get("id") if isinstance(ent, dict) else None
        nm = (names.get(eid, {}).get("name") or "").upper()
        for w in want:
            if w in nm:
                hits.setdefault(w, []).append(x)
    annotate("notice", "probe known issuers", {w: [[x["fxo_id"], x["date_added"][:10]] for x in v][:8] for w, v in hits.items()})
    # json_url sizes: a few known issuers (newest) and a spread of others
    picks = [v[-1] for v in hits.values()][:6] + a[:2] + a[len(a) // 2: len(a) // 2 + 2] + a[-2:]
    sizes = []
    for x in picks:
        if not x.get("json_url"):
            sizes.append([x["fxo_id"], "no json_url"])
            continue
        url = BASE + x["json_url"] if x["json_url"].startswith("/") else x["json_url"]
        r, s = get(url)
        row = {"fxo": x["fxo_id"], "status": r.status_code, "bytes": len(r.content), "enc": r.headers.get("Content-Encoding"), "seconds": s}
        try:
            j = r.json()
            facts = j.get("facts", {})
            row["top"] = list(j)
            row["facts"] = len(facts)
            sel = {}
            for fid, f in facts.items():
                d = f.get("dimensions", {})
                c = d.get("concept")
                if c in ("ifrs-full:Revenue", "ifrs-full:ProfitLoss", "ifrs-full:Assets", "ifrs-full:Equity"):
                    extra = sorted(k for k in d if k not in ("concept", "entity", "period", "unit", "language"))
                    sel.setdefault(c, []).append([f.get("value"), f.get("decimals"), d.get("period"), d.get("unit"), d.get("entity"), extra])
            row["sel"] = {c: [len(v), sum(1 for z in v if not z[5]), [z for z in v if not z[5]][:3]] for c, v in sel.items()}
            row["fact_keys"] = Counter(k for f in facts.values() for k in f).most_common(6)
            row["dim_keys"] = Counter(k for f in facts.values() for k in f.get("dimensions", {})).most_common(12)
            row["doc"] = {k: (v if k != "namespaces" else len(v)) for k, v in j.get("documentInfo", {}).items()}
        except Exception as exc:  # noqa: BLE001
            row["error"] = str(exc)[:200]
        sizes.append(row)
        print(json.dumps(row)[:3000])
    for i in range(0, len(sizes), 3):
        if i < 12:
            annotate("notice", f"probe json sizes {i // 3 + 1}", sizes[i:i + 3])
    r, s = get(f"{BASE}/docs/about")
    text = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", r.text))
    m = [text[max(0, x.start() - 300): x.start() + 400] for x in re.finditer(r"Terms of use|All Rights Reserved", text)]
    r2, s = get(f"{BASE}/docs/api")
    t2 = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", r2.text))
    m2 = [t2[max(0, x.start() - 300): x.start() + 500] for x in re.finditer(r"user agent|rate limit", t2)]
    idx = {}
    for year in range(2020, 2027):
        time.sleep(2)
        h = requests.head(f"{BASE}/indexes/filings-{year}.json", headers=H, timeout=60)
        idx[year] = [h.status_code, h.headers.get("Content-Length"), h.headers.get("Last-Modified")]
    print(json.dumps({"about": m, "api": m2, "idx": idx}))
    annotate("notice", "probe terms and indexes", {"about_status": r.status_code, "about": m[:3], "api_doc": m2[:3], "indexes": idx})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
