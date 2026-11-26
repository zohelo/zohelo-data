"""Read-only Dev probe of OpenAlex (register CD-028, #2996): answer shapes, sizes, counts and cost headers of the
group_by and institution requests the build would use, and the terms pages. No writes. Deleted before merge."""

from __future__ import annotations

import json
import re
import sys
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.openalex.org"
OUT: dict = {}


def get(name: str, url: str, params: dict | None = None) -> dict:
    time.sleep(1.5)
    try:
        r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=(30, 90))
    except requests.RequestException as exc:
        OUT[name] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
        return {}
    heads = {k: v for k, v in r.headers.items() if re.search(r"rate|limit|cost|budget|usage|credit|retry|x-", k, re.I)}
    info = {"status": r.status_code, "bytes": len(r.content), "headers": heads, "url": r.url[:300]}
    body = {}
    try:
        body = r.json()
        meta = body.get("meta") if isinstance(body, dict) else None
        info["meta"] = meta
        info["keys"] = sorted(body) if isinstance(body, dict) else type(body).__name__
        if isinstance(body, dict) and body.get("group_by") is not None:
            g = body["group_by"]
            info["groups"] = len(g)
            info["groups_sum"] = sum(int(x.get("count") or 0) for x in g)
            info["group_sample"] = g[:3] + g[-2:]
            info["group_keys"] = sorted({k for x in g for k in x})
        if isinstance(body, dict) and body.get("results"):
            res = body["results"]
            info["results"] = len(res)
            info["result_keys"] = sorted(res[0])
        if isinstance(body, dict) and "error" in body:
            info["error_body"] = str(body)[:400]
    except ValueError:
        info["text"] = r.text[:300]
    print(f"=== {name}: {json.dumps(info, ensure_ascii=False)[:5000]}", flush=True)
    OUT[name] = info
    return body


def page(name: str, url: str, needles: list[str]) -> None:
    time.sleep(1.5)
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 60))
    except requests.RequestException as exc:
        OUT[name] = {"error": f"{type(exc).__name__}"}
        return
    text = re.sub(r"<[^>]+>", " ", r.text)
    text = re.sub(r"\s+", " ", text)
    hits = []
    for n in needles:
        for m in re.finditer(re.escape(n), text, re.I):
            hits.append(text[max(0, m.start() - 220): m.end() + 260])
            if len(hits) > 14:
                break
    upd = re.findall(r"(?:last updated|updated)[^.]{0,60}", text, re.I)[:3]
    OUT[name] = {"status": r.status_code, "bytes": len(r.content), "updated": upd, "hits": hits[:8]}
    print(f"=== {name}: status {r.status_code}, {len(r.content)} bytes")
    for h in hits:
        print("   >>", h)
    print("   updated:", upd, flush=True)


def main(argv: list[str]) -> int:
    del argv
    page("pricing", "https://help.openalex.org/access/pricing/",
         ["CC0", "per day", "free", "group", "list", "mailto", "polite", "rate limit", "per second", "key"])
    page("grouping", "https://help.openalex.org/api/grouping.md",
         ["200", "cursor", "unknown", "group_by", "page"])
    page("limits", "https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication",
         ["per second", "per day", "mailto", "polite", "key", "cost"])
    pl = "institutions.country_code:PL"
    get("works_pl_by_year_inst", f"{API}/works", {"filter": pl, "group_by": "publication_year"})
    get("works_pl_by_year_countries", f"{API}/works", {"filter": "authorships.countries:PL", "group_by": "publication_year"})
    get("works_pl_oa_by_year", f"{API}/works", {"filter": f"{pl},open_access.is_oa:true", "group_by": "publication_year"})
    get("works_pl_by_field", f"{API}/works", {"filter": pl, "group_by": "primary_topic.field.id"})
    get("works_pl_2024_by_field", f"{API}/works", {"filter": f"{pl},publication_year:2024", "group_by": "primary_topic.field.id"})
    get("works_pl_field17_by_year", f"{API}/works", {"filter": f"{pl},primary_topic.field.id:17", "group_by": "publication_year"})
    get("fields", f"{API}/fields", {"per_page": 200, "select": "id,display_name,domain"})
    get("institutions_pl_type", f"{API}/institutions", {"filter": "country_code:PL", "group_by": "type"})
    get("institutions_pl_region", f"{API}/institutions", {"filter": "country_code:PL", "group_by": "region"})
    body = get("institutions_pl_page1", f"{API}/institutions",
               {"filter": "country_code:PL", "per_page": 200, "cursor": "*"})
    if body.get("results"):
        r0 = body["results"][0]
        sample = {k: r0.get(k) for k in ("id", "ror", "display_name", "type", "country_code", "geo", "works_count",
                                          "cited_by_count", "updated_date", "lineage", "ids", "counts_by_year")}
        print("=== institution sample:", json.dumps(sample, ensure_ascii=False)[:3000], flush=True)
        OUT["institution_sample"] = {k: (str(v)[:300]) for k, v in sample.items()}
        OUT["institution_regions"] = sorted({str((r.get("geo") or {}).get("region")) for r in body["results"]})[:40]
        cby = [len(r.get("counts_by_year") or []) for r in body["results"]]
        OUT["counts_by_year_lengths"] = [min(cby), max(cby)]
        years = sorted({c["year"] for r in body["results"] for c in (r.get("counts_by_year") or [])})
        OUT["counts_by_year_years"] = [years[0], years[-1]] if years else []
    get("works_pl_by_institution", f"{API}/works", {"filter": pl, "group_by": "authorships.institutions.lineage"})
    get("works_pl_mailto", f"{API}/works", {"filter": pl, "group_by": "publication_year", "mailto": "noreply@example.org"})
    get("works_pl_by_year_bad", f"{API}/works", {"filter": "institutions.country_code:PL,publication_year:2300",
                                                  "group_by": "publication_year"})
    compact = {}
    for k, v in OUT.items():
        if isinstance(v, dict):
            compact[k] = {kk: vv for kk, vv in v.items() if kk in ("status", "bytes", "meta", "groups", "groups_sum",
                                                                  "results", "headers", "error", "error_body", "updated")}
        else:
            compact[k] = v
    text = json.dumps(compact, ensure_ascii=False)
    for i in range(0, min(len(text), 3800 * 6), 3800):
        annotate("notice", f"openalex probe {i // 3800 + 1}", {"part": text[i:i + 3800]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
