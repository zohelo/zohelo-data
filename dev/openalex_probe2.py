"""Read-only Dev probe 2 of OpenAlex (#2996): can the 1,352 institutions in Poland be read without a key (cursor
paging past page 1 needs one)? Tries page= paging under two sorts and reports unique ids; and group_by type counts.
No writes. Deleted before merge."""

from __future__ import annotations

import json
import sys
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.openalex.org"
SELECT = "id,display_name,type,works_count,counts_by_year,geo,ror,lineage,updated_date,country_code,cited_by_count"


def get(params: dict) -> tuple[int, dict, dict]:
    time.sleep(1.0)
    r = requests.get(f"{API}/institutions", params=params, headers={"User-Agent": UA}, timeout=(30, 90))
    heads = {k: v for k, v in r.headers.items() if k.lower().startswith("x-ratelimit-remaining")}
    try:
        return r.status_code, r.json(), heads
    except ValueError:
        return r.status_code, {"text": r.text[:300]}, heads


def main(argv: list[str]) -> int:
    del argv
    out: dict = {}
    s, b, h = get({"filter": "country_code:PL", "group_by": "type"})
    out["types"] = {g["key"]: g["count"] for g in b.get("group_by", [])}
    for sort in ("works_count:desc", "display_name:asc", "id:asc"):
        ids: list[str] = []
        pages = []
        for page in range(1, 9):
            s, b, h = get({"filter": "country_code:PL", "per_page": 200, "page": page, "sort": sort, "select": SELECT})
            res = b.get("results") or []
            pages.append({"page": page, "status": s, "n": len(res), "count": (b.get("meta") or {}).get("count"),
                          "err": str(b.get("message") or b.get("error") or b.get("text") or "")[:160], **h})
            ids += [r["id"] for r in res]
            if s != 200 or not res:
                break
        out[sort] = {"rows": len(ids), "unique": len(set(ids)), "pages": pages}
        print(f"=== {sort}: {json.dumps(out[sort])}", flush=True)
        if s != 200 and page == 2:
            break
    s, b, h = get({"filter": "country_code:PL", "per_page": 200, "select": SELECT})
    res = b.get("results") or []
    out["page1_bytes_selected"] = len(json.dumps(b))
    out["regions"] = sorted({str((r.get("geo") or {}).get("region")) for r in res})[:40]
    out["region_null_share_page1"] = sum(1 for r in res if not (r.get("geo") or {}).get("region")) / max(1, len(res))
    out["cby_years"] = sorted({c["year"] for r in res for c in r.get("counts_by_year") or []})
    out["cby_keys"] = sorted({k for r in res for c in r.get("counts_by_year") or [] for k in c})
    out["cited_zero_by_year"] = {y: sum(1 for r in res for c in r.get("counts_by_year") or [] if c["year"] == y and c.get("cited_by_count") == 0)
                                 for y in out["cby_years"]}
    out["works_zero"] = sum(1 for r in res for c in r.get("counts_by_year") or [] if c.get("works_count") == 0)
    print("=== summary:", json.dumps(out, ensure_ascii=False), flush=True)
    text = json.dumps(out, ensure_ascii=False)
    for i in range(0, min(len(text), 3800 * 5), 3800):
        annotate("notice", f"openalex probe2 {i // 3800 + 1}", {"part": text[i:i + 3800]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
