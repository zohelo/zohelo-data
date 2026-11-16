"""Read-only probe 3 of RAD-on (PLS-023, #2990): why the courses crawl misses records, and the closed lists.

1. Crawl courses with the server's own cursor (size 100, as Dev run 43343139469): per page, the decoded cursor
   against the page's newest lastRefresh and the records tied with it.
2. Crawl again with extract.Client.resource (overlap cursor) and compare: which records the first crawl missed and
   whether their lastRefresh equals a page-boundary cursor.
3. Profiles of the courses' coded fields (closed lists for accepted_values).
One request at a time, 2 s apart (extract.Client), bounded retries. Writes nothing to R2. Deleted before merge.
"""

from __future__ import annotations

from collections import Counter
import json
import time

from ingestion.common.report import annotate
from ingestion.to_landing.radon import extract


def server_crawl(api: extract.Client, entity: str) -> tuple[dict, list, int]:
    seen, boundaries, token, max_count = {}, [], None, None
    for _ in range(400):
        body = api.page(entity, 100, token)
        res, pag = body["results"], body.get("pagination") or {}
        max_count = pag.get("maxCount")
        for r in res:
            seen[r["courseUuid"]] = r.get("lastRefresh")
        if not res or not pag.get("token") or pag["token"] == token:
            break
        token = pag["token"]
        t = extract.cursor_time(token)
        newest = max(int(r["lastRefresh"]) for r in res)
        boundaries.append({"t": t, "newest": newest, "ties_on_page": sum(1 for r in res if int(r["lastRefresh"]) == t),
                           "ordered": all(int(a["lastRefresh"]) <= int(b["lastRefresh"]) for a, b in zip(res, res[1:]))})
    return seen, boundaries, max_count


def prof(rows, *keys, n=40):
    c = Counter(" : ".join(str(r.get(k)) for k in keys) for r in rows)
    return {"distinct": len(c), "top": c.most_common(n)}


def main() -> int:
    api = extract.Client()
    t0 = time.monotonic()
    seen, bounds, max_count = server_crawl(api, "courses")
    crawl1 = {"distinct": len(seen), "maxCount": max_count, "pages": len(bounds) + 1,
              "cursor_is_newest": sum(1 for b in bounds if b["t"] == b["newest"]),
              "pages_ordered": sum(1 for b in bounds if b["ordered"]),
              "boundaries_with_ties": sum(1 for b in bounds if b["ties_on_page"] > 1),
              "raw_lastRefresh_type": type(next(iter(seen.values()))).__name__,
              "seconds": round(time.monotonic() - t0)}
    print("CRAWL1", json.dumps(crawl1), flush=True)
    t1 = time.monotonic()
    records, facts = None, None
    try:
        records, facts = api.resource("courses", time.monotonic() + 3600)
    except extract.Stop as exc:
        facts = {"stop": str(exc)}
    print("CRAWL2", json.dumps(facts), flush=True)
    out = {"crawl1": crawl1, "crawl2": facts, "seconds2": round(time.monotonic() - t1)}
    if records:
        missing = [k for k in records if k not in seen]
        bset = {b["t"] for b in bounds}
        mt = Counter(int(records[k]["lastRefresh"]) for k in missing)
        out["missed_by_crawl1"] = len(missing)
        out["missed_at_boundary_ms"] = sum(v for t, v in mt.items() if t in bset)
        out["missed_ms_groups"] = len(mt)
        out["only_in_crawl1"] = sum(1 for k in seen if k not in records)
        rows = list(records.values())
        inst = [i for r in rows for i in r.get("courseInstances") or []]
        disc = [d for r in rows for d in r.get("disciplines") or []]
        p = {
            "status": prof(rows, "currentStatusCode", "currentStatusName"),
            "level": prof(rows, "levelCode", "levelName"),
            "profile": prof(rows, "profileCode", "profileName"),
            "main_kind": prof(rows, "mainInstitutionKindCode", "mainInstitutionKind"),
            "voiv": prof(rows, "leadingInstitutionVoivodeshipCode", "leadingInstitutionVoivodeship"),
            "flags": prof(rows, "leadingInstitutionIsForeign", "coLed", "teacherTraining", "philological"),
            "legal": prof(rows, "legalBasisTypeCode"),
            "data_source": prof(rows, "dataSource"),
            "null_main_inst": sum(1 for r in rows if not r.get("mainInstitutionUuid")),
            "null_level": sum(1 for r in rows if not r.get("levelCode")),
            "null_status": sum(1 for r in rows if not r.get("currentStatusCode")),
            "creation_years": sorted(Counter(str(r.get("creationDate"))[:4] for r in rows).items()),
            "inst_status": prof(inst, "statusCode", "statusName"),
            "inst_form": prof(inst, "formCode", "formName"),
            "disc_leading": prof(disc, "disciplineLeading"),
            "n_instances": len(inst),
        }
        out["profile"] = p
    print("PROBE", json.dumps(out, ensure_ascii=False, default=str), flush=True)
    annotate("notice", "radon probe paging", {k: v for k, v in out.items() if k != "profile"})
    for k, v in (out.get("profile") or {}).items():
        print("PROFILE", k, json.dumps(v, ensure_ascii=False, default=str), flush=True)
    annotate("notice", "radon probe profile", {k: (v["top"] if isinstance(v, dict) else v)
                                               for k, v in (out.get("profile") or {}).items()
                                               if k not in ("voiv", "creation_years")})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
