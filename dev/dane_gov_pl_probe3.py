"""Third probe for #3263: the Landing client end to end (no R2 writes) and the repeated dane.gov.pl numbers."""

from __future__ import annotations

from collections import Counter, defaultdict
import json
import re
import time

from ingestion.common.report import annotate
from ingestion.to_landing.dane_gov_pl import extract


def number(r):
    m = re.search(r"dane\.gov\.pl/(?:[a-z]{2}/)?dataset/(\d+)", r["identifier"] or "")
    m2 = re.search(r"^https-dane-gov-pl-(?:[a-z]{2}-)?dataset-(\d+)-", r["id"])
    return (m.group(1) if m else None), (m2.group(1) if m2 else None)


def main():
    t0 = time.monotonic()
    api = extract.Client()
    records, facts = extract.catalogue(api, time.monotonic() + 3600)
    body = extract.encode(records)
    info = {
        **facts,
        "requests": api.requests,
        "mb_read": round(api.bytes / 1e6, 1),
        "seconds": round(time.monotonic() - t0),
        "jsonl_mb": round(len(body) / 1e6, 1),
        **extract.newest_modified(records),
    }
    annotate("notice", "probe3 landing client", info)
    print("INFO", json.dumps(info))
    groups = defaultdict(list)
    mismatch = []
    for r in records.values():
        a, b = number(r)
        if a and b and a != b:
            mismatch.append([r["id"], r["identifier"]])
        groups[a or b].append(r)
    dups = {k: v for k, v in groups.items() if len(v) > 1}
    rows = []
    for k, v in sorted(dups.items(), key=lambda kv: int(kv[0] or 0)):
        for r in v:
            rows.append(
                [
                    k,
                    r["id"][:70],
                    (r["identifier"] or "")[:80],
                    r["modified"],
                    r["issued"],
                    r["catalog_record_issued"],
                    r["catalog_record_modified"],
                    r["publisher_name"],
                    len(r["distributions"]),
                    (r["title_pl"] or "")[:50],
                ]
            )
    for row in rows:
        print("DUP", json.dumps(row, ensure_ascii=False))
    kinds = Counter()
    for v in dups.values():
        ids = sorted(r["id"] for r in v)
        same_slug = len({re.sub(r"^https-dane-gov-pl-(?:[a-z]{2}-)?dataset-\d+-?", "", i) for i in ids}) == 1
        langs = {
            re.match(r"^https-dane-gov-pl-([a-z]{2})-", i).group(1)
            if re.match(r"^https-dane-gov-pl-([a-z]{2})-", i)
            else "-"
            for i in ids
        }
        kinds[f"same_slug={same_slug} langs={sorted(langs)}"] += 1
    annotate(
        "notice",
        "probe3 repeated numbers",
        {"groups": len(dups), "kinds": dict(kinds), "mismatch": mismatch[:5], "sample": rows[:8]},
    )
    # field fill
    fill = Counter()
    for r in records.values():
        for k, v in r.items():
            if v not in (None, [], ""):
                fill[k] += 1
    dfill = Counter()
    for r in records.values():
        for d in r["distributions"]:
            for k, v in d.items():
                if v not in (None, ""):
                    dfill[k] += 1
    cats = Counter(
        (c["id"] or "-") + "|" + (c["resource"] or "-").rsplit("/", 1)[-1]
        for r in records.values()
        for c in r["categories"]
    )
    ptype = Counter(r["publisher_type"] for r in records.values())
    pres = Counter(bool(re.search(r"/institution/(\d+)", r["publisher_resource"] or "")) for r in records.values())
    fmt = Counter(
        (d["format"] or "-") + "|" + (d["format_resource"] or "-").rsplit("/", 1)[-1]
        for r in records.values()
        for d in r["distributions"]
    )
    stat = Counter(d["status"] for r in records.values() for d in r["distributions"])
    acc = Counter((d["access_url"] or "")[:40] for r in records.values() for d in r["distributions"])
    res_ok = Counter(
        bool(re.search(r"/dataset/\d+/resource/\d+$", d["access_url"] or ""))
        for r in records.values()
        for d in r["distributions"]
    )
    country = Counter(r["country"] for r in records.values())
    shape = {
        "fill": dict(fill),
        "dist_fill": dict(dfill),
        "categories": dict(cats),
        "publisher_type": dict(ptype),
        "publisher_institution_link": dict(pres),
        "status": dict(stat),
        "country": dict(country),
        "resource_link_ok": dict(res_ok),
    }
    annotate("notice", "probe3 shape", shape)
    print("SHAPE", json.dumps(shape, ensure_ascii=False))
    print("FORMATS", json.dumps(fmt.most_common(40)))
    print("ACCESS", json.dumps(acc.most_common(5)))
    print("SAMPLE", body[:3000].decode())


if __name__ == "__main__":
    main()
