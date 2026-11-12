"""Read-only probe for CD-031 (Our World in Data): the three selected charts' metadata.json, their indicator
metadata (origins[].license) and a profile of each CSV. GET only, one request at a time, 2 s pause. Full JSON goes
to the job log; one compact annotation per chart."""

import csv
import io
import json
import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
SLUGS = ["consumption-co2-per-capita", "electoral-democracy-index", "gdp-per-capita-maddison-project-database"]
Q = "?csvType=full&useColumnShortNames=true"


def get(url):
    time.sleep(2)
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    print(f"GET {url} -> {r.status_code} {len(r.content)} bytes final={r.url}", flush=True)
    return r


def main():
    for slug in SLUGS:
        out = {"slug": slug}
        meta = get(f"https://ourworldindata.org/grapher/{slug}.metadata.json")
        out["meta_http"] = meta.status_code
        print(meta.text, flush=True)
        m = meta.json()
        out["chart"] = {k: m["chart"].get(k) for k in ("title", "citation", "originalChartUrl", "selection")}
        out["dateDownloaded"] = m.get("dateDownloaded")
        cols = {}
        for short, col in m["columns"].items():
            c = {k: col.get(k) for k in ("shortName", "unit", "shortUnit", "lastUpdated", "nextUpdate", "timespan",
                                          "type", "owidVariableId", "citationShort", "fullMetadata")}
            url = col.get("fullMetadata") or (
                f"https://api.ourworldindata.org/v1/indicators/{col['owidVariableId']}.metadata.json"
                if col.get("owidVariableId") else None)
            if url:
                ind = get(url)
                c["indicator_http"] = ind.status_code
                if ind.status_code == 200:
                    j = ind.json()
                    print(json.dumps(j, indent=1)[:20000], flush=True)
                    c["origins"] = [
                        {k: o.get(k) for k in ("producer", "title", "license", "urlMain", "dateAccessed",
                                               "datePublished", "versionProducer")}
                        for o in j.get("origins", [])
                    ]
                    c["indicator_license"] = j.get("license")
                    c["indicator_keys"] = sorted(j.keys())[:40]
                    c["unit_ind"] = j.get("unit"), j.get("shortUnit"), j.get("display")
            cols[str(short)] = c
        out["columns"] = cols
        data = get(f"https://ourworldindata.org/grapher/{slug}.csv{Q}")
        out["csv_http"] = data.status_code
        out["csv_bytes"] = len(data.content)
        out["csv_lastmod"] = data.headers.get("Last-Modified")
        if data.status_code == 200:
            rows = list(csv.reader(io.StringIO(data.content.decode("utf-8"))))
            head, body = rows[0], rows[1:]
            out["header"] = head
            out["rows"] = len(body)
            codes = {}
            for r in body:
                codes.setdefault(r[1], set()).add(r[0])
            out["entities"] = len({r[0] for r in body})
            out["codes_iso3_like"] = sum(1 for c in codes if re.fullmatch(r"[A-Z]{3}", c))
            out["codes_other"] = sorted(c for c in codes if c and not re.fullmatch(r"[A-Z]{3}", c))
            out["no_code_entities"] = sorted(codes.get("", set()))
            out["years"] = [min(int(r[2]) for r in body), max(int(r[2]) for r in body)]
            for i, h in enumerate(head[3:], start=3):
                vals = [r[i] for r in body]
                bad = {}
                for v in vals:
                    try:
                        float(v)
                    except ValueError:
                        bad[v] = bad.get(v, 0) + 1
                out[f"col_{h}"] = {"nonnumeric": dict(sorted(bad.items(), key=lambda kv: -kv[1])[:12]),
                                   "zeros": sum(1 for v in vals if v in ("0", "0.0"))}
            keys = [(r[0], r[2]) for r in body]
            out["dup_entity_year"] = len(keys) - len(set(keys))
            out["pol"] = [r for r in body if r[1] == "POL"][-3:]
            codes_dup = {}
            for r in body:
                codes_dup.setdefault((r[1], r[2]), 0)
                codes_dup[(r[1], r[2])] += 1
            out["dup_code_year_nonempty"] = sum(1 for (c, y), n in codes_dup.items() if c and n > 1)
        else:
            out["csv_body"] = data.text[:300]
        print(json.dumps(out, indent=1, default=str), flush=True)
        small = {k: v for k, v in out.items() if k not in ("codes_other", "no_code_entities")}
        annotate("notice", f"owid probe {slug}", small)
        annotate("notice", f"owid probe {slug} codes", {"other": out.get("codes_other"),
                                                         "no_code": out.get("no_code_entities")})


if __name__ == "__main__":
    main()
