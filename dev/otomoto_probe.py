"""Read-only probe for OTOMOTO Insights (CD-034, #3264): the August 2026 Infogram edition, twice; robots.txt; the
structure of window.infographicData (charts, sheets, titles); links to other editions; the Infogram terms page.
Writes nothing to R2. Deleted before merge."""

from __future__ import annotations

from hashlib import sha256
import json
import re
import sys
import time
import urllib.robotparser

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
EDITION = "https://infogram.com/0826otomotoinsights-1hxj48mj9m8o52v"


def get(url: str) -> requests.Response:
    time.sleep(5)
    return requests.get(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=(30, 120))


def note(title: str, data) -> None:
    text = json.dumps(data, ensure_ascii=False, default=str)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, json.loads(text) if len(text) <= 3900 else {"truncated": text[:3800]})


def cell(c):
    if isinstance(c, dict):
        return c.get("value")
    return c


def main(argv: list[str]) -> int:
    del argv
    robots = get("https://infogram.com/robots.txt")
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(robots.text.splitlines())
    note("robots", {"status": robots.status_code, "text": robots.text[:400], "edition_allowed": rp.can_fetch(UA, EDITION),
                    "terms_allowed": rp.can_fetch(UA, "https://infogram.com/terms")})
    reads = []
    bodies = []
    for _ in range(2):
        r = get(EDITION)
        bodies.append(r.content)
        reads.append({"status": r.status_code, "bytes": len(r.content), "sha": sha256(r.content).hexdigest()[:16],
                      "etag": r.headers.get("ETag"), "last_modified": r.headers.get("Last-Modified"),
                      "content_type": r.headers.get("Content-Type"), "cache": r.headers.get("Cache-Control")})
    head = requests.head(EDITION, headers={"User-Agent": UA}, timeout=60, allow_redirects=True)
    txt = bodies[0].decode("utf-8")
    # where do two reads differ?
    a, b = bodies[0], bodies[1]
    diffs = []
    if a != b:
        i = 0
        n = min(len(a), len(b))
        while i < n and len(diffs) < 6:
            if a[i] != b[i]:
                diffs.append({"at": i, "a": a[max(0, i - 60):i + 60].decode("utf-8", "replace"),
                              "b": b[max(0, i - 60):i + 60].decode("utf-8", "replace")})
                i += 2000
            else:
                i += 1
    m = re.search(r"window\.infographicData\s*=\s*(\{.*?\});\s*</script>", txt, re.S)
    data_same = None
    if m:
        m2 = re.search(r"window\.infographicData\s*=\s*(\{.*?\});\s*</script>", b.decode("utf-8"), re.S)
        data_same = bool(m2) and m2.group(1) == m.group(1)
    note("edition reads", {"reads": reads, "head": [head.status_code, head.headers.get("ETag"), head.headers.get("Last-Modified")],
                           "identical_bytes": a == b, "infographicData_identical": data_same, "diffs": diffs,
                           "scripts_with_window_vars": sorted(set(re.findall(r"window\.(\w+)\s*=", txt)))[:30]})
    d = json.loads(m.group(1))
    print("TOPKEYS", list(d.keys()), flush=True)
    for k, v in d.items():
        print("KEY", k, type(v).__name__, json.dumps(v, ensure_ascii=False)[:1500] if not isinstance(v, (dict, list)) or len(json.dumps(v)) < 1500 else json.dumps(v, ensure_ascii=False)[:1500], flush=True)
    # charts
    charts = []

    def walk(o, path, ctx):
        if isinstance(o, dict):
            if "chartData" in o and isinstance(o["chartData"], dict):
                charts.append((path, o, ctx))
            for k, v in o.items():
                walk(v, path + "/" + str(k), o if "id" in o else ctx)
        elif isinstance(o, list):
            for i, v in enumerate(o):
                walk(v, path + f"[{i}]", ctx)

    walk(d, "", None)
    summary = []
    for path, props, ctx in charts:
        cd = props["chartData"]
        sheets = cd.get("data") or []
        keys = sorted(cd.keys())
        heads = [[cell(c) for c in (s[0] if s else [])][:8] for s in sheets]
        firstcol = [[cell(r[0]) for r in s[1:4]] + ["..", cell(s[-1][0]) if s else None] for s in sheets]
        item = {"path": path[-90:], "chartData_keys": keys, "sheetnames": cd.get("sheetnames"), "rows": [len(s) for s in sheets],
                "heads": heads, "firstcol": firstcol, "props_keys": sorted(props.keys())[:30],
                "ctx_type": (ctx or {}).get("type"), "ctx_id": (ctx or {}).get("id")}
        for k in ("title", "chartTitle", "name", "caption", "legend"):
            if k in props:
                item[k] = str(props[k])[:200]
        summary.append(item)
        print("CHART", json.dumps(item, ensure_ascii=False), flush=True)
        print("CHARTPROPS", json.dumps({k: v for k, v in props.items() if k != "chartData"}, ensure_ascii=False)[:3000], flush=True)
        for si, s in enumerate(sheets):
            print("SHEET", path[-60:], si, json.dumps([[cell(c) for c in r] for r in s], ensure_ascii=False)[:4000], flush=True)
    note("charts (count, rows per sheet)", {"charts": len(summary), "rows": [c["rows"] for c in summary]})
    for i in range(0, len(summary), 4):
        note(f"charts {i}-{i + 3}", [{k: c[k] for k in ("sheetnames", "heads", "firstcol", "ctx_type")} for c in summary[i:i + 4]])
    # texts: every string that looks like a title, in order
    texts = []

    def strings(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in ("text", "html", "content", "title") and isinstance(v, str) and 3 < len(v) < 600:
                    texts.append(re.sub(r"<[^>]+>", " ", v)[:200])
                strings(v)
        elif isinstance(o, list):
            for v in o:
                strings(v)

    strings(d)
    for t in texts:
        print("TEXT", t, flush=True)
    links = sorted(set(re.findall(r"https?://[\w.-]*infogram\.com/[\w/-]+", txt)))
    otomoto = sorted(set(re.findall(r"https?://[\w.-]*otomoto\.pl/[\w/.-]*", txt)))
    user = {k: d.get(k) for k in d if re.search(r"user|owner|author|profile|team|creator", k, re.I)}
    note("links and owner", {"infogram_links": links[:30], "otomoto_links": otomoto[:20], "owner_keys": str(user)[:800],
                             "text_count": len(texts), "texts_head": texts[:25]})
    if rp.can_fetch(UA, "https://infogram.com/terms"):
        t = get("https://infogram.com/terms")
        plain = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", re.sub(r"(?is)<(script|style).*?</\1>", " ", t.text)))
        hits = []
        for mm in re.finditer(r"scrap|crawl|robot|automated|spider|harvest|data mining|extract", plain, re.I):
            hits.append(plain[max(0, mm.start() - 200): mm.start() + 250])
            if len(hits) >= 8:
                break
        note("infogram terms", {"status": t.status_code, "final": t.url, "chars": len(plain), "hits": hits})
        for h in hits:
            print("TERMS", h, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
