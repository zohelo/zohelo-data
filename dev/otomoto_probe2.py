"""Read-only probe 2 for OTOMOTO Insights (CD-034, #3264): the August 2026 edition's tabs (blocks) with their texts and
charts in order and position, the full disclaimer and data-source texts, and the Infogram sitemap index named by
infogram.com/robots.txt (one request). Writes nothing to R2. Deleted before merge."""

from __future__ import annotations

import json
import re
import sys
import time

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


def plain(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s or "")).replace("&nbsp;", " ").strip()


def cell(c):
    return c.get("value") if isinstance(c, dict) else c


def main(argv: list[str]) -> int:
    del argv
    r = get(EDITION)
    m = re.search(r"window\.infographicData\s*=\s*(\{.*?\});\s*</script>", r.text, re.S)
    d = json.loads(m.group(1))
    content = d["elements"]["content"]["content"]
    print("CONTENT KEYS", list(content.keys()), flush=True)
    entities = content.get("entities") or {}
    sample = next(iter(entities.values()))
    print("ENTITY SAMPLE", json.dumps(sample, ensure_ascii=False)[:2500], flush=True)
    tabs = []
    for block_id in content["blockOrder"]:
        block = content["blocks"][block_id]
        items = []
        for key in block.get("entities", []):
            e = entities.get(key) or {}
            props = e.get("props") or {}
            pos = {k: e.get(k) for k in ("top", "left", "width", "height") if k in e}
            if not pos:
                pos = {k: props.get(k) for k in ("top", "left", "x", "y") if k in props}
            if isinstance(props.get("chartData"), dict):
                cd = props["chartData"]
                items.append({"key": key[-12:], "type": e.get("type"), "pos": pos, "chart": cd.get("sheetnames"),
                              "heads": [[cell(c) for c in (s[0] if s else [])][:4] for s in cd.get("data", [])][:3],
                              "rows": [len(s) for s in cd.get("data", [])]})
            else:
                txt = ""
                for k in ("content", "text", "html", "title"):
                    if isinstance(props.get(k), str):
                        txt = plain(props[k])
                        break
                if not txt:
                    blob = json.dumps(props, ensure_ascii=False)
                    found = re.findall(r'"(?:text|content|html)":\s*"((?:[^"\\]|\\.)*)"', blob)
                    txt = plain(" ".join(json.loads(f'"{f}"') for f in found))
                items.append({"key": key[-12:], "type": e.get("type"), "pos": pos, "text": txt[:160]})
        tab = {"block": block_id[:8], "header": plain(json.dumps(block.get("design", {}).get("header", {}), ensure_ascii=False))[:80],
               "items": items}
        tabs.append(tab)
        print("TAB", json.dumps(tab, ensure_ascii=False), flush=True)
    for t in tabs:
        compact = [(i.get("type"), i.get("pos"), i.get("chart") or i.get("text", "")[:70], i.get("heads"))
                   for i in t["items"] if i.get("chart") or (i.get("text") and len(i["text"]) < 90)]
        note(f"tab {t['block']}", compact[:30])
    # full long texts: disclaimers, data sources
    full = []

    def strings(o):
        if isinstance(o, dict):
            for v in o.values():
                strings(v)
        elif isinstance(o, list):
            for v in o:
                strings(v)
        elif isinstance(o, str):
            p = plain(o)
            if re.search(r"Wszelkie informacje|Wszystkie wizualizacje|należą do Grupy OLX|wykorzyst|kopiowa|licencj|prawa autorskie|©|Źródła danych", p):
                if p not in full:
                    full.append(p)

    strings(d)
    for f in full:
        print("FULLTEXT", f, flush=True)
    note("rights texts", [f[:1200] for f in full][:6])
    s = get("https://infogram.com/sitemaps/index.xml")
    locs = re.findall(r"<loc>([^<]+)</loc>", s.text)
    note("sitemap index", {"status": s.status_code, "bytes": len(s.content), "locs": len(locs), "first": locs[:8], "last": locs[-8:]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
