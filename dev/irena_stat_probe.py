"""Read-only probe for GL-035 (IRENASTAT PxWeb API): robots.txt, API config, the table tree of the folder
"Power Capacity and Generation", each table's metadata, and one small data query (Poland) per table.

GET and POST only to pxweb.irena.org, one request at a time, 3 s apart, an identifying User-Agent, bounded retries.
Nothing is written to R2. Full answers go to DEV_OUTPUT_DIR (artifact) and the job log; compact results to at most
ten notices. Deleted before merge (never on the PR branch).
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HOST = "https://pxweb.irena.org"
API = f"{HOST}/api/v1/en/IRENASTAT"
OUT = os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output")
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})


def call(url: str, query: dict | None = None) -> dict:
    last: dict = {}
    for attempt in range(4):
        time.sleep(3 if attempt == 0 else 15 * attempt)
        try:
            r = S.post(url, json=query, timeout=(20, 180)) if query else S.get(url, timeout=(20, 180))
        except requests.RequestException as exc:
            last = {"err": type(exc).__name__}
            continue
        last = {"st": r.status_code, "ct": r.headers.get("content-type", "")[:50], "n": len(r.content),
                "lm": r.headers.get("last-modified"), "server": r.headers.get("server")}
        if r.status_code in (429, 500, 502, 503, 504):
            last["body"] = r.text[:300]
            continue
        last["text"] = r.text
        return last
    return last


def short(r: dict, keep: int = 0) -> dict:
    out = {k: v for k, v in r.items() if k != "text"}
    if keep and "text" in r:
        out["head"] = r["text"][:keep]
    return out


def q(path: str) -> str:
    return "/".join(urllib.parse.quote(p) for p in path.split("/"))


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    full: dict = {}
    robots = call(f"{HOST}/robots.txt")
    config = call(f"{HOST}/api/v1/?config")
    root = call(API)
    full["robots"], full["config"], full["root"] = robots, config, root
    annotate("notice", "irena probe 1 robots config root",
             {"robots": short(robots, 300), "config": short(config, 600), "root": short(root, 1500)})
    tree: list[dict] = []
    tables: list[dict] = []
    folders = []
    try:
        for item in json.loads(root["text"]):
            if item.get("type") == "l" and "Power" in item.get("text", ""):
                folders.append(item["id"])
    except (KeyError, ValueError) as exc:
        annotate("error", "irena probe root not JSON", {"error": str(exc)})
        return 1
    while folders:
        f = folders.pop(0)
        r = call(f"{API}/{q(f)}")
        full.setdefault("folders", {})[f] = r
        try:
            items = json.loads(r["text"])
        except (KeyError, ValueError):
            tree.append({"folder": f, "error": short(r, 300)})
            continue
        for item in items:
            tree.append({"folder": f, **{k: item.get(k) for k in ("id", "type", "text", "updated")}})
            if item.get("type") == "l":
                folders.append(f"{f}/{item['id']}")
            elif item.get("type") == "t":
                tables.append({"path": f"{f}/{item['id']}", "text": item.get("text"), "updated": item.get("updated")})
    annotate("notice", "irena probe 2 tree", {"items": tree[:60]})
    metas = []
    for t in tables:
        r = call(f"{API}/{q(t['path'])}")
        full.setdefault("meta", {})[t["path"]] = r
        try:
            m = json.loads(r["text"])
        except (KeyError, ValueError):
            metas.append({"path": t["path"], "error": short(r, 300)})
            continue
        cells = 1
        variables = []
        for v in m.get("variables", []):
            cells *= len(v.get("values", []))
            variables.append({"code": v.get("code"), "text": v.get("text"), "n": len(v.get("values", [])),
                              "time": v.get("time"), "elim": v.get("elimination"),
                              "first": list(zip(v.get("values", [])[:6], v.get("valueTexts", [])[:6]))})
        t["variables"] = m.get("variables", [])
        metas.append({"path": t["path"], "title": m.get("title"), "cells": cells, "vars": variables})
    for i in range(0, len(metas), 2):
        annotate("notice", f"irena probe 3 meta {i // 2 + 1}", metas[i:i + 2])
    samples = []
    for t in tables[:4]:
        variables = t.get("variables") or []
        if not variables:
            continue
        sel = []
        for v in variables:
            codes = v.get("values", [])
            texts = v.get("valueTexts", [])
            pick = [c for c, x in zip(codes, texts) if "Poland" in x or c in ("POL", "PL")]
            if pick:
                sel.append({"code": v["code"], "selection": {"filter": "item", "values": pick}})
            else:
                sel.append({"code": v["code"], "selection": {"filter": "all", "values": ["*"]}})
        for fmt in ("json-stat2", "csv"):
            r = call(f"{API}/{q(t['path'])}", {"query": sel, "response": {"format": fmt}})
            full.setdefault("samples", {})[f"{t['path']}|{fmt}"] = r
            info = short(r)
            text = r.get("text", "")
            if fmt == "json-stat2" and text:
                try:
                    js = json.loads(text)
                    vals = js.get("value", [])
                    info.update({"size": js.get("size"), "id": js.get("id"), "n_values": len(vals),
                                 "nulls": sum(1 for x in vals if x is None), "status": str(js.get("status"))[:400],
                                 "extension": str(js.get("extension"))[:300], "note": str(js.get("note"))[:300],
                                 "updated": js.get("updated"), "first_values": vals[:12]})
                except ValueError:
                    info["head"] = text[:300]
            else:
                info["head"] = text[:700]
            samples.append({"path": t["path"], "fmt": fmt, **info})
    for i in range(0, len(samples), 2):
        annotate("notice", f"irena probe 4 sample {i // 2 + 1}", samples[i:i + 2])
    with open(os.path.join(OUT, "irena_probe.json"), "w", encoding="utf-8") as fh:
        json.dump(full, fh, ensure_ascii=False, indent=1, default=str)
    print(json.dumps({k: (v if k != "samples" else {kk: short(vv, 4000) for kk, vv in v.items()})
                      for k, v in full.items()}, ensure_ascii=False, default=str)[:400000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
