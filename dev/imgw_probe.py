"""Read-only probe of the IMGW public data archive and API (no writes anywhere).

Lists the archive directory tree (sizes per family and year range), downloads the format
description files and one small sample ZIP per family (in memory), reads the API endpoints once
and the apiinfo page (Regulamin). Sequential requests, a pause between them, bounded retries.
"""

from __future__ import annotations

import html
import io
import json
import re
import sys
import time
import zipfile
from collections import defaultdict
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate

BASE = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/"
API = "https://danepubliczne.imgw.pl/api/data/"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers["User-Agent"] = UA
LOG = []


def get(url: str) -> requests.Response:
    for attempt in range(3):
        time.sleep(0.4)
        try:
            r = S.get(url, timeout=(20, 120))
            if r.status_code == 200 or 400 <= r.status_code < 500:
                return r
        except requests.RequestException as exc:
            print("retry", url, exc, flush=True)
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"failed {url}")


ROW = re.compile(r'<a href="([^"?][^"]*)">[^<]*</a>\s*</td>\s*<td[^>]*>([^<]*)</td>\s*<td[^>]*>\s*([^<]*)</td>', re.I)
ROW2 = re.compile(r'<a href="([^"?][^"]*)">[^<]*</a>\s+(\d{4}-\d\d-\d\d \d\d:\d\d)\s+(\S+)', re.I)


def size_of(text: str) -> float:
    text = text.strip()
    m = re.match(r"([\d.]+)([KMG]?)", text)
    if not m:
        return 0.0
    return float(m.group(1)) * {"": 1, "K": 1e3, "M": 1e6, "G": 1e9}[m.group(2)]


def listing(url: str) -> list[tuple[str, str, float]]:
    r = get(url)
    body = r.text
    out = []
    for href, modified, size in ROW.findall(body) or ROW2.findall(body):
        if href.startswith("/") or href.startswith(".."):
            continue
        out.append((urljoin(url, href), modified.strip(), size_of(size)))
    if not out:
        LOG.append({"unparsed_listing": url, "head": body[:600]})
    return out


def family(rel: str) -> str:
    parts = rel.split("/")[:-1]
    out = []
    for part in parts:
        if re.match(r"^(1[89]|20)\d\d", part):
            break
        out.append(part)
    return "/".join(out)


def main() -> int:
    robots = get("https://danepubliczne.imgw.pl/robots.txt")
    annotate("notice", "imgw robots.txt", {"http": robots.status_code, "text": robots.text[:1500]})
    files = []  # (url, modified, bytes)
    pages = 0
    queue = [BASE]
    while queue and pages < 900:
        url = queue.pop(0)
        pages += 1
        for child, modified, size in listing(url):
            if child.endswith("/"):
                queue.append(child)
            else:
                files.append((child, modified, size))
    tree = defaultdict(lambda: {"files": 0, "bytes": 0.0, "years": set(), "newest": "", "examples": []})
    docs = []
    for url, modified, size in files:
        rel = url[len(BASE):]
        t = tree[family(rel)]
        t["files"] += 1
        t["bytes"] += size
        t["newest"] = max(t["newest"], modified)
        for y in re.findall(r"(?<!\d)(1[89]\d\d|20\d\d)(?!\d)", rel):
            t["years"].add(int(y))
        if len(t["examples"]) < 3 or not url.endswith(".zip"):
            if len(t["examples"]) < 6:
                t["examples"].append(rel)
        if not url.lower().endswith(".zip"):
            docs.append((url, size))
    summary = {
        k: {"files": v["files"], "MB": round(v["bytes"] / 1e6, 1), "years": [min(v["years"]), max(v["years"])] if v["years"] else None,
            "newest": v["newest"], "ex": v["examples"][:4]}
        for k, v in sorted(tree.items())
    }
    print(json.dumps({"pages": pages, "files": len(files), "tree": summary}, indent=1, ensure_ascii=False), flush=True)
    total = round(sum(f[2] for f in files) / 1e9, 2)
    compact = {k: [v["files"], v["MB"], v["years"], v["newest"][:10]] for k, v in summary.items()}
    blob = json.dumps(compact, ensure_ascii=False, separators=(",", ":"))
    for i in range(0, min(len(blob), 3700 * 3), 3700):
        annotate("notice", f"imgw tree {i // 3700} (files, MB, years, newest) total {total} GB pages {pages}", blob[i:i + 3700])
    # directory examples per family (last path names), to read the naming
    names = defaultdict(list)
    for url, modified, size in files:
        rel = url[len(BASE):]
        fam = family(rel)
        names[fam].append(rel[len(fam) + 1:] + f"|{int(size / 1e3)}k")
    ex = {k: (v[:3] + ["..."] + v[-4:]) for k, v in names.items()}
    print(json.dumps(ex, indent=1, ensure_ascii=False), flush=True)
    annotate("notice", "imgw file names per family", json.dumps(ex, ensure_ascii=False)[:3800])
    # format description and station files
    doc_out = {}
    for url, size in docs:
        r = get(url)
        raw = r.content
        try:
            text = raw.decode("utf-8")
            enc = "utf-8"
        except UnicodeDecodeError:
            text = raw.decode("cp1250", "replace")
            enc = "cp1250"
        rel = url[len(BASE):]
        doc_out[rel] = {"bytes": len(raw), "enc": enc}
        print(f"===== {rel} ({len(raw)} bytes, {enc})\n{text[:6000]}\n", flush=True)
        LOG.append({"doc": rel, "text": text[:6000]})
    annotate("notice", "imgw docs", doc_out)
    # one small sample zip per family (newest small file)
    samples = {}
    for fam, entries in names.items():
        cands = [(u, s) for u, m, s in files if family(u[len(BASE):]) == fam and u.endswith(".zip")]
        if not cands:
            continue
        cands.sort(key=lambda x: x[0])
        url, size = cands[-1]
        if size > 30e6:
            small = [c for c in cands if c[1] < 30e6]
            if not small:
                continue
            url, size = small[-1]
        r = get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        info = []
        for m in z.infolist():
            data = z.read(m)
            text = data.decode("cp1250", "replace")
            lines = text.splitlines()
            info.append({"member": m.filename, "bytes": m.file_size, "lines": len(lines),
                         "cols": len(lines[0].split(",")) if lines else 0, "head": lines[:3]})
            print(f"===== SAMPLE {url} :: {m.filename}\n" + "\n".join(lines[:8]), flush=True)
        samples[url[len(BASE):]] = info
    blob = json.dumps(samples, ensure_ascii=False)
    for i in range(0, min(len(blob), 3700 * 4), 3700):
        annotate("notice", f"imgw samples {i // 3700}", blob[i:i + 3700])
    # API
    api = {}
    for ep in ["synop", "hydro", "hydro2", "meteo", "warningsmeteo", "warningshydro", "product"]:
        r = get(API + ep)
        try:
            data = r.json()
        except ValueError:
            api[ep] = {"http": r.status_code, "bytes": len(r.content), "text": r.text[:200]}
            continue
        first = data[0] if isinstance(data, list) and data else data
        api[ep] = {"http": r.status_code, "bytes": len(r.content), "n": len(data) if isinstance(data, list) else None,
                   "first": first if len(json.dumps(first)) < 900 else json.dumps(first)[:900],
                   "headers": {k: r.headers.get(k) for k in ("Last-Modified", "ETag", "Cache-Control", "Content-Type")}}
        print(f"===== API {ep}\n{json.dumps(data, ensure_ascii=False)[:5000]}", flush=True)
    blob = json.dumps(api, ensure_ascii=False)
    for i in range(0, min(len(blob), 3700 * 3), 3700):
        annotate("notice", f"imgw api {i // 3700}", blob[i:i + 3700])
    # apiinfo page, Regulamin
    r = get("https://danepubliczne.imgw.pl/apiinfo")
    text = html.unescape(re.sub(r"<[^>]+>", " ", r.text))
    text = re.sub(r"\s+", " ", text)
    i = text.find("Regulamin")
    print("===== APIINFO\n" + text, flush=True)
    annotate("notice", "imgw apiinfo Regulamin", {"http": r.status_code, "at": i, "text": text[max(0, i - 100): i + 3500]})
    for kw in ["Źródło", "zrodlo", "przetworz", "komercyjn"]:
        j = text.find(kw)
        if j >= 0:
            print(f"===== KW {kw}: {text[max(0, j - 600): j + 900]}", flush=True)
    for item in LOG:
        print(json.dumps(item, ensure_ascii=False)[:8000], flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
