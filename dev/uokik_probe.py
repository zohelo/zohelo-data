"""Dev probe for #3061/#3064 (UOKiK decisions, PLG-020/PLG-021; test branch only, never on main).

Read-only. Reads only https://decyzje.uokik.gov.pl/bp/dec_prez.nsf (never uokik.gov.pl): the categorised Domino view
WWW-wszystkie as JSON, one request at a time, 1 s apart, long timeouts, at most 3 tries; a timeout is reported.
1. the collapsed view: the top-level categories (names, children, descendants);
2. the whole expanded view in pages of 500 (positions continue from the last entry): per category the years, the
   decision entries, @unid repeats, labels of the HTML cell, shapes of the numbers, DKK numbers;
nothing is stored; the log gets aggregates and masked cell skeletons (no party names).
"""

from __future__ import annotations

from collections import Counter, defaultdict
import html
import json
import re
import time

import requests

from ingestion.common.report import annotate

BASE = "https://decyzje.uokik.gov.pl/bp/dec_prez.nsf/WWW-wszystkie?ReadViewEntries&OutputFormat=JSON"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers["User-Agent"] = UA
STATS = {"requests": 0, "seconds": [], "retries": []}
LAST = [0.0]


def get(url: str) -> dict:
    for attempt in range(3):
        wait = 1.0 - (time.monotonic() - LAST[0])
        if wait > 0:
            time.sleep(wait)
        STATS["requests"] += 1
        t = time.monotonic()
        try:
            r = S.get(url, timeout=(30, 180))
            LAST[0] = time.monotonic()
            STATS["seconds"].append(round(LAST[0] - t, 1))
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code} {r.headers.get('content-type')}")
            return json.loads(r.content.decode("utf-8", "replace"), strict=False)
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            LAST[0] = time.monotonic()
            STATS["retries"].append(f"{type(exc).__name__}: {str(exc)[:150]}")
            print("retry", url, exc, flush=True)
            if attempt == 2:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError


def cell_texts(entry: dict) -> list[str]:
    out = []
    for d in entry.get("entrydata", []):
        for kind in ("text", "number", "datetime"):
            if kind in d:
                out.append(str(d[kind].get("0", "")))
        if "textlist" in d:
            out.extend(str(t.get("0", "")) for t in d["textlist"].get("text", []))
    return out


def skeleton(fragment: str) -> str:
    """Tags kept; text nodes ending with ':' kept (labels); other text as shape (letters L, digits N)."""
    def mask(m: re.Match) -> str:
        t = html.unescape(m.group(0))
        if t.strip().endswith(":") and len(t) < 60:
            return t
        return re.sub(r"\d", "N", re.sub(r"[^\W\d_]+", "L", t))
    return re.sub(r"(?<=>)[^<]+|^[^<]+", mask, fragment)[:1500]


def text(fragment: str) -> str:
    return " ".join(html.unescape(re.sub(r"(?s)<[^>]+>", " ", fragment)).split())


def network() -> dict:
    """Addresses of the host and whether a TCP connection to 443 and 80 opens (one try each, 20 s)."""
    import socket

    out = {}
    try:
        infos = socket.getaddrinfo("decyzje.uokik.gov.pl", 443, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        return {"resolve_error": str(exc)}
    for family, kind, proto, _, address in infos:
        for port in (443, 80):
            sock = socket.socket(family, kind, proto)
            sock.settimeout(20)
            try:
                sock.connect((address[0], port, *address[2:]))
                out[f"{address[0]}:{port}"] = "open"
            except OSError as exc:
                out[f"{address[0]}:{port}"] = f"{type(exc).__name__}: {exc}"[:120]
            finally:
                sock.close()
    return out


def main() -> int:
    reach = network()
    print("NETWORK", json.dumps(reach), flush=True)
    annotate("notice", "uokik probe network", reach)
    if not any(k.endswith(":443") and v == "open" for k, v in reach.items()):
        return 1
    # 1. categories
    top = get(BASE + "&CollapseView&count=200")
    cats = []
    for e in top.get("viewentry", []):
        cats.append({"pos": e.get("@position"), "children": e.get("@children"), "descendants": e.get("@descendants"),
                     "cell": cell_texts(e), "keys": sorted(e)})
    head = {k: v for k, v in top.items() if k != "viewentry"}
    print("TOP", json.dumps({"head": head, "cats": cats}, ensure_ascii=False), flush=True)
    annotate("notice", "uokik probe categories", {"head": head, "cats": [[c["pos"], c["cell"], c["children"],
                                                                         c["descendants"]] for c in cats]})
    # 2. the whole view
    started, start, seen_pos = time.monotonic(), "1", set()
    entries: list[dict] = []
    pages = 0
    while time.monotonic() - started < 40 * 60:
        page = get(BASE + f"&start={start}&count=501")
        pages += 1
        got = [e for e in page.get("viewentry", []) if e.get("@position") not in seen_pos]
        if pages == 1:
            print("PAGE1HEAD", json.dumps({k: v for k, v in page.items() if k != "viewentry"}), flush=True)
            print("FIRST", json.dumps([{k: v for k, v in e.items() if k != "entrydata"} for e in page["viewentry"][:4]]),
                  flush=True)
        if not got:
            break
        for e in got:
            seen_pos.add(e.get("@position"))
        entries.extend(got)
        start = got[-1]["@position"]
        if len(page.get("viewentry", [])) < 501:
            break
    by_cat: dict = defaultdict(lambda: {"years": Counter(), "decisions": 0, "dkk": 0, "prefix": Counter()})
    labels, unids, samples, depth = Counter(), Counter(), {}, Counter()
    cat_name, year = None, None
    leaf_keys, cat_keys = Counter(), Counter()
    number_shapes, date_shapes = Counter(), Counter()
    unid_cats = defaultdict(set)
    for e in entries:
        pos = e["@position"].split(".")
        depth[len(pos)] += 1
        cells = cell_texts(e)
        if e.get("@category") == "true" or (len(pos) < 3 and "@unid" not in e):
            cat_keys[tuple(sorted(e))] += 1
            if len(pos) == 1:
                cat_name = " | ".join(cells)
            elif len(pos) == 2:
                year = " | ".join(cells)
                by_cat[cat_name]["years"][year] += 0
            continue
        leaf_keys[tuple(sorted(e))] += 1
        frag = " ".join(cells)
        b = by_cat[cat_name]
        b["decisions"] += 1
        b["years"][year] += 1
        unids[e.get("@unid")] += 1
        unid_cats[e.get("@unid")].add(cat_name)
        for node in re.findall(r"(?:^|>)([^<]{2,50})(?=<|$)", frag):
            node = html.unescape(node).strip()
            if node.endswith(":"):
                labels[node] += 1
        t = text(frag)
        m = re.search(r"Numer decyzji:\s*(\S+)", t)
        num = m.group(1) if m else ""
        number_shapes[re.sub(r"\d", "N", num)] += 1
        b["prefix"][re.match(r"[^\d]*", num).group(0)] += 1
        if "DKK" in num.upper():
            b["dkk"] += 1
        for d in re.findall(r"\b\d{1,4}[-./]\d{1,2}[-./]\d{1,4}\b", t):
            date_shapes[re.sub(r"\d", "N", d)] += 1
        if cat_name not in samples:
            samples[cat_name] = [skeleton(frag), num, e.get("@unid"), e.get("@position")]
        elif len(samples) < 40 and len(samples.get(cat_name + "#2", [])) == 0 and b["decisions"] == 50:
            samples[cat_name + "#2"] = [skeleton(frag), num]
    out = {
        "pages": pages,
        "entries": len(entries),
        "depth": dict(depth),
        "seconds_total": round(time.monotonic() - started),
        "requests": STATS["requests"],
        "slowest": sorted(STATS["seconds"])[-5:],
        "retries": STATS["retries"],
        "decisions": sum(b["decisions"] for b in by_cat.values()),
        "distinct_unid": len(unids),
        "unid_repeated": sum(1 for v in unids.values() if v > 1),
        "unid_in_two_categories": sum(1 for v in unid_cats.values() if len(v) > 1),
        "leaf_keys": [[list(k), v] for k, v in leaf_keys.items()],
        "category_keys": [[list(k), v] for k, v in cat_keys.items()],
    }
    print("SUMMARY", json.dumps(out, ensure_ascii=False), flush=True)
    annotate("notice", "uokik probe view", out)
    cats_out = {k: {"decisions": v["decisions"], "dkk": v["dkk"], "years": [min(v["years"] or [""]), max(v["years"] or [""]),
                len(v["years"])], "prefix": v["prefix"].most_common(8)} for k, v in by_cat.items()}
    print("CATS", json.dumps(cats_out, ensure_ascii=False), flush=True)
    annotate("notice", "uokik probe per category", cats_out)
    shp = {"labels": labels.most_common(30), "number_shapes": number_shapes.most_common(30),
           "date_shapes": date_shapes.most_common(10)}
    print("SHAPES", json.dumps(shp, ensure_ascii=False), flush=True)
    annotate("notice", "uokik probe shapes", shp)
    print("SAMPLES", json.dumps(samples, ensure_ascii=False), flush=True)
    annotate("notice", "uokik probe samples (masked)", {k: v[:2] for k, v in list(samples.items())[:6]})
    # 3. one raw leaf entry, masked, all keys
    leaf = next((e for e in entries if "@unid" in e and len(e["@position"].split(".")) >= 3), None)
    if leaf:
        masked = {k: v for k, v in leaf.items() if k != "entrydata"}
        masked["entrydata"] = [{k: (v if k.startswith("@") else "<masked>") for k, v in d.items()} for d in leaf["entrydata"]]
        print("LEAF", json.dumps(masked, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
