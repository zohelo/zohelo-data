"""Read-only probe, round 2 (12 Nov 2026): World Bank GEM API source 15 (countries, periods, page timing),
EEA greenhouse gas inventory download route (share redirect, public WebDAV listing, Discodata), OECD PISA
published country-mean tables (StatLink workbooks). GET/HEAD/PROPFIND only; writes nothing.
"""

from __future__ import annotations

import json
import re
import sys
import time

import requests

from dev.w3intl_probe import S, annotate, hdr, js, log


def call(method: str, url: str, timeout: int = 420, tries: int = 4, **kw):
    for attempt in range(tries):
        time.sleep(1.0)
        t0 = time.monotonic()
        try:
            r = S.request(method, url, timeout=timeout, **kw)
            out = {"st": r.status_code, "h": dict(r.headers), "body": r.content, "url": r.url, "sec": round(time.monotonic() - t0, 1)}
            if r.status_code in (429, 500, 502, 503, 504) and attempt < tries - 1:
                time.sleep(20 * 2**attempt)
                continue
            return out
        except requests.RequestException as exc:
            if attempt == tries - 1:
                return {"err": f"{type(exc).__name__}: {str(exc)[:120]}", "sec": round(time.monotonic() - t0, 1)}
            time.sleep(20 * 2**attempt)
    return {"err": "unreachable"}


def gem() -> None:
    api = "https://api.worldbank.org/v2/sources/15"
    out: dict = {}
    for c in ("country", "time"):
        r = call("GET", f"{api}/{c}?format=json&per_page=5000")
        d = js(r) if "body" in r else None
        try:
            vals = [(v["id"], v["value"]) for v in d["source"][0]["concept"][0]["variable"]]
            out[c] = {"n": len(vals), "head": vals[:5], "tail": vals[-5:], "sec": r["sec"]}
            log(f"gem {c}", vals)
        except Exception:  # noqa: BLE001
            out[c] = {**hdr(r), "sec": r.get("sec")}
    r = call("GET", f"{api}/country/POL;DEU;EUU/series/CPTOTSAXNZGY/time/all?format=json&per_page=50000")
    d = js(r) if "body" in r else None
    if isinstance(d, dict):
        recs = d["source"]["data"]
        times = sorted({v["id"] for rec in recs for v in rec["variable"] if v["concept"] == "Time"})
        out["sample3"] = {"total": d.get("total"), "pages": d.get("pages"), "sec": r["sec"], "bytes": len(r["body"]),
                          "non_null": sum(1 for x in recs if x.get("value") is not None), "times": times[:3] + times[-8:],
                          "rec": json.dumps(recs[-1])[:500], "keys": list(d.keys()), "lastupdated": d.get("lastupdated")}
    else:
        out["sample3"] = {**hdr(r), "sec": r.get("sec")}
    r = call("GET", f"{api}/country/all/series/CPTOTSAXNZGY/time/all?format=json&per_page=50000&page=1")
    d = js(r) if "body" in r else None
    if isinstance(d, dict):
        recs = d["source"]["data"]
        out["all_p1"] = {"total": d.get("total"), "pages": d.get("pages"), "sec": r["sec"], "bytes": len(r["body"]),
                         "non_null": sum(1 for x in recs if x.get("value") is not None)}
    else:
        out["all_p1"] = {**hdr(r), "sec": r.get("sec"), "body": (r.get("body") or b"")[:150].decode("utf-8", "replace")}
    r = call("HEAD", "https://databank.worldbank.org/data/download/GemDataEXTR.zip", timeout=60, tries=1, allow_redirects=True)
    out["bulk_zip_head"] = {**hdr(r), "final": r.get("url")}
    annotate("notice", "probe2 gem", out)


def eea() -> None:
    out: dict = {}
    cat = "https://sdi.eea.europa.eu/catalogue/srv/api"
    r = call("GET", f"{cat}/records/3b7fe76c-524a-439a-bfd2-a6e4046302a2/related?type=children", headers={"Accept": "application/json"})
    d = js(r) if "body" in r else None
    kids = []
    for item in (d or {}).get("children", []) or []:
        title = item.get("title") or item.get("resourceTitle") or {}
        kids.append({"id": item.get("id") or item.get("uuid"), "title": json.dumps(title)[:140]})
    out["children"] = kids[:12]
    log("eea related children raw", d)
    r = call("GET", "https://sdi.eea.europa.eu/data/f6a252a6-1bde-43a8-9e6a-f58146fd691e", timeout=60, allow_redirects=True)
    final = r.get("url", "")
    hist = [h for h in re.findall(r"datashare/s/([A-Za-z0-9]+)", final + " " + (r.get("body") or b"").decode("utf-8", "replace"))]
    token = hist[0] if hist else None
    out["share"] = {"final": final, "token": token}
    if token:
        dav = "https://sdi.eea.europa.eu/datashare/public.php/webdav/"
        listing = []
        todo = [""]
        while todo and len(listing) < 200:
            path = todo.pop(0)
            r = call("PROPFIND", dav + path, timeout=120, tries=2, auth=(token, ""), headers={"Depth": "1"})
            text = (r.get("body") or b"").decode("utf-8", "replace")
            if "err" in r or r.get("st") not in (207,):
                out.setdefault("propfind_errors", []).append({"path": path, **hdr(r)})
                break
            for resp in re.findall(r"<d:response>(.*?)</d:response>", text, re.S):
                href = re.search(r"<d:href>([^<]+)</d:href>", resp).group(1)
                size = re.search(r"<d:getcontentlength>(\d+)</d:getcontentlength>", resp)
                lm = re.search(r"<d:getlastmodified>([^<]+)</d:getlastmodified>", resp)
                rel = href.split("/public.php/webdav/", 1)[-1]
                if rel == path:
                    continue
                listing.append({"p": rel, "size": int(size.group(1)) if size else None, "lm": lm.group(1) if lm else None})
                if href.endswith("/") and rel.count("/") <= 3:
                    todo.append(rel)
        out["files"] = [x for x in listing if x["size"] is not None][:40]
        out["folders"] = [x["p"] for x in listing if x["size"] is None][:20]
        log("eea webdav listing", listing)
        csvs = [x for x in listing if x["size"] and re.search(r"\.(csv|zip)$", x["p"], re.I)]
        if csvs:
            small = min(csvs, key=lambda x: x["size"])
            r = call("GET", dav + small["p"], timeout=120, tries=2, auth=(token, ""), headers={"Range": "bytes=0-1500"})
            out["first_bytes"] = {"p": small["p"], **hdr(r), "start": (r.get("body") or b"")[:1200].decode("utf-8", "replace")}
    q = "SELECT name FROM sys.databases"
    r = call("GET", "https://discodata.eea.europa.eu/sql", timeout=60, tries=1, params={"query": q, "p": 1, "nrOfHits": 500})
    d = js(r) if "body" in r else None
    names = [x.get("name") for x in (d or {}).get("results", [])] if isinstance(d, dict) else []
    out["discodata_dbs"] = names[:80] if names else {**hdr(r), "body": (r.get("body") or b"")[:200].decode("utf-8", "replace")}
    annotate("notice", "probe2 eea 1/2", {k: out.get(k) for k in ("children", "share", "folders", "propfind_errors")})
    annotate("notice", "probe2 eea 2/2", {k: out.get(k) for k in ("files", "first_bytes", "discodata_dbs")})


def pisa() -> None:
    out: dict = {}
    for key, url in (
        ("vol1", "https://www.oecd.org/en/publications/pisa-2022-results-volume-i_53f23881-en.html"),
        ("dash", "https://www.oecd.org/en/data/dashboards/pisa-education-and-skills.html"),
        ("db2025", "https://www.oecd.org/en/data/datasets/pisa-2025-database.html"),
        ("terms", "https://www.oecd.org/en/about/terms-conditions.html"),
    ):
        r = call("GET", url, timeout=60, tries=2)
        text = (r.get("body") or b"").decode("utf-8", "replace")
        links = sorted(set(re.findall(r'href="([^"]*(?:stat\.link|\.xlsx|\.csv|/full-report/|annex|data-explorer|DataExplorer|sdmx)[^"]*)"', text, re.I)))
        plain = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", re.sub(r"(?is)<(script|style).*?</\1>", " ", text)))
        terms = [plain[max(0, m.start() - 150): m.start() + 350] for m in re.finditer(r"PISA data|Creative Commons|CC BY|dataset[s]? (?:are|is) |third.part", plain, re.I)][:5]
        out[key] = {**hdr(r), "links": links[:25], "terms": terms}
        log(f"pisa {key} links", links)
        log(f"pisa {key} terms", terms)
    annotate("notice", "probe2 pisa 1/2", {k: out[k] for k in ("vol1", "dash")})
    annotate("notice", "probe2 pisa 2/2", {k: out[k] for k in ("db2025", "terms")})


def main() -> int:
    for name in sys.argv[1:] or ["eea", "pisa", "gem"]:
        try:
            globals()[name]()
        except Exception as exc:  # noqa: BLE001
            annotate("error", f"probe2 {name} failed", {"error": f"{type(exc).__name__}: {str(exc)[:800]}"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
