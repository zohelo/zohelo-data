"""Read-only probe for wave-3 international sources (12 Nov 2026): World Bank GEM (API source 15), UNHCR
Refugee Statistics API, EEA greenhouse gas inventory (catalogue records), OECD PISA country means.

GET/HEAD only, one request at a time with a pause, identifying User-Agent, robots.txt honoured. Details are
printed to the log; one or two compact annotations per source. Writes nothing.
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.parse
import urllib.robotparser

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers.update({"User-Agent": UA})
ROBOTS: dict[str, object] = {}


def allowed(url: str) -> str | None:
    p = urllib.parse.urlsplit(url)
    host = f"{p.scheme}://{p.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = S.get(host + "/robots.txt", timeout=30)
            rp.parse(r.text.splitlines() if r.status_code == 200 and "<html" not in r.text[:300].lower() else [])
            ROBOTS[host] = rp
        except Exception as exc:  # noqa: BLE001
            ROBOTS[host] = f"robots {type(exc).__name__}"
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return None
    return None if rp.can_fetch("zohelo-data", url) else "robots disallow"


def get(url: str, accept: str = "*/*", method: str = "GET", limit: int | None = None):
    time.sleep(0.7)
    no = allowed(url)
    if no:
        return {"err": no}
    try:
        r = S.request(method, url, headers={"Accept": accept}, timeout=120, stream=limit is not None)
        body = r.raw.read(limit) if limit is not None else r.content
        return {"st": r.status_code, "h": dict(r.headers), "body": body, "url": r.url}
    except Exception as exc:  # noqa: BLE001
        return {"err": f"{type(exc).__name__}: {str(exc)[:120]}"}


def hdr(r) -> dict:
    if "err" in r:
        return {"err": r["err"]}
    h = {k.lower(): v for k, v in r["h"].items()}
    out = {"st": r["st"], "ct": (h.get("content-type") or "")[:40], "len": h.get("content-length"), "lm": h.get("last-modified")}
    rl = {k: v for k, v in h.items() if "ratelimit" in k or k == "retry-after"}
    if rl:
        out["rl"] = rl
    if r.get("body") is not None:
        out["read"] = len(r["body"])
    return {k: v for k, v in out.items() if v}


def js(r):
    try:
        return json.loads(r["body"])
    except Exception:  # noqa: BLE001
        return None


def log(title: str, data) -> None:
    print(f"### {title}: {json.dumps(data, default=str)[:20000]}", flush=True)


# ---------------------------------------------------------------- World Bank GEM, API source 15
def gem() -> None:
    api = "https://api.worldbank.org/v2"
    out: dict = {}
    r = get(f"{api}/sources/15?format=json")
    meta = js(r)
    out["source"] = meta[1][0] if meta and len(meta) > 1 else hdr(r)
    r = get(f"{api}/sources/15/series?format=json&per_page=1000")
    listing = js(r) or {}
    series = []
    try:
        concepts = listing["source"][0]["concept"]
        out["concepts"] = [c["id"] for c in concepts]
        series = [(v["id"], v["value"]) for c in concepts for v in c["variable"]]
    except Exception as exc:  # noqa: BLE001
        out["series_err"] = f"{type(exc).__name__} {hdr(r)} {str(r.get('body'))[:300]}"
    out["series_total"] = listing.get("total")
    log("gem series", series)
    r = get(f"{api}/sources/15/concepts?format=json")
    log("gem concepts", js(r) or hdr(r))
    for c in ("country", "time"):
        r = get(f"{api}/sources/15/{c}?format=json&per_page=2000")
        d = js(r)
        try:
            vals = [v["id"] for v in d["source"][0]["concept"][0]["variable"]]
            out[f"{c}_count"] = len(vals)
            out[f"{c}_sample"] = vals[:6] + vals[-6:]
            log(f"gem {c}", vals)
        except Exception:  # noqa: BLE001
            out[f"{c}_err"] = str(r.get("body"))[:300]
    totals = {}
    for sid, _name in series:
        r = get(f"{api}/sources/15/country/all/series/{sid}/time/all?format=json&per_page=1")
        d = js(r)
        totals[sid] = (d or {}).get("total") if isinstance(d, dict) else str(r.get("body"))[:120]
    log("gem totals", totals)
    nums = [v for v in totals.values() if isinstance(v, int)]
    out["observations_sum"] = sum(nums)
    out["largest"] = sorted(totals.items(), key=lambda kv: -(kv[1] if isinstance(kv[1], int) else 0))[:5]
    if series:
        sid = series[0][0]
        for cc in ("POL", "EUU"):
            r = get(f"{api}/sources/15/country/{cc}/series/{sid}/time/all?format=json&per_page=5")
            d = js(r)
            log(f"gem sample {cc}", d)
            out[f"sample_{cc}"] = (json.dumps(d)[:600] if d else hdr(r))
        r = get(f"{api}/sources/15/country/all/series/{sid}/time/all?format=json&per_page=50000")
        out["page_full_first_series"] = hdr(r)
        d = js(r)
        if isinstance(d, dict):
            times = sorted({v["id"] for rec in d["source"]["data"] for v in rec["variable"] if v["concept"] == "Time"})
            out["first_series_time_codes"] = times[:4] + times[-4:]
            out["first_series_keys"] = list(d.keys())
    annotate("notice", "probe gem 1/2", {k: out.get(k) for k in ("source", "concepts", "series_total", "observations_sum", "largest", "country_count", "time_count", "time_sample")})
    annotate("notice", "probe gem 2/2", {k: out.get(k) for k in ("country_sample", "sample_POL", "sample_EUU", "page_full_first_series", "first_series_time_codes", "first_series_keys", "series_err")})


# ---------------------------------------------------------------- UNHCR Refugee Statistics API
def unhcr() -> None:
    api = "https://api.unhcr.org/population/v1"
    out: dict = {}
    probes = {
        "years": f"{api}/years/?limit=200",
        "pop_default": f"{api}/population/?limit=3&yearFrom=2024&yearTo=2024",
        "pop_pol": f"{api}/population/?limit=100&coa=POL&cf_type=ISO&yearFrom=1951",
        "pop_coa_all": f"{api}/population/?limit=1&coa_all=true&yearFrom=1951",
        "pop_coa_all_big": f"{api}/population/?limit=10000&coa_all=true&yearFrom=2023&yearTo=2023",
        "pop_pair": f"{api}/population/?limit=1&coa_all=true&coo_all=true&yearFrom=1951",
        "pop_pol_by_origin": f"{api}/population/?limit=3&coa=POL&cf_type=ISO&coo_all=true&year=2024",
        "asylum_apps_pol": f"{api}/asylum-applications/?limit=3&coa=POL&cf_type=ISO&yearFrom=2020",
        "footnotes": f"{api}/footnotes/?limit=3",
        "countries": f"{api}/countries/?limit=400",
    }
    for key, url in probes.items():
        r = get(url, accept="application/json")
        d = js(r)
        log(f"unhcr {key} {url}", d if d is not None else hdr(r))
        if isinstance(d, dict):
            items = d.get("items") or []
            out[key] = {
                **{k: d.get(k) for k in ("page", "maxPages", "total", "limit") if k in d},
                "n": len(items),
                "h": hdr(r),
                "first": json.dumps(items[0])[:700] if items else None,
            }
        else:
            out[key] = hdr(r)
    r = get(f"{api}/population/?limit=100&coa=POL&cf_type=ISO&yearFrom=2023&download=true")
    out["download"] = hdr(r)
    annotate("notice", "probe unhcr 1/2", {k: out.get(k) for k in ("years", "pop_default", "pop_pol", "pop_coa_all", "pop_coa_all_big")})
    annotate("notice", "probe unhcr 2/2", {k: out.get(k) for k in ("pop_pair", "pop_pol_by_origin", "asylum_apps_pol", "footnotes", "countries", "download")})


# ---------------------------------------------------------------- EEA greenhouse gas inventory
def eea() -> None:
    cat = "https://sdi.eea.europa.eu/catalogue/srv/api"
    out: dict = {}
    series_id = "3b7fe76c-524a-439a-bfd2-a6e4046302a2"
    version_id = "f6a252a6-1bde-43a8-9e6a-f58146fd691e"
    for key, rid in (("series", series_id), ("version", version_id)):
        r = get(f"{cat}/records/{rid}", accept="application/json")
        d = js(r)
        text = r.get("body", b"").decode("utf-8", "replace") if "body" in r else ""
        urls = sorted(set(re.findall(r"https?://[^\s\"'<>\\]+", text)))
        log(f"eea {key} urls", urls)
        log(f"eea {key} head", text[:6000])
        out[key] = {"h": hdr(r), "top_keys": list(d.keys())[:30] if isinstance(d, dict) else None,
                    "data_urls": [u for u in urls if re.search(r"/data/|webdav|download|\.zip|\.csv", u)][:12]}
        r = get(f"{cat}/records/{rid}/related?type=children&type=parent&type=siblings&type=associated", accept="application/json")
        d = js(r)
        log(f"eea {key} related", d if d is not None else hdr(r))
        rel = json.dumps(d) if d is not None else ""
        out[key]["related_ids"] = sorted(set(re.findall(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", rel)))[:15]
        out[key]["related_h"] = hdr(r)
    r = get(f"https://sdi.eea.europa.eu/data/{version_id}", method="GET", limit=4096)
    out["data_version"] = {**hdr(r), "final": r.get("url"), "start": (r.get("body") or b"")[:300].decode("utf-8", "replace")}
    r = get(f"https://sdi.eea.europa.eu/webdav/datastore/public/", limit=200000)
    text = (r.get("body") or b"").decode("utf-8", "replace")
    out["webdav_root"] = {**hdr(r), "ghg": sorted(set(re.findall(r'href="([^"]*(?:ghg|GHG|greenhouse|govreg|unfccc|UNFCCC)[^"]*)"', text)))[:10]}
    log("eea webdav root", re.findall(r'href="([^"]+)"', text)[:300])
    r = get("https://www.eea.europa.eu/en/datahub/datahubitem-view/3b7fe76c-524a-439a-bfd2-a6e4046302a2")
    text = (r.get("body") or b"").decode("utf-8", "replace")
    out["datahub_page"] = {**hdr(r), "links": sorted(set(re.findall(r'href="([^"]*(?:sdi\.eea|/data/|download)[^"]*)"', text)))[:10]}
    annotate("notice", "probe eea 1/2", {k: out.get(k) for k in ("series", "version")})
    annotate("notice", "probe eea 2/2", {k: out.get(k) for k in ("data_version", "webdav_root", "datahub_page")})


# ---------------------------------------------------------------- OECD PISA
def pisa() -> None:
    out: dict = {}
    r = get("https://sdmx.oecd.org/public/rest/dataflow/all/all/latest?detail=allstubs", accept="application/vnd.sdmx.structure+xml;version=2.1")
    text = (r.get("body") or b"").decode("utf-8", "replace")
    flows = re.findall(r'<structure:Dataflow[^>]*id="([^"]+)"[^>]*agencyID="([^"]+)"[^>]*version="([^"]+)".*?<common:Name xml:lang="en">([^<]*)</common:Name>', text, re.S)
    if not flows:
        flows = [(m[1], m[0], m[2], m[3]) for m in re.findall(r'<structure:Dataflow[^>]*agencyID="([^"]+)"[^>]*id="([^"]+)"[^>]*version="([^"]+)".*?<common:Name xml:lang="en">([^<]*)</common:Name>', text, re.S)]
    hits = [f for f in flows if re.search(r"PISA|student|skills|perform|mathemat|reading", f[0] + " " + f[3], re.I)]
    out["dataflows"] = {"h": hdr(r), "count": len(flows), "pisa_like": hits[:25]}
    log("pisa dataflows like", hits)
    for f in hits[:4]:
        fid, agency, ver = f[0], f[1], f[2]
        r = get(f"https://sdmx.oecd.org/public/rest/data/{agency},{fid},{ver}/all?format=csvfile&dimensionAtObservation=AllDimensions&lastNObservations=1", limit=3000)
        out.setdefault("samples", {})[fid] = {**hdr(r), "start": (r.get("body") or b"")[:900].decode("utf-8", "replace")}
    r = get("https://gpseducation.oecd.org/IndicatorExplorer?plotter=h5&query=2")
    text = (r.get("body") or b"").decode("utf-8", "replace")
    out["gps"] = {**hdr(r), "api_like": sorted(set(re.findall(r'["\']([^"\']*(?:api|json|Handler|Service|ashx|svc)[^"\']*)["\']', text)))[:15]}
    r = get("https://www.oecd.org/en/data/datasets/pisa-2022-database.html")
    text = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", (r.get("body") or b"").decode("utf-8", "replace")))
    out["db2022"] = {**hdr(r), "terms": [text[max(0, m.start() - 200): m.start() + 300] for m in re.finditer(r"licen|terms and conditions|copyright|Creative Commons|third.part", text, re.I)][:4]}
    annotate("notice", "probe pisa 1/2", out.get("dataflows"))
    annotate("notice", "probe pisa 2/2", {k: out.get(k) for k in ("samples", "gps", "db2022")})


def main() -> int:
    which = sys.argv[1:] or ["gem", "unhcr", "eea", "pisa"]
    for name in which:
        try:
            globals()[name]()
        except Exception as exc:  # noqa: BLE001
            annotate("error", f"probe {name} failed", {"error": f"{type(exc).__name__}: {str(exc)[:800]}"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
