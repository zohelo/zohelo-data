"""Read-only probe of the GUGiK BDOT10k package index (PL-ENV-003, Task #3097).

GET and HEAD only, one request at a time, one second apart, robots.txt honoured, a request budget. No package is
downloaded (HEAD only for sizes). Reports the BDOT10k page's terms sentences and service links, the capabilities of
candidate index services (WMS/WFS), WFS hits and sample features of index layers, a WMS GetFeatureInfo sample, and
mentions of object statistics. Writes nothing to R2. Everything goes to the log.
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.robotparser

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BUDGET = {"requests": 110}
ROBOTS: dict[str, urllib.robotparser.RobotFileParser | None] = {}
PAGE = "https://www.geoportal.gov.pl/pl/dane/baza-danych-obiektow-topograficznych-bdot10k/"
CANDIDATES = [
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT/WMS/PobieranieBDOT10k",
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT10k/WMS/DaneDoPobrania",
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT/WFS/PobieranieBDOT10k",
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT10k/WFS/Skorowidze",
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT/WMS/SkorowidzeBDOT10k",
]


def note(section: str, item) -> None:
    print(f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)[:20000]}", flush=True)


def robots_ok(url: str) -> bool:
    parts = urllib.parse.urlsplit(url)
    host = f"{parts.scheme}://{parts.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = requests.get(host + "/robots.txt", headers={"User-Agent": UA}, timeout=30)
            rp.parse(r.text.splitlines() if r.status_code == 200 else [])
            note("robots", {"host": host, "status": r.status_code, "text": r.text[:400] if r.ok else ""})
        except requests.RequestException as e:
            note("robots", {"host": host, "error": f"{type(e).__name__}: {str(e)[:120]}"})
            rp = None
        ROBOTS[host] = rp
    rp = ROBOTS[host]
    return rp is None or rp.can_fetch(UA, url)


def fetch(url: str, method: str = "GET", limit: int = 3_000_000):
    if BUDGET["requests"] <= 0:
        return None, {"status": "request budget used"}
    if not robots_ok(url):
        return None, {"status": "robots disallow"}
    BUDGET["requests"] -= 1
    time.sleep(1.0)
    try:
        with requests.request(method, url, headers={"User-Agent": UA}, timeout=(20, 120), stream=True) as r:
            hdr = {k.lower(): v[:160] for k, v in r.headers.items() if k.lower() in (
                "content-type", "content-length", "last-modified", "etag", "content-disposition")}
            info = {"status": r.status_code, "final": r.url, **hdr}
            if method == "HEAD":
                return None, info
            body = b""
            for block in r.iter_content(1 << 20):
                body += block
                if len(body) >= limit:
                    break
            return body, {**info, "read": len(body)}
    except requests.RequestException as e:
        return None, {"status": f"{type(e).__name__}: {str(e)[:200]}"}


def plain(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def snippets(text: str, pattern: str, width: int = 250, n: int = 12) -> list[str]:
    return [text[max(0, m.start() - width): m.end() + width] for m in re.finditer(pattern, text, re.I)][:n]


def page() -> list[str]:
    body, info = fetch(PAGE)
    note("page", info)
    html = (body or b"").decode("utf-8", "replace")
    text = plain(html)
    for pat in (r"bezpłatn", r"dowoln", r"licencj", r"uwolnien", r"statysty", r"liczb\w* obiekt", r"aktualn"):
        note("page.terms", {"pattern": pat, "snippets": snippets(text, pat)})
    urls = sorted(set(re.findall(r"https?://[^\s\"'<>)]+", html)))
    found = [u for u in urls if re.search(r"wss/service|opendata|integracja|bdot|skorowid|WMS|WFS", u, re.I)]
    note("page.links", found[:80])
    return [u.split("?")[0] for u in found if "wss/service" in u or "integracja" in u]


def caps(base: str) -> None:
    for service in ("WFS", "WMS"):
        body, info = fetch(f"{base}?SERVICE={service}&REQUEST=GetCapabilities")
        text = (body or b"").decode("utf-8", "replace")
        ok = info.get("status") == 200 and "Capabilities" in text[:3000]
        note("caps", {"base": base, "service": service, **info, "ok": ok, "head": text[:300] if not ok else ""})
        if not ok:
            continue
        if service == "WFS":
            names = re.findall(r"<(?:wfs:)?Name>([^<]+)</(?:wfs:)?Name>", text)
            titles = re.findall(r"<(?:wfs:)?Title>([^<]+)</(?:wfs:)?Title>", text)
            fmts = sorted(set(re.findall(r"<(?:ows:)?Value>([^<]*(?:json|gml|GML|csv)[^<]*)</", text)))[:20]
            note("caps.wfs", {"base": base, "feature_types": names[:40], "titles": titles[:40], "formats": fmts})
            for name in names[:6]:
                wfs_layer(base, name)
        else:
            layers = re.findall(r"<Layer[^>]*queryable=\"1\"[^>]*>\s*<Name>([^<]+)</Name>\s*<Title>([^<]*)</Title>", text)
            alln = re.findall(r"<Name>([^<]+)</Name>\s*<Title>([^<]*)</Title>", text)
            infofmt = re.findall(r"<GetFeatureInfo>(.*?)</GetFeatureInfo>", text, re.S)
            note("caps.wms", {"base": base, "queryable": layers[:40], "layers": alln[:60],
                              "info_formats": re.findall(r"<Format>([^<]+)</Format>", infofmt[0])[:10] if infofmt else []})
            for name, _ in layers[:3]:
                wms_info(base, name)
        return


def wfs_layer(base: str, name: str) -> None:
    q = urllib.parse.quote(name)
    body, info = fetch(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={q}&RESULTTYPE=hits")
    note("wfs.hits", {"layer": name, **info, "body": (body or b"")[:600].decode("utf-8", "replace")})
    body, info = fetch(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=DescribeFeatureType&TYPENAMES={q}")
    note("wfs.describe", {"layer": name, **info, "body": (body or b"")[:3000].decode("utf-8", "replace")})
    body, info = fetch(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={q}&COUNT=3")
    note("wfs.sample", {"layer": name, **info, "body": (body or b"")[:6000].decode("utf-8", "replace")})


def wms_info(base: str, name: str) -> None:
    # Warsaw centre in EPSG:2180 (x = easting); WMS 1.3.0 with EPSG:2180 uses northing,easting axis order.
    bbox = "486000,637000,487000,638000"
    for fmt in ("text/html", "application/vnd.ogc.gml", "text/xml"):
        url = (f"{base}?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetFeatureInfo&LAYERS={urllib.parse.quote(name)}"
               f"&QUERY_LAYERS={urllib.parse.quote(name)}&CRS=EPSG:2180&BBOX={bbox}&WIDTH=101&HEIGHT=101"
               f"&I=50&J=50&INFO_FORMAT={urllib.parse.quote(fmt)}&FEATURE_COUNT=10&STYLES=&FORMAT=image/png")
        body, info = fetch(url)
        text = (body or b"").decode("utf-8", "replace")
        note("wms.info", {"layer": name, "format": fmt, **info, "body": text[:5000]})
        if info.get("status") == 200 and len(text) > 200 and "Exception" not in text[:500]:
            for u in sorted(set(re.findall(r"https?://[^\s\"'<>]+\.(?:zip|gml|7z)", text)))[:4]:
                _, h = fetch(u, "HEAD")
                note("package.head", {"url": u, **h})
            return


SERVICES = [
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT/WFS/PobieranieBDOT10k",
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT10k/WFS/StatystykiBudynkow",
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT10k/WFS/StatystykiSieciKomunikacyjnej",
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT10k/WFS/StatystykiSieciWodnej",
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT10k/WFS/StatystykiObiektowInnych",
]


def get_retry(url: str, tries: int = 3):
    for attempt in range(tries):
        body, info = fetch(url)
        if info.get("status") == 200 and body:
            return body, info
        time.sleep(10 * (attempt + 1))
    return body, info


def describe(base: str, name: str) -> list[tuple[str, str]]:
    q = urllib.parse.quote(name)
    body, info = get_retry(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=DescribeFeatureType&TYPENAMES={q}", 2)
    text = (body or b"").decode("utf-8", "replace")
    els = re.findall(r'<(?:xsd?:)?element[^>]*name="([^"]+)"[^>]*type="([^"]+)"', text)
    note("wfs.describe", {"layer": name, "status": info.get("status"), "elements": els[:80],
                          "head": text[:400] if not els else ""})
    return els


def service(base: str) -> None:
    body, info = get_retry(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetCapabilities")
    text = (body or b"").decode("utf-8", "replace")
    names = re.findall(r"<(?:wfs:)?FeatureType[^>]*>\s*<(?:wfs:)?Name>([^<]+)</", text)
    titles = re.findall(r"<(?:wfs:)?FeatureType[^>]*>\s*<(?:wfs:)?Name>[^<]+</(?:wfs:)?Name>\s*<(?:wfs:)?Title>([^<]*)<", text)
    fmts = sorted(set(re.findall(r"<(?:ows:)?Value>([^<]*(?:json|JSON|csv|CSV|gml|GML)[^<]*)</", text)))[:25]
    note("wfs.caps", {"base": base, "status": info.get("status"), "types": names, "titles": titles,
                      "formats": fmts, "head": text[:300] if not names else ""})
    for name in names[:6]:
        q = urllib.parse.quote(name)
        hb, hi = fetch(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={q}&RESULTTYPE=hits")
        hits = re.findall(r'numberMatched="([^"]+)"', (hb or b"").decode("utf-8", "replace"))
        note("wfs.hits", {"layer": name, "status": hi.get("status"), "numberMatched": hits,
                          "body": "" if hits else (hb or b"")[:300].decode("utf-8", "replace")})
        els = describe(base, name)
        props = [n for n, t in els if "gml:" not in t]
        pn = "&PROPERTYNAME=" + urllib.parse.quote(",".join(props)) if props else ""
        sb, si = fetch(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={q}&COUNT=3{pn}")
        note("wfs.sample", {"layer": name, **si, "body": (sb or b"")[:7000].decode("utf-8", "replace")})
        if "Pobieranie" in base:
            for fmt in ("application/json", "geojson", "csv"):
                jb, ji = fetch(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={q}&COUNT=2"
                               f"&OUTPUTFORMAT={urllib.parse.quote(fmt)}{pn}")
                note("wfs.format", {"layer": name, "format": fmt, **ji,
                                    "body": (jb or b"")[:1500].decode("utf-8", "replace")})


def main() -> None:
    idx = SERVICES[0]
    q = "ms:BDOT10k_powiaty"
    pn = "&PROPERTYNAME=TERYT,NAZWA_POWIATU,Data_aktualizacji,URL_GML"
    body, info = get_retry(f"{idx}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={q}{pn}")
    text = (body or b"").decode("utf-8", "replace")
    members = re.findall(r"<ms:BDOT10k_powiaty\b.*?</ms:BDOT10k_powiaty>", text, re.S)
    dates = re.findall(r"<ms:Data_aktualizacji>([^<]*)<", text)
    teryt = re.findall(r"<ms:TERYT>([^<]*)<", text)
    urls = re.findall(r"<ms:URL_GML>([^<]*)<", text)
    note("index.full", {**info, "members": len(members), "head": text[:1500], "first": members[:3], "last": members[-2:]})
    note("index.dates", {"distinct_formats": sorted({re.sub(r"[0-9]", "9", d) for d in dates}),
                         "min": min(dates) if dates else None, "max": max(dates) if dates else None,
                         "years": sorted({d[:4] for d in dates}), "empty": sum(1 for d in dates if not d.strip()),
                         "teryt_distinct": len(set(teryt)), "teryt_len": sorted({len(t) for t in teryt}),
                         "url_patterns": sorted({re.sub(r"[0-9]{2,4}_", "N_", re.sub(r"/[0-9]{2}/", "/NN/", u)) for u in urls})})
    by_year = {}
    for d in dates:
        by_year[d[:4]] = by_year.get(d[:4], 0) + 1
    note("index.by_year", by_year)
    body2, info2 = fetch(f"{idx}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={q}&COUNT=1")
    note("index.with_geometry_one", {**info2, "bytes": len(body2 or b"")})
    for base, layer in ((SERVICES[1], "ms:budynki_woj"), (SERVICES[4], "ms:obiekty_inne_woj"),
                        (SERVICES[2], "ms:drogi_woj"), (SERVICES[1], "ms:budynki_pow")):
        els = describe(base, layer)
        props = [n for n, t in els[1:] if "gml:" not in t]
        pn = "&PROPERTYNAME=" + ",".join(props)
        sb, si = fetch(f"{base}?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES={layer}{pn}")
        t = (sb or b"").decode("utf-8", "replace")
        n = len(re.findall(r"<ms:TERYT>", t))
        note("stats.full", {"layer": layer, **si, "features": n, "head": t[:300],
                            "first": re.sub(r"\s+", " ", t[t.find("<ms:JEDNOSTKA"):][:3500])})
        mb, mi = fetch(f"{base}?REQUEST=GetMetadata&layer={layer.split(':')[1]}")
        note("stats.meta", {"layer": layer, "status": mi.get("status"),
                            "body": re.sub(r"\s+", " ", (mb or b"").decode("utf-8", "replace"))[:2500]})
    note("budget", BUDGET)


if __name__ == "__main__":
    main()
