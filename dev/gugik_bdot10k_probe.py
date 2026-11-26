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
BUDGET = {"requests": 70}
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


def main() -> None:
    discovered = page()
    seen = []
    for base in CANDIDATES + discovered:
        if base not in seen and BUDGET["requests"] > 8:
            seen.append(base)
            caps(base)
    for u in ("https://opendata.geoportal.gov.pl/bdot10k/", "https://opendata.geoportal.gov.pl/bdot10k/schemat2021/"):
        body, info = fetch(u)
        note("opendata.dir", {"url": u, **info, "body": (body or b"")[:3000].decode("utf-8", "replace")})
    note("budget", BUDGET)


if __name__ == "__main__":
    main()
