"""Read-only reachability probe for the source review batch C+G (12 Nov 2026).

GET/HEAD only, one compact annotation per register id. robots.txt is honoured (for the generic
agent and for Claude agents); a refused or failing host is reported, never retried another way.
"""

import concurrent.futures as cf
import html
import json
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser

from ingestion.common.report import annotate

UA = "zohelo-data-research/1.0 (+https://zohelo.com)"
CTX = ssl.create_default_context()
ROBOTS: dict[str, object] = {}


def raw_get(url, n=400000, method="GET", accept="*/*", timeout=40):
    req = urllib.request.Request(
        url, method=method, headers={"User-Agent": UA, "Accept": accept, "Accept-Language": "pl,en"}
    )
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        body = b"" if method == "HEAD" else r.read(n)
        h = r.headers
        return {
            "st": r.status,
            "ct": (h.get("content-type") or "")[:40],
            "len": h.get("content-length"),
            "lm": h.get("last-modified"),
            "etag": (h.get("etag") or "")[:40] or None,
            "rl": {k: v for k, v in h.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"} or None,
            "final": r.geturl() if r.geturl() != url else None,
            "body": body,
        }


def robots_ok(url):
    p = urllib.parse.urlsplit(url)
    host = f"{p.scheme}://{p.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = raw_get(host + "/robots.txt", 200000, timeout=20)
            txt = r["body"].decode("utf-8", "replace")
            rp.parse(txt.splitlines() if "<html" not in txt[:300].lower() else [])
            ROBOTS[host] = rp
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                ROBOTS[host] = f"robots.txt {e.code}"
            else:
                rp.parse([])
                ROBOTS[host] = rp
        except Exception as e:  # noqa: BLE001
            ROBOTS[host] = f"robots.txt {type(e).__name__}"
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return rp
    for agent in ("zohelo-data-research", "ClaudeBot", "Claude-User"):
        if not rp.can_fetch(agent, url):
            return f"robots disallow ({agent})"
    return None


def get(url, n=400000, method="GET", accept="*/*"):
    """Fetch; returns a dict with 'err' instead of raising."""
    try:
        no = robots_ok(url)
        if no:
            return {"err": no}
        return raw_get(url, n, method, accept)
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"err": f"{type(e).__name__}: {str(e)[:80]}"}


def meta(r):
    if "err" in r:
        return r["err"]
    out = {k: r[k] for k in ("st", "ct", "len", "lm", "etag", "rl", "final") if r.get(k)}
    if r.get("body"):
        out["read"] = len(r["body"])
    return out


def head(url):
    r = get(url, method="HEAD")
    if "err" in r and r["err"].startswith("HTTP 4"):
        r = get(url, n=1)
    return meta(r)


def text(r):
    return r["body"].decode("utf-8", "replace") if "body" in r else ""


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def links(t, pattern, limit=12, base=""):
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', t):
        h, a = html.unescape(m.group(1)), plain(m.group(2))[:50]
        if re.search(pattern, h + " " + a, re.I):
            item = f"{a}|{urllib.parse.urljoin(base, h)}" if a else urllib.parse.urljoin(base, h)
            if item not in out:
                out.append(item)
            if len(out) >= limit:
                break
    return out


def around(t, pattern, before=60, after=300, limit=2):
    p = plain(t) if "<" in t[:2000] else re.sub(r"\s+", " ", t)
    out, last = [], -9999
    for m in re.finditer(pattern, p, re.I):
        if m.start() - last < after:
            continue
        last = m.start()
        out.append(p[max(0, m.start() - before) : m.start() + after])
        if len(out) >= limit:
            break
    return out or "no match"


def js_only(r):
    t = text(r)
    return {"plain_chars": len(plain(t)), "scripts": len(re.findall(r"(?i)<script", t)), "title": plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:60]}


def file_urls(page, pattern=r"\.(xlsx?|zip|csv)(\?|$)|static\.nbp\.pl/dane", limit=6):
    r = get(page)
    if "err" in r:
        return {"page": r["err"]}
    ls = links(text(r), pattern, limit, page)
    out = {"page": r["st"], "n": len(ls), "files": ls}
    out["head"] = [head(u.split("|")[-1]) for u in ls[:2]]
    return out


NBP = "https://nbp.pl/en/statistic-and-financial-reporting/monetary-and-financial-statistics/"
KNF = "https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne"


def plg_004():
    r = get(NBP)
    hub = links(text(r), r"monetary-and-financial-statistics/[a-z0-9-]+/?$", 20, NBP) if "body" in r else r["err"]
    return {
        "hub": [x.split("statistics/")[-1] for x in hub] if isinstance(hub, list) else hub,
        "m3": file_urls(NBP + "m3-and-counterparts/"),
        "m3_text": around(text(get(NBP + "m3-and-counterparts/")), r"updat|revis|publish", 80, 250, 1),
    }


def plg_005():
    return {
        "mfi_assets_liabilities": file_urls(NBP + "assets-and-liabilities-of-monetary-financial-institutions/"),
        "aggregated": file_urls(NBP + "aggregated-balance/", limit=3),
        "consolidated": file_urls(NBP + "consolidated-balance-sheet-of-mfis/", limit=3),
    }


def plg_017():
    nota = get("https://www.knf.gov.pl/o_nas/urzad_komisji/nota_prawna")
    lst = get(KNF)
    return {
        "monthly": file_urls(KNF + "?articleId=56224&p_id=18", r"\.xlsx?", 4),
        "nota": around(text(nota), r"Przy wykorzystywaniu", 250, 420, 1) if "body" in nota else nota["err"],
        "listing_bank": links(text(lst), r"bank", 8, KNF) if "body" in lst else lst["err"],
    }


def plg_018():
    lst = get(KNF)
    out = {"listing": [x[:120] for x in links(text(lst), r"articleId", 14, KNF)] if "body" in lst else lst["err"]}
    img = "https://www.knf.gov.pl/knf/pl/komponenty/img/"
    out["insurance_A"] = head(img + "A_informacje_IIQ2026_99684.xlsx")
    out["insurance_D"] = head(img + "D_sprawozdanie%20statystyczne_IIQ2026_99687.xlsx")
    out["ofe"] = head(img + "WWW_OFE_dane_mies_2026_09_100266.xlsx")
    return out


def gl_036():
    c = get("https://cds.climate.copernicus.eu/api/catalogue/v1/collections/reanalysis-era5-land-monthly-means", accept="application/json")
    out = {"catalogue": meta(c)}
    if "body" in c:
        try:
            j = json.loads(text(c))
            out["keys"] = sorted(j)[:20]
            out["license"] = j.get("license")
            out["extent"] = j.get("extent", {}).get("temporal")
        except Exception as e:  # noqa: BLE001
            out["json"] = str(e)[:60]
    out["retrieve_no_token"] = meta(get("https://cds.climate.copernicus.eu/api/retrieve/v1/processes/reanalysis-era5-land-monthly-means", n=2000, accept="application/json"))
    return out


def gl_037():
    rid = "3b7fe76c-524a-439a-bfd2-a6e4046302a2"
    rec = get(f"https://sdi.eea.europa.eu/catalogue/srv/api/records/{rid}", n=600000, accept="application/json")
    out = {"record": meta(rec)}
    t = text(rec)
    urls = sorted(set(re.findall(r'https://sdi\.eea\.europa\.eu/(?:webdav|data)/[^"\\<\s]+', t)))
    out["urls"] = urls[:6]
    out["licence"] = around(t, r"CC-BY|creativecommons", 80, 200, 1)
    out["data_link"] = head(f"https://sdi.eea.europa.eu/data/{rid}")
    dav = [u for u in urls if "webdav" in u]
    if dav:
        d = get(dav[0] + "/")
        out["webdav"] = links(text(d), r".", 12, dav[0] + "/") if "body" in d else d["err"]
    out["discodata"] = meta(get("https://discodata.eea.europa.eu/Help.html", n=2000))
    return out


def wfs_caps(url, n=1500000):
    r = get(url, n=n)
    if "err" in r:
        return {"caps": r["err"]}
    t = text(r)
    names = re.findall(r"<(?:wfs:)?Name>([^<]+)</(?:wfs:)?Name>", t)
    return {
        "caps": meta(r),
        "n_types": len(names),
        "types": names[:14],
        "fees": plain(" ".join(re.findall(r"(?is)<(?:ows:)?Fees>.*?</(?:ows:)?Fees>", t)))[:200],
        "constraints": plain(" ".join(re.findall(r"(?is)<(?:ows:)?AccessConstraints>.*?</(?:ows:)?AccessConstraints>", t)))[:400],
        "count_default": re.findall(r'(?is)CountDefault.*?<ows:DefaultValue>(\d+)', t)[:1],
    }


def env_006():
    wfs = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza"
    out = wfs_caps(wfs + "?service=WFS&request=GetCapabilities")
    page = get("https://www.geoportal.gov.pl/pl/dane/ewidencja-gruntow-i-budynkow-egib/", n=900000)
    t = text(page)
    out["page_bulk"] = around(t, r"GeoParquet", 300, 300, 1)
    out["page_free"] = around(t, r"bezpłatn|dowolnego wykorzyst", 120, 250, 2)
    out["opendata_links"] = links(t, r"opendata\.geoportal|parquet|gpkg", 6)
    return out


def env_007():
    b = "https://opendata.geoportal.gov.pl/prg/adresy/"
    return {"shp": head(b + "PunktyAdresowe/POLSKA.zip"), "gml": head(b + "PRG-punkty_adresowe.zip")}


def env_011():
    a = get("https://wody.gios.gov.pl/pjwp/")
    b = get("https://wody.gios.gov.pl/pjwp/publication/367")
    out = {"home": meta(a), "pub367": meta(b)}
    if "body" in a:
        out["home_js"] = js_only(a)
        out["api_hints"] = sorted(set(re.findall(r'["\'](/?[a-zA-Z0-9_/.-]*api[a-zA-Z0-9_/.-]*)["\']', text(a))))[:8]
    reuse = get("https://www.gov.pl/web/gios/monitoring-wod")
    out["gov_links"] = links(text(reuse), r"wyniki|dane|xls|zip|pjwp", 8, "https://www.gov.pl") if "body" in reuse else reuse["err"]
    return out


def env_012():
    b = "https://mjwp.gios.gov.pl/"
    out = {"xlsx_2025": [head(b + "g2/oryginal/2026_03/" + f + ".xlsx") for f in ("a9590ac1beebee567d198e94179f5a37", "a11519d817605decc04b34db8e993004")]}
    for y in ("2024", "2015", "1991-2003"):
        r = get(f"{b}wyniki-badan/wyniki-badan-{y}.html")
        if "err" in r:
            out[y] = r["err"]
            continue
        ls = links(text(r), r"\.(xlsx?|zip|csv|pdf)$", 30, b)
        out[y] = {"n": len(ls), "ext": sorted({x.rsplit(".", 1)[-1] for x in ls}), "first": ls[:1]}
    out["terms"] = around(text(get(f"{b}wyniki-badan/wyniki-badan-2025.html")), r"Podmioty wykorzystujące", 10, 330, 1)
    return out


def env_013():
    a = get("https://wody.isok.gov.pl/hydroportal.html")
    out = {"hydroportal": meta(a)}
    if "body" in a:
        out["js"] = js_only(a)
    i = get("https://isok.gov.pl/inspire.html")
    out["inspire"] = links(text(i), r"wms|wfs|atom|pobier|download|csw|geonetwork", 8, "https://isok.gov.pl/") if "body" in i else i["err"]
    rest = get("https://wody.isok.gov.pl/gis/rest/services?f=json", n=20000, accept="application/json")
    out["arcgis_rest"] = meta(rest)
    if "body" in rest:
        out["arcgis_body"] = text(rest)[:300]
    return out


def pgi_services(pattern):
    out = {}
    u = get("http://uslugi_gis.pgi.gov.pl")
    out["uslugi_gis"] = links(text(u), pattern, 8, "http://uslugi_gis.pgi.gov.pl") if "body" in u else u["err"]
    rest = get("https://cbdgmapa.pgi.gov.pl/arcgis/rest/services?f=json", n=40000, accept="application/json")
    out["arcgis_rest"] = meta(rest)
    if "body" in rest:
        out["arcgis_names"] = re.findall(r'"name"\s*:\s*"([^"]+)"', text(rest))[:25]
    return out


def env_014():
    out = pgi_services(r"otwor|wfs|wms|cbdg")
    dm = get("https://dm.pgi.gov.pl/")
    out["dm"] = meta(dm)
    if "body" in dm:
        out["dm_text"] = around(text(dm), r"Linki do pobrania|e-mail", 200, 300, 1)
        out["dm_otwory"] = around(text(dm), r"cbdg_otwory", 80, 120, 1)
    p = get("https://baza.pgi.gov.pl/podsystemy/otwory")
    out["page_links"] = links(text(p), r"shp|pobra|dm\.pgi|wms|wfs", 6, "https://baza.pgi.gov.pl") if "body" in p else p["err"]
    return out


def env_015():
    out = {}
    p = get("https://www.pgi.gov.pl/midas/dane-przestrzenne.html")
    out["page"] = meta(p)
    if "body" in p:
        out["links"] = links(text(p), r"wfs|wms|shp|zip|dm\.pgi|cbdg", 8, "https://www.pgi.gov.pl")
        out["text"] = around(text(p), r"WFS|SHP|aktualiz", 120, 300, 2)
    for name in ("midas",):
        c = wfs_caps(f"https://cbdgmapa.pgi.gov.pl/arcgis/services/{name}/MapServer/WFSServer?service=WFS&request=GetCapabilities", 400000)
        out["wfs_" + name] = c
    return out


def env_016():
    return {
        "mhp_page": meta(get("https://www.pgi.gov.pl/psh/dane-hydrogeologiczne-psh/947-bazy-danych-hydrogeologiczne/8888-dane-hydrogeologiczne-mhp.html", n=5000)),
        "order": around(text(get("https://www.pgi.gov.pl/psh/dane-hydrogeologiczne-psh/8850-jak-zamowic-dane-hydrogeologiczne.html")), r"bezpłatnie|opłat", 200, 400, 2),
        "epsh": meta(get("https://epsh.pgi.gov.pl/epsh/", n=5000)),
    }


def env_017():
    w = "https://sdi.gdos.gov.pl/wfs"
    out = wfs_caps(w + "?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetCapabilities")
    hits = {}
    for t in ("ParkiNarodowe", "Rezerwaty", "ParkiKrajobrazowe", "SpecjalneObszaryOchrony", "ObszarySpecjalnejOchrony", "PomnikiPrzyrody", "UzytkiEkologiczne"):
        r = get(f"{w}?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TYPENAME=GDOS:{t}&resultType=hits", n=3000)
        m = re.search(r'numberOfFeatures="(\d+)"', text(r))
        hits[t] = int(m.group(1)) if m else r.get("err", "?")
    out["hits"] = hits
    z = get(f"{w}?SERVICE=WFS&VERSION=1.0.0&REQUEST=GetFeature&TYPENAME=GDOS:ParkiNarodowe&SRSNAME=EPSG:2180&outputFormat=shape-zip", n=20000000)
    out["parki_narodowe_zip"] = meta(z)
    b = wfs_caps("https://bankdanych.gdos.gov.pl/backend/wfs?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetCapabilities")
    out["bdzp"] = {k: b.get(k) for k in ("caps", "n_types", "fees", "constraints")}
    out["bdzp"]["types"] = (b.get("types") or [])[:6]
    g = get("https://www.gov.pl/web/gdos/ponowne-wykorzystywanie-informacji")
    out["reuse_page"] = around(text(g), r"warunk\w+ ponownego|źródł", 100, 400, 2) if "body" in g else g["err"]
    return out


def env_018():
    out = {}
    for host in ("https://www9.bdl.lasy.gov.pl", "https://www.bdl.lasy.gov.pl", "https://ogcapi.bdl.lasy.gov.pl"):
        try:
            r = raw_get(host + "/robots.txt", 3000, timeout=20)
            out[host.split("//")[1] + "/robots.txt"] = re.sub(r"\s+", " ", r["body"].decode("utf-8", "replace"))[:350]
        except Exception as e:  # noqa: BLE001
            out[host.split("//")[1] + "/robots.txt"] = f"{type(e).__name__}: {str(e)[:60]}"
    p = get("https://www9.bdl.lasy.gov.pl/portal/uslugi-ogc?v=1")
    out["ogc_page"] = meta(p)
    if "body" in p:
        out["ogc_links"] = links(text(p), r"wfs|wms|wmts|ogcapi|features", 10, "https://www9.bdl.lasy.gov.pl")
        out["ogc_text"] = around(text(p), r"licenc|regulamin|warunk|wykorzyst", 100, 350, 2)
    return out


def pse(entity, day):
    q = urllib.parse.quote(f"business_date eq '{day}'")
    r = get(f"https://api.raporty.pse.pl/api/{entity}?$filter={q}", n=3000000, accept="application/json")
    if "err" in r:
        return r["err"]
    try:
        j = json.loads(text(r))
        v = j.get("value", [])
        return {"rows": len(v), "keys": sorted(v[0])[:14] if v else None, "first": {k: v[0][k] for k in list(v[0])[:6]} if v else None, "next": bool(j.get("nextLink")), "bytes": len(r["body"]), "rl": r.get("rl")}
    except Exception as e:  # noqa: BLE001
        return {"st": r["st"], "body": text(r)[:150], "e": str(e)[:50]}


def env_019():
    root = get("https://api.raporty.pse.pl/api/", accept="application/json")
    out = {"root": meta(root)}
    names = re.findall(r'"name"\s*:\s*"([^"]+)"', text(root))
    out["n_entities"] = len(names)
    out["entities"] = names[:60]
    out["rce_pln_2026-11-10"] = pse("rce-pln", "2026-11-10")
    out["rce_pln_2024-06-14"] = pse("rce-pln", "2024-06-14")
    out["rce_pln_2024-06-13"] = pse("rce-pln", "2024-06-13")
    out["his_wlk_cal_2026-11-10"] = pse("his-wlk-cal", "2026-11-10")
    home = get("https://www.pse.pl/", n=900000)
    out["legal_links"] = links(text(home), r"nota|regulamin|prawn|zastrze|polityk", 6, "https://www.pse.pl/") if "body" in home else home["err"]
    return out


def env_020():
    u = "https://www.ure.gov.pl"
    p = get(u + "/pl/energia-elektryczna/ceny-wskazniki/7852,Srednia-cena-sprzedazy-energii-elektrycznej-na-rynku-konkurencyjnym-roczna-i-kwa.html")
    out = {"p7852": meta(p)}
    if "body" in p:
        t = text(p)
        out["tr"] = len(re.findall(r"(?i)<tr", t))
        out["dates"] = around(t, r"Data publikacji", 0, 120, 1)
        out["legal_links"] = links(t, r"prawn|polityk|ponown|zastrze|autorsk", 6, u)
    g = get(u + "/pl/paliwa-gazowe/ceny-wskazniki")
    out["gas"] = [x[:110] for x in links(text(g), r"ceny-wskazniki/\d+", 8, u)] if "body" in g else g["err"]
    b = get("https://bip.ure.gov.pl/bip/ponowne-wykorzystywanie")
    out["bip_reuse"] = around(text(b), r"warunk\w+ ponownego|źródł", 100, 400, 2) if "body" in b else b["err"]
    pol = get(u + "/pl/urzad/polityka-prywatnosci-i-informacje-prawne")
    out["site_legal"] = around(text(pol), r"prawa autorskie|kopiowa|wykorzyst\w+ (treści|materiał)|źródł", 100, 400, 2) if "body" in pol else pol["err"]
    return out


def entsog(frm, to):
    q = urllib.parse.urlencode({"limit": 1, "operatorKey": "PL-TSO-0002", "indicator": "Physical Flow", "periodType": "day", "from": frm, "to": to})
    r = get("https://transparency.entsog.eu/api/v1/operationaldatas?" + q, n=200000, accept="application/json")
    if "err" in r:
        return r["err"]
    try:
        j = json.loads(text(r))
        return {"total": j.get("meta", {}).get("total"), "rl": r.get("rl")}
    except Exception:  # noqa: BLE001
        return {"st": r["st"], "body": text(r)[:120]}


def env_021():
    out = {"entsog_2026-11-01": entsog("2026-11-01", "2026-11-02"), "entsog_2016-01-01": entsog("2016-01-01", "2016-01-02"), "entsog_2013-10-01": entsog("2013-10-01", "2013-10-02")}
    pts = get("https://transparency.entsog.eu/api/v1/operatorpointdirections?operatorKey=PL-TSO-0002&limit=1", n=100000, accept="application/json")
    m = re.search(r'"total"\s*:\s*(\d+)', text(pts))
    out["entsog_point_directions"] = int(m.group(1)) if m else meta(pts)
    e = get("https://www.entsog.eu/", n=900000)
    ll = links(text(e), r"legal|terms|disclaimer|copyright", 4, "https://www.entsog.eu/") if "body" in e else e["err"]
    out["entsog_legal_links"] = ll
    if isinstance(ll, list) and ll:
        lg = get(ll[0].split("|")[-1])
        out["entsog_legal"] = around(text(lg), r"reproduc|copyright|re-?use", 120, 450, 2) if "body" in lg else lg["err"]
    s = get("https://swi.gaz-system.pl/mir/")
    out["swi"] = meta(s)
    if "body" in s:
        out["swi_js"] = js_only(s)
    return out


def env_035():
    p = get("https://rejestr-urbanistyczny.gov.pl/published", n=900000)
    out = {"published": meta(p)}
    if "body" in p:
        t = text(p)
        out["js"] = js_only(p)
        out["links"] = links(t, r"wfs|wms|csw|api|pobier|regulamin|usług", 8, "https://rejestr-urbanistyczny.gov.pl/")
        out["hints"] = sorted(set(re.findall(r'https?://[a-zA-Z0-9./_-]*(?:wfs|wms|csw|geoserver|api)[a-zA-Z0-9./_-]*', t, re.I)))[:8]
    g = get("https://www.gov.pl/web/gov/skorzystaj-z-rejestru-urbanistycznego")
    out["service_card"] = around(text(g), r"WFS|usług\w+ sieciow", 120, 350, 2) if "body" in g else g["err"]
    return out


GROUPS = [
    [("PLG-004", plg_004), ("PLG-005", plg_005)],
    [("PLG-017", plg_017), ("PLG-018", plg_018)],
    [("GL-036", gl_036)],
    [("GL-037", gl_037)],
    [("PL-ENV-006", env_006), ("PL-ENV-007", env_007)],
    [("PL-ENV-011", env_011), ("PL-ENV-012", env_012)],
    [("PL-ENV-013", env_013)],
    [("PL-ENV-014", env_014), ("PL-ENV-015", env_015), ("PL-ENV-016", env_016)],
    [("PL-ENV-017", env_017)],
    [("PL-ENV-018", env_018)],
    [("PL-ENV-019", env_019)],
    [("PL-ENV-020", env_020)],
    [("PL-ENV-021", env_021)],
    [("PL-ENV-035", env_035)],
]


def run_group(group):
    res = []
    for sid, fn in group:
        try:
            res.append((sid, fn()))
        except Exception as e:  # noqa: BLE001
            res.append((sid, {"probe_error": f"{type(e).__name__}: {str(e)[:200]}"}))
    return res


def main():
    results = []
    with cf.ThreadPoolExecutor(len(GROUPS)) as ex:
        for res in ex.map(run_group, GROUPS):
            results += res
    # GitHub shows at most 10 annotations per level and step: half as notices, half as warnings.
    for i, (sid, data) in enumerate(results):
        annotate("notice" if i % 2 == 0 else "warning", f"probe {sid}", data)


if __name__ == "__main__":
    main()
