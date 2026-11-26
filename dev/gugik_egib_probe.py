"""Read-only probe, GUGiK KI EGiB aggregate WFS (register PL-ENV-006, Task #3299), 26 Nov 2026.

Checks that resultType=hits with a fes:PropertyIsLike filter on NUMER_JEDNOSTKI (ms:dzialki) and ID_BUDYNKU
(ms:budynki) answers for one or two powiats and one voivodeship, the unfiltered national hits, the shape of the unit
codes (PROPERTYNAME only, 3 features), and the powiat list of GUGiK's BDOT10k package index (TERYT codes).
GET only, one request at a time, 2 s apart, the platform's User-Agent. No R2 access, nothing written.
"""

import json
import re
import sys
import time

import requests

from ingestion.common.report import annotate

EGIB = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza"
BDOT = (
    "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT/WFS/PobieranieBDOT10k?SERVICE=WFS&VERSION=1.0.0"
    "&REQUEST=GetFeature&TYPENAME=BDOT10k_powiaty&PROPERTYNAME=TERYT,NAZWA_POWIATU"
)
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers["User-Agent"] = UA
OUT: dict = {}


def get(url, params=None, timeout=(30, 300)):
    time.sleep(2)
    t = time.monotonic()
    try:
        r = S.get(url, params=params, timeout=timeout)
        return r.status_code, r.content, round(time.monotonic() - t, 2), r.headers.get("Content-Type")
    except requests.RequestException as exc:
        return None, str(exc)[:200].encode(), round(time.monotonic() - t, 2), None


def like(prop, pattern):
    return (
        '<fes:Filter xmlns:fes="http://www.opengis.net/fes/2.0">'
        '<fes:PropertyIsLike wildCard="%" singleChar="#" escapeChar="!">'
        f"<fes:ValueReference>{prop}</fes:ValueReference><fes:Literal>{pattern}</fes:Literal>"
        "</fes:PropertyIsLike></fes:Filter>"
    )


def hits(type_name, prop=None, pattern=None):
    params = {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": type_name,
              "RESULTTYPE": "hits"}
    if prop:
        params["FILTER"] = like(prop, pattern)
    st, body, sec, ct = get(EGIB, params)
    m = re.search(rb'numberMatched="([^"]+)"', body)
    rec = {"http": st, "s": sec, "bytes": len(body), "ct": ct, "matched": m.group(1).decode() if m else None}
    if not m:
        rec["body"] = body[:400].decode("utf-8", "replace")
    print(json.dumps({"hits": [type_name, prop, pattern], **rec}, ensure_ascii=False), flush=True)
    return rec


def sample(type_name, prop, pattern, propname):
    params = {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": type_name, "COUNT": "3",
              "PROPERTYNAME": propname, "FILTER": like(prop, pattern)}
    st, body, sec, ct = get(EGIB, params)
    text = body.decode("utf-8", "replace")
    vals = re.findall(rf"<ms:{propname}>([^<]*)</ms:{propname}>", text)
    print(json.dumps({"sample": [type_name, pattern], "http": st, "s": sec, "vals": vals, "head": text[:300]},
                     ensure_ascii=False), flush=True)
    return {"http": st, "s": sec, "vals": vals}


def main(args):
    st, body, sec, ct = get("https://mapy.geoportal.gov.pl/robots.txt")
    OUT["robots"] = {"http": st, "head": body[:200].decode("utf-8", "replace")}
    OUT["pl"] = {t: hits(t) for t in ("ms:dzialki", "ms:budynki")}
    first = hits("ms:dzialki", "NUMER_JEDNOSTKI", "0201%")
    OUT["p0201_dz"] = first
    if first["matched"] is None:
        OUT["p0201_dz_ms"] = hits("ms:dzialki", "ms:NUMER_JEDNOSTKI", "0201%")
    OUT["p0201_bu"] = hits("ms:budynki", "ID_BUDYNKU", "0201%")
    OUT["p1465_dz"] = hits("ms:dzialki", "NUMER_JEDNOSTKI", "1465%")
    OUT["p1465_bu"] = hits("ms:budynki", "ID_BUDYNKU", "1465%")
    OUT["w02_dz"] = hits("ms:dzialki", "NUMER_JEDNOSTKI", "02%")
    OUT["w02_bu"] = hits("ms:budynki", "ID_BUDYNKU", "02%")
    OUT["s_dz"] = sample("ms:dzialki", "NUMER_JEDNOSTKI", "1465%", "NUMER_JEDNOSTKI")
    OUT["s_bu"] = sample("ms:budynki", "ID_BUDYNKU", "1465%", "ID_BUDYNKU")
    st, body, sec, ct = get(BDOT)
    codes = sorted(set(re.findall(rb"<ms:TERYT>(\d{4})</ms:TERYT>", body)))
    codes = [c.decode() for c in codes]
    by_woj: dict = {}
    for c in codes:
        by_woj.setdefault(c[:2], []).append(c[2:])
    OUT["bdot"] = {"http": st, "s": sec, "bytes": len(body), "powiats": len(codes)}
    print("POWIATS " + json.dumps(by_woj), flush=True)
    if not codes:
        print(body[:600].decode("utf-8", "replace"), flush=True)
    annotate("notice", "gugik_egib probe", {k: v for k, v in OUT.items() if k != "robots"})
    annotate("notice", "gugik_egib powiats", by_woj)
    annotate("notice", "gugik_egib robots", OUT["robots"])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
