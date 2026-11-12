"""Read-only probe for PL-ENV-033 (GUNB RWDZ): producer bulk page, WFS capabilities, attribute list, paging.

GET only, one request at a time with pauses, user agent set. Values of fields that may name persons are never
printed: only field names, fill counts and value shapes (letters -> a, digits -> 9); sample values only for fields
whose names look like codes or dates.
"""

import re
import time
import urllib.error
import urllib.request
from collections import Counter

UA = "zohelo-data/1.0 (open data platform; contact via github.com/zohelo)"
B = "https://wyszukiwarka.gunb.gov.pl"
W = "https://mapy.geoportal.gov.pl/wss/ext/GlownyUrzadNadzoruBudowlanego/RWDZ-WFS"
SAFE = re.compile(r"(?i)data|date|kod|teryt|terc|rodzaj|kategor|typ|organ|woj|powiat|gmina|jedn|status|rok|numer_?w|id$|gml_id|obiekt|klasa|zamier|decyz|wniosk")


def get(url, n=3_000_000, timeout=120):
    t = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(n)
            return {"st": r.status, "ct": r.headers.get("Content-Type"), "lm": r.headers.get("Last-Modified"),
                    "len": r.headers.get("Content-Length"), "body": body, "s": round(time.time() - t, 1)}
    except urllib.error.HTTPError as e:
        return {"st": e.code, "err": f"HTTP {e.code}", "body": e.read(3000), "s": round(time.time() - t, 1)}
    except Exception as e:  # noqa: BLE001
        return {"err": f"{type(e).__name__}: {str(e)[:200]}", "s": round(time.time() - t, 1)}


def txt(r):
    return r.get("body", b"").decode("utf-8", "replace")


def plain(s):
    return re.sub(r"\s+", " ", re.sub(r"(?s)<script.*?</script>|<style.*?</style>|<[^>]+>", " ", s)).strip()


def show(label, r, n=600):
    print(f"## {label}: st={r.get('st')} err={r.get('err')} ct={r.get('ct')} lm={r.get('lm')} len={r.get('len')} "
          f"bytes={len(r.get('body', b''))} s={r.get('s')}")
    if n:
        print(plain(txt(r))[:n])


def shape(v):
    return re.sub(r"[0-9]", "9", re.sub(r"[^\W\d_]", "a", v))[:40]


def features(xml):
    return re.findall(r"(?s)<ms:(pozwolenia_\w+)\b[^>]*>(.*?)</ms:\1>", xml)


def fields(body):
    return re.findall(r"(?s)<ms:(\w+)>([^<]*)</ms:\1>", body)


def main():
    print("# GUNB producer site")
    for p in ("/robots.txt", "/pobranie.html", "/"):
        r = get(B + p)
        show(p, r, 2500)
        if p == "/pobranie.html" and "body" in r:
            t = txt(r)
            ls = sorted(set(re.findall(r'href=["\']([^"\']+\.(?:zip|csv|7z|gz|xlsx?))["\']', t, re.I)))
            print("files:", len(ls), ls[:40])
            for u in ls[:3]:
                u = u if u.startswith("http") else B + "/" + u.lstrip("/")
                time.sleep(2)
                h = get(u, n=1)
                print("  file", u, h.get("st"), h.get("ct"), h.get("len"), h.get("lm"), h.get("err"))
        time.sleep(3)
    r = get("https://dev.wyszukiwarka.gunb.gov.pl/docs")
    show("api docs", r, 800)

    print("# WFS capabilities")
    time.sleep(2)
    c = get(W + "?SERVICE=WFS&REQUEST=GetCapabilities")
    show("caps", c, 0)
    ct = txt(c)
    print("versions", sorted(set(re.findall(r'version="([0-9.]+)"', ct[:3000]))))
    for tag in ("Fees", "AccessConstraints", "Abstract"):
        print(tag, plain(" ".join(re.findall(rf"(?is)<(?:ows:)?{tag}>.*?</(?:ows:)?{tag}>", ct)))[:600])
    print("types", re.findall(r"<(?:wfs:)?Name>([^<]+)</(?:wfs:)?Name>", ct))
    print("outputformats", sorted(set(re.findall(r"(?is)<ows:Value>([^<]*)</ows:Value>", ct)))[:60])
    print("constraints", re.findall(r'(?is)<ows:Constraint name="(\w+)">\s*<ows:NoValues/>\s*<ows:DefaultValue>([^<]*)', ct))
    print("constraints2", re.findall(r'(?is)<(?:fes|ows):Constraint name="(\w+)">.{0,200}?DefaultValue>([^<]*)', ct)[:40])

    print("# DescribeFeatureType")
    time.sleep(2)
    d = get(W + "?SERVICE=WFS&VERSION=2.0.0&REQUEST=DescribeFeatureType&TYPENAMES=ms:pozwolenia_2026")
    show("dft 2.0.0", d, 0)
    print(re.findall(r'<(?:xsd?:)?element[^>]*name="([^"]+)"[^>]*?(?:type="([^"]+)")?', txt(d)))
    time.sleep(2)
    d = get(W + "?SERVICE=WFS&VERSION=1.1.0&REQUEST=DescribeFeatureType&TYPENAME=ms:pozwolenia_2026")
    show("dft 1.1.0", d, 0)
    print(re.findall(r'<(?:xsd?:)?element[^>]*name="([^"]+)"[^>]*?(?:type="([^"]+)")?', txt(d)))

    print("# GetFeature sample (100) of 2026")
    time.sleep(2)
    g = get(W + "?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms:pozwolenia_2026&COUNT=100&STARTINDEX=0")
    show("gf 100", g, 0)
    x = txt(g)
    print("head", re.sub(r"\s+", " ", x[:900]))
    fs = features(x)
    print("features", len(fs), "bytes/feature", round(len(g.get("body", b"")) / max(len(fs), 1)))
    fill, shapes, samples, order = Counter(), {}, {}, []
    for _, body in fs:
        for k, v in fields(body):
            if k not in order:
                order.append(k)
            if v.strip():
                fill[k] += 1
            shapes.setdefault(k, Counter())[shape(v.strip())] += 1
            if SAFE.search(k):
                samples.setdefault(k, Counter())[v.strip()[:60]] += 1
    geom = re.findall(r"(?s)<ms:(\w+)>\s*<gml:", fs[0][1]) if fs else []
    print("geometry fields", geom)
    for k in order:
        print(f"  {k}: filled {fill[k]}/{len(fs)} shapes {shapes[k].most_common(4)}"
              + (f" samples {samples[k].most_common(6)}" if k in samples else " (values not printed)"))

    print("# formats and propertyName")
    for of in ("application/json", "geojson", "csv", "text/csv", "GML2"):
        time.sleep(2)
        r = get(W + f"?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms:pozwolenia_2026&COUNT=2&OUTPUTFORMAT={of}", n=4000)
        print("  of", of, r.get("st"), r.get("ct"), r.get("err"), len(r.get("body", b"")), plain(txt(r))[:160] if r.get("err") or "xml" in str(r.get("ct")) else "")
    if order:
        keep = ",".join(k for k in order if SAFE.search(k))[:600]
        time.sleep(2)
        r = get(W + f"?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms:pozwolenia_2026&COUNT=5&PROPERTYNAME={keep}")
        print("  propertyName", keep, r.get("st"), r.get("err"), [k for k, _ in fields(features(txt(r))[0][1])] if features(txt(r)) else plain(txt(r))[:300])

    print("# paging speed and order (2026)")
    for start, cnt in ((0, 1000), (100000, 1000), (300000, 5000)):
        time.sleep(3)
        r = get(W + f"?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms:pozwolenia_2026&COUNT={cnt}&STARTINDEX={start}", n=200_000_000, timeout=300)
        x = txt(r)
        fs = features(x)
        ids = [dict(fields(b)).get("id") or re.search(r'gml:id="([^"]+)"', b or "") for _, b in fs[:1]]
        print(f"  start {start} count {cnt}: st {r.get('st')} {r.get('err')} features {len(fs)} bytes {len(r.get('body', b''))} s {r.get('s')}"
              f" matched {re.findall(r'numberMatched=\"(\w+)\"', x[:2000])} returned {re.findall(r'numberReturned=\"(\w+)\"', x[:2000])}"
              f" gml ids {re.findall(r'gml:id=\"([^\"]+)\"', x)[:2]}")
    print("# hits")
    for y in ("2020", "2021", "2022", "2023", "2024", "2025", "2026", "pozostale"):
        time.sleep(2)
        r = get(W + f"?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms:pozwolenia_{y}&RESULTTYPE=hits", n=4000)
        print("  ", y, r.get("st"), r.get("err"), re.findall(r'numberMatched="(\w+)"', txt(r)), r.get("s"))


if __name__ == "__main__":
    main()
