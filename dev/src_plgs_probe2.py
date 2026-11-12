"""Second read-only probe for the source review batch PLG, PLS (PLG-034, PLG-016; 12 Nov 2026, fix round).

Official alternative routes after the review of #2895: the Council of Ministers work list on gov.pl (CSV),
one slow retry of the RPL host, ESMA, EIOPA and EBA registers, a dane.gov.pl search. GET/HEAD only,
robots.txt honoured, one request per host at a time. No personal data is printed: the gov.pl CSV is
reported by header, counts and dates only (it has a column of responsible officials).
"""

import collections
import csv
import io
import json
import re
import urllib.error
import urllib.parse

from dev.src_plgs_probe import around, emit, err, get, links, meta, plain, raw_get, robots_raw, short, text


def jget(url, n=3000000, timeout=40):
    r = get(url, n=n, accept="application/json", timeout=timeout)
    if "err" in r:
        return None, r["err"]
    try:
        return json.loads(text(r)), meta(r)
    except Exception as e:  # noqa: BLE001
        return None, {"st": r["st"], "ct": r.get("ct"), "body": plain(text(r))[:160], "e": str(e)[:40]}


def csv_stats(url, maxlen=40000000):
    r = get(url, n=maxlen, timeout=240)
    if "err" in r:
        return r["err"]
    b = r["body"]
    out = {"bytes": len(b), "declared": r.get("len"), "lm": r.get("lm"), "etag": r.get("etag"), "ct": r.get("ct"), "cd": r.get("cd")}
    enc = "utf-8-sig"
    try:
        t = b.decode(enc)
    except UnicodeDecodeError:
        enc = "cp1250"
        t = b.decode(enc, "replace")
    out["enc"] = enc
    first = t.split("\n", 1)[0]
    sep = max(";,\t|", key=first.count)
    out["sep"] = sep
    rows = list(csv.reader(io.StringIO(t), delimiter=sep))
    head = [h.strip() for h in rows[0]]
    body = [x for x in rows[1:] if any(c.strip() for c in x)]
    out["header"] = [h[:42] for h in head]
    out["rows"] = len(body)
    out["ragged"] = sum(1 for x in body if len(x) != len(head))

    def col(pat):
        return next((i for i, h in enumerate(head) if re.search(pat, h, re.I)), None)

    num = col(r"numer|nr ")
    if num is not None:
        vals = [x[num].strip() for x in body if len(x) > num]
        out["numbers"] = {"distinct": len(set(vals)), "repeated": sum(1 for v, c in collections.Counter(vals).items() if c > 1),
                          "prefixes": dict(collections.Counter(re.match(r"[A-Za-z]*", v).group(0)[:4] for v in vals).most_common(10)),
                          "sample": vals[:2]}
    for key, pat in (("status", r"status"), ("kind", r"rodzaj"), ("type", r"typ")):
        i = col(pat)
        if i is not None:
            out[key] = {k[:28]: v for k, v in collections.Counter(x[i].strip() for x in body if len(x) > i).most_common(7)}
    dates = {}
    for i, h in enumerate(head):
        if re.search(r"data|termin", h, re.I):
            ds = sorted(d for d in (re.search(r"\d{4}-\d{2}-\d{2}|\d{2}[.-]\d{2}[.-]\d{4}", x[i]) for x in body if len(x) > i) if d for d in [d.group(0)])
            iso = sorted(d if d[4] == "-" else f"{d[6:]}-{d[3:5]}-{d[:2]}" for d in ds)
            dates[h[:30]] = [iso[0], iso[-1], len(iso)] if iso else "no dates"
    out["dates"] = dates
    out["rcl_links"] = sum(1 for x in body if "legislacja" in " ".join(x).lower())
    out["empty_cells"] = {h[:18]: n for i, h in enumerate(head) for n in [sum(1 for x in body if len(x) > i and not x[i].strip())] if n}
    return out


def govpl():
    a = {"robots": robots_raw("https://www.gov.pl", 600)}
    r = get("https://www.gov.pl/web/premier/wplip-rm", n=1500000)
    if "err" in r:
        a["page"] = r["err"]
    else:
        t = text(r)
        a["page"] = {"st": r["st"], "csv_links": sorted(set(re.findall(r"/register-file/[A-Za-z0-9_.-]+", t)))[:4],
                     "label": around(t, r"Pobierz dane do pliku", 20, 60, 1),
                     "footer": around(t, r"Treści tekstowe publikowane|nie wymaga zgody|materiałów urzędowych", 60, 330, 2)}
    t2 = get("https://www.gov.pl/web/premier/ponowne-wykorzystywanie")
    a["kprm_reuse"] = around(text(t2), r"są udostępniane w celu ponownego|Creative Commons|nie ponosi odpowiedzialności|w jakimkolwiek celu", 220, 420, 3) if "body" in t2 else t2["err"]
    b = {"rm": csv_stats("https://www.gov.pl/register-file/Rejestr_20874195.csv")}
    c = {}
    for key, u in (("archive_2019_2023_xls", "https://www.gov.pl/register-file/Rejestr_20476989.xls"),
                   ("archive_2015_2019", "https://www.gov.pl/attachment/432eecef-aa0e-403a-a8fc-1d436aee8609"),
                   ("archive_2011_2015", "https://www.gov.pl/attachment/7302f733-d506-4c6e-8ab9-c977b3c0118a")):
        h = get(u, method="HEAD")
        c[key] = meta(h)
    j, m = jget("https://api.sejm.gov.pl/sejm/term10/processes?limit=50", timeout=40)
    if isinstance(j, list):
        withrcl = [p for p in j if p.get("rclNum") or p.get("rclLink")]
        c["sejm_processes"] = {"n": len(j), "with_rcl": len(withrcl), "sample": [{k: str(p.get(k))[:70] for k in ("number", "rclNum", "rclLink")} for p in withrcl[:2]]}
    else:
        c["sejm_processes"] = m if j is None else str(j)[:150]
    c["prm"] = csv_stats("https://www.gov.pl/register-file/Rejestr_20874196.csv")
    return [("PLG-034 gov.pl work list page and terms", a), ("PLG-034 Council of Ministers list CSV", b), ("PLG-034 PM list, archives, Sejm link", c)]


def rpl():
    out = {}
    for host in ("https://legislacja.rcl.gov.pl", "https://legislacja.gov.pl"):
        try:
            r = raw_get(host + "/robots.txt", 60000, timeout=120)
            out[short(host)] = {"st": r["st"], "final": r.get("final"), "robots": re.sub(r"\s*\n\s*", " | ", r["body"].decode("utf-8", "replace").strip())[:900]}
        except urllib.error.HTTPError as e:
            out[short(host)] = f"robots.txt HTTP {e.code}"
        except Exception as e:  # noqa: BLE001
            out[short(host)] = f"robots.txt {type(e).__name__}: {str(e)[:70]}"
    if isinstance(out.get("legislacja.rcl.gov.pl"), dict):
        r = get("https://legislacja.rcl.gov.pl/", timeout=120)
        if "err" in r:
            out["home"] = r["err"]
        else:
            t = text(r)
            out["home"] = {"st": r["st"], "chars": len(plain(t)), "feeds": re.findall(r"(?i)<link[^>]+(?:rss|atom)[^>]*>", t)[:3],
                           "footer": [short(x) for x in links(t, r"prawn|regulamin|polityk|ponown|rss|api|eksport", 8, "https://legislacja.rcl.gov.pl", 30)]}
    return [("PLG-034 RPL host retry", out)]


def dane(q):
    j, m = jget("https://api.dane.gov.pl/1.4/datasets?per_page=6&q=" + urllib.parse.quote(q), timeout=45)
    if not j:
        return m
    data = j.get("data") if isinstance(j, dict) else None
    if not isinstance(data, list):
        return {"shape": str(j)[:120]}
    return {"count": (j.get("meta") or {}).get("count"), "hits": [f"{d.get('id')}:{str((d.get('attributes') or {}).get('title'))[:55]}" for d in data[:6]]}


def esma():
    base = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select?wt=json&rows=1&q="
    out = {"robots": robots_raw("https://registers.esma.europa.eu", 300)}
    j, m = jget(base + urllib.parse.quote("ae_entityTypeCode:MIF"))
    if not j:
        out["MIF"] = m
        return out
    docs = j.get("response", {}).get("docs", [])
    out["MIF"] = {"numFound": j.get("response", {}).get("numFound"), "fields": sorted(docs[0])[:60] if docs else None}
    cands = [k for k in (docs[0] if docs else {}) if re.search(r"ountry|emberState|omeMS", k)]
    out["country_fields"] = {k: str(docs[0][k])[:30] for k in cands}
    hit = None
    for k in cands:
        for v in ("PL", "POLAND", "Poland"):
            jj, _ = jget(base + urllib.parse.quote(f"ae_entityTypeCode:MIF AND {k}:{v}"))
            n = (jj or {}).get("response", {}).get("numFound")
            if n:
                hit = (k, v)
                d = jj["response"]["docs"][0]
                out["MIF_PL"] = {"filter": f"{k}:{v}", "numFound": n, "doc": {x: str(d[x])[:45] for x in sorted(d) if re.search(r"lei|name|ountry|ompetent|status|Date|Code", x)}}
                break
        if hit:
            break
    if hit:
        for code in ("UCI", "AIF", "CSP"):
            jj, mm = jget(base + urllib.parse.quote(f"ae_entityTypeCode:{code} AND {hit[0]}:{hit[1]}"))
            out[code + "_PL"] = (jj or {}).get("response", {}).get("numFound") if jj else mm
    lg = get("https://registers.esma.europa.eu/publication/legalNoticePage")
    out["legal"] = around(text(lg), r"Reproduction of all information|free of charge through|stated explicitly", 40, 330, 3) if "body" in lg else lg["err"]
    return out


def eiopa():
    out = {"robots": robots_raw("https://register.eiopa.europa.eu", 300)}
    for u in ("https://register.eiopa.europa.eu/registers/register-of-insurance-undertakings",
              "https://register.eiopa.europa.eu/Pages/Supervision/Register-of-Insurance-Undertakings.aspx"):
        r = get(u, n=1500000)
        key = "new" if "registers/" in u else "aspx"
        if "err" in r:
            out[key] = r["err"]
            continue
        t = text(r)
        out[key] = {"st": r["st"], "final": r.get("final"), "chars": len(plain(t)),
                    "export": [short(x)[:110] for x in links(t, r"csv|excel|xlsx|export|download", 6, r.get("final") or u, 40)],
                    "kw": around(t, r"Export Insurance Undertaking|updated weekly|weekly on", 60, 200, 1)}
    lg = get("https://www.eiopa.europa.eu/legal-notice_en")
    out["legal"] = around(text(lg), r"Reproduction of information and documents|Source: EIOPA|free of charge", 40, 330, 2) if "body" in lg else lg["err"]
    return out


def eba():
    out = {"robots": robots_raw("https://euclid.eba.europa.eu", 300)}
    u = "https://euclid.eba.europa.eu/register/pir/registerDownload"
    r = get(u, n=600000)
    if "err" in r:
        out["download_page"] = r["err"]
    else:
        t = text(r)
        out["download_page"] = {**{k: v for k, v in meta(r).items() if k != "read"}, "chars": len(plain(t)), "scripts": len(re.findall(r"(?i)<script", t)),
                                "bot_wall": sorted(set(x.lower() for x in re.findall(r"(?i)recaptcha|captcha", t)))[:2],
                                "links": [short(x)[:110] for x in links(t, r"\.zip|\.json|download|\.xlsx", 6, r.get("final") or u, 40)]}
    pg = get("https://www.eba.europa.eu/risk-and-data-analysis/data/registers/payment-institutions-register", n=900000)
    out["pir_page"] = {"st": pg["st"], "links": [short(x)[:120] for x in links(text(pg), r"euclid|\.zip|\.json|download", 6, "https://www.eba.europa.eu", 40)],
                       "kw": around(text(pg), r"download the whole content|at least once per day", 80, 240, 1)} if "body" in pg else pg["err"]
    cir = get("https://www.eba.europa.eu/risk-and-data-analysis/data/registers/credit-institutions-register", n=900000)
    out["cir_page"] = {"st": cir["st"], "links": [short(x)[:120] for x in links(text(cir), r"euclid|\.zip|\.json|\.xlsx|download", 6, "https://www.eba.europa.eu", 40)]} if "body" in cir else cir["err"]
    lg = get("https://eba.europa.eu/find-out-about-us/legal-notice")
    out["legal"] = around(text(lg), r"Reproduction of all EBA material|source is acknowledged", 60, 380, 1) if "body" in lg else lg["err"]
    return out


def knf_alt():
    res = []
    for name, fn in (("PLG-016 ESMA registers", esma), ("PLG-016 EIOPA register", eiopa), ("PLG-016 EBA registers", eba)):
        try:
            res.append((name, fn()))
        except Exception as e:  # noqa: BLE001
            res.append((name, err(e)))
    d = {}
    for q in ("Komisja Nadzoru Finansowego", "proces legislacyjny"):
        try:
            d[q] = dane(q)
        except Exception as e:  # noqa: BLE001
            d[q] = err(e)
        if isinstance(d[q], str) and "timed out" in d[q]:
            break
    res.append(("PLG-016 PLG-034 dane.gov.pl search", d))
    return res


def main():
    import concurrent.futures as cf

    def run(fn):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            return [(fn.__name__, err(e))]

    results = []
    with cf.ThreadPoolExecutor(3) as ex:
        for res in ex.map(run, (govpl, rpl, knf_alt)):
            results += res
    for title, data in results[:10]:
        emit("probe2 " + title, data)


if __name__ == "__main__":
    main()
