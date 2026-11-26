"""Read-only probe for the re-review of parked register entries, batch 5, Task #2917 (26 Nov 2026).

GL-031 ITU, GL-032 WIPO, GL-033 UNODC, GL-034 IEA, GL-035 IRENA, GL-040 UNIDO, PLS-008 REGON BIR1,
PLS-018 GIS report, PLS-033 KOWR, TP-007 PAP. GET and HEAD only, one request at a time, at least a second
apart per host, robots.txt honoured; a refused, walled or timed-out host is reported and not asked again.
At most ten notices carry the summary.
"""

import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser

UA = "zohelo-data-research/1.0 (+https://zohelo.com)"
CTX = ssl.create_default_context()
ROBOTS: dict[str, object] = {}
ROBOTS_TXT: dict[str, str] = {}
LAST: dict[str, float] = {}
DEAD: dict[str, str] = {}
COUNT = [0]
LIMIT = 3800
N: dict[str, object] = {}
ERR: list[str] = []
FILE_RE = r"\.(xlsx?|csv|zip|json|xml|ods|pdf|docx?)(\?|$)"
WALL = r"(?i)incapsula|_Incapsula_Resource|cf-challenge|captcha|access denied"


def raw(url, n=3000000, timeout=40, method="GET"):
    netloc = urllib.parse.urlsplit(url).netloc
    wait = 1.0 - (time.time() - LAST.get(netloc, 0))
    if wait > 0:
        time.sleep(wait)
    COUNT[0] += 1
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "pl,en"}, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            h = r.headers
            return {
                "st": r.status,
                "ct": (h.get("content-type") or "")[:60],
                "len": h.get("content-length"),
                "lm": h.get("last-modified"),
                "etag": (h.get("etag") or "")[:30] or None,
                "cd": (h.get("content-disposition") or "")[:90] or None,
                "final": r.geturl() if r.geturl() != url else None,
                "body": r.read(n) if method == "GET" else b"",
            }
    finally:
        LAST[netloc] = time.time()


def robots_ok(url, timeout=25):
    p = urllib.parse.urlsplit(url)
    host = f"{p.scheme}://{p.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            txt = raw(host + "/robots.txt", 100000, timeout)["body"].decode("utf-8", "replace")
            ROBOTS_TXT[host] = txt[:1500]
            rp.parse(txt.splitlines() if "<html" not in txt[:300].lower() and "<!doctype" not in txt[:300].lower() else [])
            if re.search(WALL, txt[:5000]):
                ROBOTS_TXT[host] = "bot-protection page instead of robots.txt"
                ROBOTS[host] = "bot protection on robots.txt"
            else:
                ROBOTS[host] = rp
        except urllib.error.HTTPError as e:
            ROBOTS_TXT[host] = f"HTTP {e.code}"
            if e.code in (401, 403):
                ROBOTS[host] = f"robots.txt {e.code}"
            else:
                rp.parse([])
                ROBOTS[host] = rp
        except Exception as e:
            ROBOTS_TXT[host] = ROBOTS[host] = f"robots.txt {type(e).__name__}: {str(e)[:80]}"
        print(f"=== ROBOTS {host}\n{ROBOTS_TXT[host]}\n=== END", flush=True)
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return rp
    for agent in ("zohelo-data-research", "ClaudeBot", "Claude-User"):
        if not rp.can_fetch(agent, url):
            return f"robots disallow ({agent})"
    return None


def get(url, n=3000000, timeout=40, method="GET", robots_timeout=25):
    host = urllib.parse.urlsplit(url).netloc
    if host in DEAD:
        return {"err": f"host skipped after: {DEAD[host]}"}
    try:
        no = robots_ok(url, robots_timeout)
        if no:
            if "timed out" in no or "bot protection" in no or "URLError" in no:
                DEAD[host] = no
            return {"err": no}
        return raw(url, n, timeout, method)
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:
        msg = f"{type(e).__name__}: {str(e)[:90]}"
        if "timed out" in msg or "Name or service" in msg or "CERTIFICATE" in msg:
            DEAD[host] = msg
        return {"err": msg}


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def links(t, base):
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', t):
        h, a = html.unescape(m.group(1)).strip(), plain(m.group(2))[:80]
        if h.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        u = urllib.parse.urljoin(base, h).split("#")[0]
        if (a, u) not in out:
            out.append((a, u))
    return out


def around(p, pattern, before=150, after=500, limit=4):
    out, last = [], -9999
    for m in re.finditer(pattern, p, re.I):
        if m.start() - last < after:
            continue
        last = m.start()
        out.append(p[max(0, m.start() - before) : m.start() + after])
        if len(out) >= limit:
            break
    return out


def tag(url):
    return re.sub(r"https?://(www\.)?", "", url)[:100]


def fail(url, r):
    print(f"=== {url}\nERR {r['err']}\n=== END", flush=True)
    ERR.append(f"{tag(url)}: {r['err']}")


def page(url, n=3000000, timeout=40, text=6000, robots_timeout=25):
    """Fetch an HTML page, print its text and links; return (text, links, html) or None."""
    r = get(url, n, timeout, robots_timeout=robots_timeout)
    if "err" in r:
        fail(url, r)
        return None
    t = r.pop("body").decode("utf-8", "replace")
    p = plain(t)
    wall = re.findall(WALL, t[:30000])[:2]
    title = plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:90]
    print(f"=== PAGE {url}\nMETA {json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False)} read={len(t)}", flush=True)
    print(f"TITLE {title}\nCHARS {len(p)} scripts={len(re.findall('(?i)<script', t))} tables={len(re.findall('(?i)<table', t))} wall={wall}", flush=True)
    print(f"TEXT {p[:text]}", flush=True)
    ll = links(t, r.get("final") or url)
    if wall and len(p) < 600:
        DEAD[urllib.parse.urlsplit(url).netloc] = "bot-protection page"
        ERR.append(f"{tag(url)}: bot-protection page ({len(p)} chars)")
    return p, ll, t


def end():
    print("=== END", flush=True)


def jget(url, n=8000000, timeout=60):
    r = get(url, n, timeout)
    if "err" in r:
        fail(url, r)
        return None
    try:
        return json.loads(r["body"].decode("utf-8", "replace"))
    except ValueError:
        print(f"=== {url}\nNOT JSON ct={r['ct']} head={r['body'][:300]!r}\n=== END", flush=True)
        ERR.append(f"{tag(url)}: not JSON ({r['ct']})")
        return None



def emit(title, data):
    msg = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    b = msg.encode("utf-8")
    if len(b) > LIMIT:
        msg = b[: LIMIT - 4].decode("utf-8", "ignore") + "…"
    msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{msg}", flush=True)


LIC = r"(?i)licencj|licenc|license|prawa autorskie|wszelkie prawa|all rights|copyright|ponowne wykorzyst|creative commons|cc by|warunki korzystania|regulamin|kopiowanie|rozpowszechnian"
DATA = r"(?i)pobierz|do pobrania|download|eksport|export|api|dane\.gov\.pl|otwarte dane|xlsx|csv"


SERVICE = r"(?i)wfs|wms|ogc|getcapabilities|atom|arcgis/rest|/api/|uslugi|services?\b"


def follow(ll, limit=4):
    out = {}
    for a, u in [(a, u) for a, u in ll if re.search(SERVICE, a + " " + u)][:limit]:
        r = get(u, 200000, 60)
        if "err" in r:
            out[u[-90:]] = r["err"][:60]
            continue
        b = r.pop("body").decode("utf-8", "replace")
        print(f"=== FOLLOW {a} | {u}\nMETA {json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False)}\nBODY {plain(b)[:3000]}\n=== END", flush=True)
        out[u[-90:]] = {"st": r.get("st"), "ct": r.get("ct"), "types": re.findall(r"<(?:wfs:)?Name>([^<]+)</", b)[:10], "head": plain(b)[:160]}
    return out


def probe_page(url, text=2500, follow_files=3, svc=False):
    got = page(url, text=text)
    if not got:
        return {"err": ERR[-1].split(": ", 1)[-1][:90] if ERR else "failed"}
    p, ll, t = got
    files = [(a, u) for a, u in ll if re.search(FILE_RE, u, re.I)]
    dl = [(a, u) for a, u in ll if re.search(DATA, a + " " + u) and (a, u) not in files]
    for a, u in files[:80]:
        print(f"  FILE {a} | {u}", flush=True)
    for a, u in dl[:40]:
        print(f"  DATA {a} | {u}", flush=True)
    for a, u in ll[:150]:
        print(f"  LINK {a} | {u}", flush=True)
    lic = around(p, LIC, 120, 300, 4)
    for s in lic:
        print(f"  LIC {s}", flush=True)
    end()
    ext: dict[str, int] = {}
    for _, u in files:
        e = re.search(FILE_RE, u, re.I).group(1).lower()
        ext[e] = ext.get(e, 0) + 1
    tab = [(a, u) for a, u in files if re.search(r"(?i)\.(xlsx?|csv|zip|ods|json|xml)(\?|$)", u)]
    heads = []
    for a, u in tab[:follow_files]:
        h = get(u, method="HEAD")
        heads.append({"u": u[-80:], "st": h.get("st") or h.get("err"), "ct": h.get("ct"), "len": h.get("len"), "lm": h.get("lm")})
    fol = follow(ll) if svc else None
    return {"follow": fol, "chars": len(p), "scripts": len(re.findall("(?i)<script", t)), "ext": ext, "tab_files": [u[-90:] for _, u in tab[:6]],
            "dl_links": [f"{a[:30]}|{u[-70:]}" for a, u in dl[:5]], "lic": [s[:240] for s in lic[:2]], "head": heads}


def dane(q, kind="datasets"):
    api = "https://api.dane.gov.pl/1.4"
    url = f"{api}/{kind}?q={urllib.parse.quote(q)}&per_page=15"
    r = get(url, 4000000, 75, robots_timeout=75)
    if "err" in r:
        fail(url, r)
        return {"err": r["err"][:70]}
    try:
        d = json.loads(r["body"].decode("utf-8", "replace"))
    except ValueError:
        return {"err": "not JSON"}
    rows = []
    for x in d.get("data", [])[:15]:
        at = x.get("attributes", {})
        inst = ((x.get("relationships", {}).get("institution", {}) or {}).get("data") or {}).get("id")
        row = {"id": x.get("id"), "t": (at.get("title") or "")[:70], "fmt": at.get("formats"), "lic": at.get("license_name") or at.get("license_condition_default_cc40"),
               "inst": inst, "mod": (at.get("modified") or at.get("data_date") or "")[:10], "freq": at.get("update_frequency")}
        if kind == "institutions":
            row = {"id": x.get("id"), "t": (at.get("title") or "")[:70], "n_ds": at.get("datasets_count") or at.get("published_datasets_count")}
        rows.append(row)
    print(f"=== DANE {kind} q={q} count={(d.get('meta') or {}).get('count')}", flush=True)
    for x in rows:
        print("  " + json.dumps(x, ensure_ascii=False), flush=True)
    end()
    return {"n": (d.get("meta") or {}).get("count"), "rows": rows[:5]}


def dane_ds(did):
    """One dataset: its resources and licence fields."""
    api = "https://api.dane.gov.pl/1.4"
    out = {}
    d = jget(f"{api}/datasets/{did}")
    if d:
        at = d.get("data", {}).get("attributes", {})
        print(f"=== DANE dataset {did}\n{json.dumps(at, ensure_ascii=False)[:3000]}\n=== END", flush=True)
        out = {k: at.get(k) for k in ("title", "license_name", "license_condition_default_cc40", "license_condition_responsibilities", "license_condition_source", "license_condition_modification", "license_condition_cc40_responsibilities", "update_frequency", "modified")}
    r = jget(f"{api}/datasets/{did}/resources?per_page=30")
    if r:
        print(f"=== DANE resources raw {did} {json.dumps(r, ensure_ascii=False)[:2500]}\n=== END", flush=True)
        res = []
        for x in r.get("data", [])[:30]:
            at = x.get("attributes", {})
            res.append({"t": (at.get("title") or "")[:60], "fmt": at.get("format"), "url": (at.get("link") or at.get("download_url") or "")[-80:], "date": (at.get("data_date") or at.get("modified") or "")[:10]})
        print(f"=== DANE resources {did}", flush=True)
        for x in res:
            print("  " + json.dumps(x, ensure_ascii=False), flush=True)
        end()
        out["n_res"] = (r.get("meta") or {}).get("count")
        out["res"] = res[:6]
    return out




def jinfo(u, show=2500):
    """GET a JSON (or other) API answer; print the head; return status, type, size, top keys and count."""
    r = get(u, 6000000, 90)
    if "err" in r:
        fail(u, r)
        return {"err": r["err"][:80]}
    b = r.pop("body")
    body = b.decode("utf-8", "replace")
    print(f"=== API {u}\nMETA {json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False)} read={len(b)}\nBODY {body[:show]}\n=== END", flush=True)
    out = {"st": r.get("st"), "ct": r.get("ct"), "len": len(b), "lm": r.get("lm")}
    try:
        d = json.loads(body)
        if isinstance(d, dict):
            out["keys"] = list(d)[:8]
            for k in ("count", "total", "@odata.count"):
                if k in d:
                    out["count"] = d[k]
            v = d.get("value") or d.get("data") or d.get("result")
            if isinstance(v, list):
                out["n"] = len(v)
                out["first"] = json.dumps(v[0], ensure_ascii=False)[:420] if v else None
        elif isinstance(d, list):
            out["n"] = len(d)
            out["first"] = json.dumps(d[0], ensure_ascii=False)[:420] if d else None
    except ValueError:
        out["head"] = re.sub(r"\s+", " ", plain(body)[:300])
        out["lic"] = [s[:300] for s in around(plain(body), LIC, 100, 300, 2)]
    return out


def attachments(url, limit=6):
    """gov.pl pages: list /attachment/ links and HEAD the first few (type, size, file name)."""
    got = page(url, text=2500)
    if not got:
        return {"err": ERR[-1].split(": ", 1)[-1][:90] if ERR else "failed"}
    p, ll, t = got
    att = [(a, u) for a, u in ll if "/attachment/" in u]
    for a, u in att[:60]:
        print(f"  ATT {a} | {u}", flush=True)
    files = [(a, u) for a, u in ll if re.search(FILE_RE, u, re.I)]
    for a, u in files[:30]:
        print(f"  FILE {a} | {u}", flush=True)
    end()
    heads = []
    for a, u in att[:limit]:
        h = get(u, method="HEAD")
        heads.append({"a": a[:45], "st": h.get("st") or h.get("err"), "ct": (h.get("ct") or "")[:30], "len": h.get("len"), "cd": (h.get("cd") or "")[-60:]})
    return {"chars": len(p), "n_att": len(att), "n_files": len(files), "titles": [a[:50] for a, _ in att[:14]], "heads": heads}


def short(d):
    if isinstance(d, dict):
        d = {k: v for k, v in d.items() if v not in (None, [], {}, "")}
        if "lic" in d:
            d["lic"] = [s[:230] for s in d["lic"]]
    return d


def main():
    res: dict[str, dict] = {}
    # GL-031 ITU: the producer's host again, then official republishers (World Bank Data360 ITU_DH, ITU statistics pages).
    itu = res["GL-031"] = {}
    itu["datahub_about"] = short(probe_page("https://datahub.itu.int/about", 2500))
    itu["itu_stat_page"] = short(probe_page("https://www.itu.int/en/ITU-D/Statistics/Pages/stat/default.aspx", 2500, 4))
    itu["d360_ind"] = jinfo("https://data360api.worldbank.org/data360/indicators?datasetId=ITU_DH")
    itu["d360_data_pol"] = jinfo("https://data360api.worldbank.org/data360/data?DATABASE_ID=ITU_DH&REF_AREA=POL&skip=0")
    itu["wb_catalog"] = jinfo("https://datacatalogapi.worldbank.org/ddhxext/DatasetView?dataset_unique_id=0067055", 6000)
    emit("parked5 GL-031 ITU", itu)
    # GL-032 WIPO
    w = res["GL-032"] = {}
    w["about"] = short(probe_page("https://www.wipo.int/en/web/ip-statistics/about", 3000))
    w["ipstats"] = short(probe_page("https://www.wipo.int/en/web/ip-statistics", 2500, 4))
    w["www3"] = short(probe_page("https://www3.wipo.int/ipstats/", 2500, 4))
    w["d360_wipo"] = jinfo("https://data360api.worldbank.org/data360/indicators?datasetId=WIPO_IPS")
    emit("parked5 GL-032 WIPO", w)
    # GL-033 UNODC
    u = res["GL-033"] = {}
    u["portal"] = short(probe_page("https://data.unodc.org/", 3000, 4))
    u["dataunodc"] = short(probe_page("https://dataunodc.un.org/dp-intentional-homicide-victims", 2500, 4))
    u["legal"] = short(probe_page("https://www.unodc.org/unodc/legal.html", 2500, 0))
    emit("parked5 GL-033 UNODC", u)
    # GL-034 IEA, GL-035 IRENA
    e = res["GL-034"] = {}
    e["noncc"] = short(probe_page("https://www.iea.org/terms/terms-of-use-for-non-cc-material", 2500, 0))
    e["mes"] = short(probe_page("https://www.iea.org/data-and-statistics/data-product/monthly-electricity-statistics", 2500, 4))
    i = res["GL-035"] = {}
    i["downloads"] = short(probe_page("https://www.irena.org/Data/Downloads/IRENASTAT", 2500, 4))
    i["pxweb_api"] = jinfo("https://pxweb.irena.org/api/v1/en/IRENASTAT")
    i["terms"] = short(probe_page("https://www.irena.org/terms-and-conditions", 2500, 0))
    emit("parked5 GL-034 IEA GL-035 IRENA", {"GL-034": e, "GL-035": i})
    # GL-040 UNIDO, PLS-008 REGON
    n = res["GL-040"] = {}
    n["free"] = short(probe_page("https://stat.unido.org/content/general/databases-for-free", 3000, 4))
    n["terms"] = short(probe_page("https://stat.unido.org/content/legal/Terms%20and%20Conditions", 3000, 0))
    n["root"] = short(probe_page("https://stat.unido.org/", 1500, 0))
    g = res["PLS-008"] = {"page": short(probe_page("https://api.stat.gov.pl/Home/RegonApi", 3000, 0))}
    emit("parked5 GL-040 UNIDO PLS-008 REGON", {"GL-040": n, "PLS-008": g})
    # PLS-018 GIS, PLS-033 KOWR: gov.pl attachments
    res["PLS-018"] = {"gis": attachments("https://www.gov.pl/web/gis/raport---stan-sanitarny-kraju", 8)}
    emit("parked5 PLS-018 GIS", res["PLS-018"])
    k = res["PLS-033"] = {}
    k["zboza"] = attachments("https://www.gov.pl/web/kowr/Rynek-zboz", 4)
    k["mieso"] = attachments("https://www.gov.pl/web/kowr/Rynek-miesa", 4)
    emit("parked5 PLS-033 KOWR", k)
    # TP-007 PAP: one read of the feed (no workaround), dane.gov.pl for the two gov.pl report families
    p = res["TP-007"] = {}
    p["rss"] = jinfo("https://www.pap.pl/rss.xml", 1500)
    p["robots"] = ROBOTS_TXT.get("https://www.pap.pl", "")[:400]
    d = {"stan_sanitarny": dane("stan sanitarny"), "kowr": dane("KOWR"), "kowr_inst": dane("Krajowy Ośrodek Wsparcia Rolnictwa", "institutions")}
    emit("parked5 TP-007 PAP and dane.gov.pl", {"TP-007": p, "dane": d})
    emit("parked5 robots errors", {"requests": COUNT[0], "dead": DEAD, "robots": {h: v[:160] for h, v in ROBOTS_TXT.items()}, "errors": ERR[:25]})


if __name__ == "__main__":
    main()
