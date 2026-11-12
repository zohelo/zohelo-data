"""Read-only probe for the rework of research PRs #2892 and #2895 (EX-001, EX-002, PLG-016, PLG-034), 12 Nov 2026.

GET and HEAD only, one request at a time, at least a second apart per host, robots.txt honoured (generic agent and
Claude agents); a refused, walled or timed-out host is reported and not asked again in the run. Looks for the
legitimate alternative routes the review named: other loaded sources that hold UPRP counts (GUS BDL, WDI,
Eurostat), UPRP annual-report and bulletin PDFs, dane.gov.pl, the KNF pages and register applications, the EU
registers (EBA, ESMA, EIOPA), and the RCL hosts with a longer timeout plus gov.pl and the Sejm API. Page text goes
to the job log; at most ten notices carry the summary. With arguments: fetch only the given addresses.
"""

import html
import io
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import zipfile
import zlib

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
        except Exception as e:  # noqa: BLE001
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
    except Exception as e:  # noqa: BLE001
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


def shape(v, depth=0):
    if isinstance(v, dict):
        if depth > 3:
            return f"dict({len(v)})"
        return {str(k)[:40]: shape(x, depth + 1) for k, x in list(v.items())[:40]}
    if isinstance(v, list):
        return [f"list({len(v)})", shape(v[0], depth + 1)] if v else "list(0)"
    return type(v).__name__


# ---------------------------------------------------------------- EX-001: other sources that hold UPRP counts


def bdl():
    base = "https://bdl.stat.gov.pl/api/v1"
    found, subjects = {}, {}
    for word in ("patent", "wynalaz", "wzory użytkowe", "znaki towarowe", "wzory przemysłowe"):
        d = jget(f"{base}/variables/search?name={urllib.parse.quote(word)}&page-size=100&format=json&lang=pl")
        if not d:
            continue
        res = d.get("results", [])
        print(f"=== BDL search '{word}': total {d.get('totalRecords')}", flush=True)
        for v in res:
            name = " / ".join(str(v.get(k)) for k in ("n1", "n2", "n3", "n4", "n5") if v.get(k))
            print(f"  var {v.get('id')} subject {v.get('subjectId')} level {v.get('level')} unit {v.get('measureUnitName')} | {name}", flush=True)
            found[v.get("id")] = {"s": v.get("subjectId"), "lvl": v.get("level"), "u": v.get("measureUnitName"), "n": name[:110]}
        end()
    for sid in sorted({v["s"] for v in found.values() if v["s"]})[:8]:
        d = jget(f"{base}/subjects/{sid}?format=json&lang=pl")
        if d:
            subjects[sid] = {"name": d.get("name"), "parent": d.get("parentId"), "levels": d.get("levels"), "upd": d.get("lastUpdate") or d.get("description")}
            print(f"=== BDL subject {sid}: {json.dumps(d, ensure_ascii=False)[:600]}\n=== END", flush=True)
            pid = d.get("parentId")
            if pid and pid not in subjects:
                d2 = jget(f"{base}/subjects/{pid}?format=json&lang=pl")
                if d2:
                    subjects[pid] = {"name": d2.get("name"), "parent": d2.get("parentId")}
    data = {}
    picked = [i for i, v in found.items() if re.search(r"(?i)patent|wynalaz|ochronn|znak|wzor", v["n"])][:6]
    for vid in picked:
        for lvl in (0, 2):
            d = jget(f"{base}/data/by-variable/{vid}?unit-level={lvl}&page-size=100&format=json&lang=pl")
            if not d:
                continue
            res = d.get("results", [])
            years = sorted({x.get("year") for u in res for x in u.get("values", [])})
            sample = [(u.get("name"), [(x.get("year"), x.get("val")) for x in u.get("values", [])[-2:]]) for u in res[:2]]
            data[f"{vid}@{lvl}"] = {"units": len(res), "years": f"{years[0]}-{years[-1]}" if years else None, "n": len(years), "sample": sample}
            print(f"=== BDL data var {vid} level {lvl}: {json.dumps(data[f'{vid}@{lvl}'], ensure_ascii=False)}\n=== END", flush=True)
    N["bdl"] = {"vars": {k: found[k] for k in list(found)[:14]}, "n_vars": len(found), "subjects": subjects, "data": data}


def wdi():
    out = {}
    for code in ("IP.PAT.RESD", "IP.PAT.NRES", "IP.TMK.RESD", "IP.TMK.NRES", "IP.TMK.TOTL", "IP.IDS.RESD", "IP.IDS.NRES"):
        d = jget(f"https://api.worldbank.org/v2/country/POL/indicator/{code}?format=json&per_page=100")
        if not d or len(d) < 2 or not d[1]:
            out[code] = "no data"
            continue
        vals = sorted((x["date"], x["value"]) for x in d[1] if x.get("value") is not None)
        out[code] = {"name": d[1][0]["indicator"]["value"][:60], "src": d[0].get("sourceid"), "upd": d[0].get("lastupdated"), "years": f"{vals[0][0]}-{vals[-1][0]}" if vals else None, "last": vals[-1] if vals else None}
    print(f"=== WDI POL {json.dumps(out, ensure_ascii=False)}\n=== END", flush=True)
    N["wdi"] = out


def eurostat():
    r = get("https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en", 30000000, 90)
    if "err" in r:
        fail("eurostat toc", r)
        return
    rows = []
    for line in r["body"].decode("utf-8", "replace").splitlines():
        c = [x.strip().strip('"') for x in line.split("\t")]
        if len(c) > 6 and c[2] in ("dataset", "table") and re.search(r"(?i)patent|trade ?mark|community design|intellectual property|design application", c[0]):
            rows.append({"code": c[1], "title": c[0][:90], "upd": c[3], "start": c[5], "end": c[6]})
    seen, uniq = set(), []
    for x in rows:
        if x["code"] not in seen:
            seen.add(x["code"])
            uniq.append(x)
    print(f"=== EUROSTAT TOC bytes={len(r['body'])} matches={len(uniq)}", flush=True)
    for x in uniq:
        print("  " + json.dumps(x, ensure_ascii=False), flush=True)
    end()
    N["eurostat"] = {"n": len(uniq), "rows": [f"{x['code']} {x['title'][:60]} {x['start']}-{x['end']}" for x in uniq[:22]]}


# ---------------------------------------------------------------- UPRP files


def pdf_info(b):
    out = {"bytes": len(b), "magic": b[:8].decode("latin-1"), "pages_marked": len(re.findall(rb"/Type\s*/Page(?![s\w])", b))}
    n = ok = text_streams = literal = hexs = images = 0
    chunks = []
    for m in re.finditer(rb"stream\r?\n(.*?)endstream", b, re.S):
        n += 1
        try:
            d = zlib.decompressobj().decompress(m.group(1))
        except zlib.error:
            continue
        ok += 1
        out["pages_marked"] += len(re.findall(rb"/Type\s*/Page(?![s\w])", d))
        if b"BT" in d and (b"Tj" in d or b"TJ" in d):
            text_streams += 1
            lit = re.findall(rb"\(((?:\\.|[^\\()])*)\)", d)
            literal += len(lit)
            hexs += len(re.findall(rb"<[0-9A-Fa-f]{4,}>", d))
            if len(chunks) < 4000:
                chunks.append(b"".join(lit).decode("latin-1", "replace"))
    images = len(re.findall(rb"/Subtype\s*/Image", b))
    joined = " ".join(chunks)
    out.update({"streams": n, "inflated": ok, "text_streams": text_streams, "literal_strings": literal, "hex_strings": hexs, "images": images})
    out["voivodeship_words"] = {w: len(re.findall(w, joined, re.I)) for w in ("mazowieck", "podlask", "opolsk", "lubusk")}
    out["sample"] = [s[:260] for s in around(joined, r"mazowieck", 120, 260, 2)]
    out["text_head"] = re.sub(r"\s+", " ", joined[:400])
    return out


def uprp():
    out = {}
    got = page("https://uprp.gov.pl/pl/publikacje/raport-roczny-uprp", text=1500)
    if got:
        pdfs = [(a, u) for a, u in got[1] if re.search(r"\.pdf(\?|$)", u, re.I)]
        other = [(a, u) for a, u in got[1] if re.search(r"\.(xlsx?|csv|zip|ods)(\?|$)", u, re.I)]
        for a, u in pdfs:
            print(f"  PDF {a} | {u}", flush=True)
        end()
        heads = []
        for a, u in pdfs[:34]:
            h = get(u, method="HEAD")
            heads.append({"a": a[:30], "st": h.get("st") or h.get("err"), "len": h.get("len"), "lm": h.get("lm")})
        print(f"=== UPRP report HEAD {json.dumps(heads, ensure_ascii=False)}\n=== END", flush=True)
        sizes = [int(h["len"]) for h in heads if h.get("len")]
        out["annual_report"] = {"pdf_links": len(pdfs), "other_file_links": len(other), "head_200": sum(1 for h in heads if h["st"] == 200), "bytes_total": sum(sizes), "first": pdfs[0][1][-60:] if pdfs else None, "last": pdfs[-1][1][-60:] if pdfs else None, "lm_newest": heads[0].get("lm") if heads else None}
        newest = next((u for a, u in pdfs if "2025" in u + a), pdfs[0][1] if pdfs else None)
        if newest:
            r = get(newest, 90000000, 120)
            if "err" in r:
                fail(newest, r)
            else:
                info = pdf_info(r["body"])
                print(f"=== UPRP PDF {newest}\n{json.dumps(info, ensure_ascii=False)}\n=== END", flush=True)
                out["newest_pdf"] = {"u": newest[-50:], **{k: info[k] for k in ("bytes", "pages_marked", "text_streams", "literal_strings", "hex_strings", "images", "voivodeship_words")}, "sample": [s[:200] for s in info["sample"][:1]]}
    got = page("https://uprp.gov.pl/pl/publikacje/biuletyn-i-wiadomosci-uprp", n=20000000, timeout=120, text=1200)
    if got:
        fl = [(a, u) for a, u in got[1] if re.search(FILE_RE, u, re.I)]
        ext: dict[str, int] = {}
        for _, u in fl:
            e = re.search(FILE_RE, u, re.I).group(1).lower()
            ext[e] = ext.get(e, 0) + 1
        years = sorted(set(re.findall(r"(?:19|20)\d\d", " ".join(u for _, u in fl))))
        kinds = {k: sum(1 for a, u in fl if re.search(k, a + u, re.I)) for k in ("BUP", "WUP", "znaki", "wynalaz")}
        print(f"FILES {len(fl)} ext={ext} years={years[:3]}..{years[-3:]} kinds={kinds}", flush=True)
        for a, u in fl[:12] + fl[-6:]:
            print(f"  FILE {a} | {u}", flush=True)
        end()
        heads = []
        for a, u in fl[:3]:
            h = get(u, method="HEAD")
            heads.append({"u": u[-70:], "st": h.get("st") or h.get("err"), "len": h.get("len"), "lm": h.get("lm")})
        out["bulletins"] = {"page_bytes": len(got[2]), "file_links": len(fl), "ext": ext, "years": [years[0], years[-1]] if years else None, "kinds": kinds, "head": heads}
    N["uprp"] = out


# ---------------------------------------------------------------- dane.gov.pl


def dane():
    out = {}
    api = "https://api.dane.gov.pl/1.4"
    for label, q in (("uprp_inst", "institutions?q=patentowy"), ("uprp", "datasets?q=patentowy&per_page=20"), ("patent", "datasets?q=patent&per_page=20"), ("knf_inst", "institutions?q=nadzoru%20finansowego"), ("knf", "datasets?q=nadzoru%20finansowego&per_page=20"), ("rcl_inst", "institutions?q=legislacji"), ("rpl", "datasets?q=proces%20legislacyjny&per_page=20")):
        r = get(f"{api}/{q}", 3000000, 75, robots_timeout=75)
        if "err" in r:
            fail(f"{api}/{q}", r)
            out[label] = r["err"][:70]
            continue
        try:
            d = json.loads(r["body"].decode("utf-8", "replace"))
        except ValueError:
            out[label] = "not JSON"
            continue
        rows = []
        for x in d.get("data", [])[:20]:
            at = x.get("attributes", {})
            rows.append({"id": x.get("id"), "t": (at.get("title") or "")[:80], "fmt": at.get("formats"), "lic": at.get("license_name") or at.get("license_condition_default_cc40"), "inst": ((x.get("relationships", {}).get("institution", {}) or {}).get("data") or {}).get("id"), "mod": (at.get("modified") or "")[:10]})
        print(f"=== DANE {q} count={(d.get('meta') or {}).get('count')}", flush=True)
        for x in rows:
            print("  " + json.dumps(x, ensure_ascii=False), flush=True)
        if d.get("data"):
            print(f"ATTR-KEYS {sorted(d['data'][0].get('attributes', {}).keys())}", flush=True)
        end()
        out[label] = {"count": (d.get("meta") or {}).get("count"), "rows": rows[:6]}
    N["dane"] = out


# ---------------------------------------------------------------- KNF


def app_shell(url, bundles=4):
    """A JavaScript application page: its scripts, and the address-like strings in its bundles."""
    got = page(url, text=400)
    if not got:
        return {"err": ERR[-1][-70:] if ERR else "failed"}
    t = got[2]
    host = urllib.parse.urlsplit(url).netloc
    srcs = [urllib.parse.urljoin(url, html.unescape(s)) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', t)]
    srcs = [s for s in srcs if urllib.parse.urlsplit(s).netloc == host]
    hints: list[str] = []
    for s in sorted(srcs, key=lambda x: (not re.search(r"main|app|index|chunk", x), x))[:bundles]:
        r = get(s, 8000000, 60)
        if "err" in r:
            fail(s, r)
            continue
        js = r["body"].decode("utf-8", "replace")
        for m in re.findall(r"[\"'`]([^\"'`\s]{3,140})[\"'`]", js):
            if re.search(r"(?i)(^|/)(api|rest|services?|export|download|pobierz|search|szukaj|v\d)(/|$|\?)|\.(json|csv|xlsx?)($|\?)|^https?://[^/]*(knf|eba|europa)", m) and m not in hints and not re.search(r"\.(js|css|png|svg|woff2?)($|\?)|w3\.org|angular|reactjs|github", m):
                hints.append(m)
        print(f"  BUNDLE {s} bytes={len(js)}", flush=True)
    print(f"HINTS {len(hints)}", flush=True)
    for h in hints[:90]:
        print(f"  HINT {h}", flush=True)
    end()
    return {"chars": len(got[0]), "scripts": len(srcs), "hints": len(hints), "sample": hints[:9]}


def knf():
    start = "https://www.knf.gov.pl/podmioty"
    queue, seen, files, pages_read = [(start, 0)], {start}, {}, 0
    while queue and pages_read < 42:
        url, depth = queue.pop(0)
        got = page(url, text=500)
        if not got:
            continue
        pages_read += 1
        for a, u in got[1]:
            sp = urllib.parse.urlsplit(u)
            if re.search(r"\.(xlsx?|csv|zip|json|xml|ods)(\?|$)", u, re.I) or (re.search(r"komponenty/img/", u) and not re.search(r"\.(png|jpe?g|gif|svg)(\?|$)", u, re.I)):
                if u not in files:
                    files[u] = (a, url)
                    print(f"  FILE {a} | {u}", flush=True)
            elif sp.netloc == "www.knf.gov.pl" and sp.path.startswith("/podmioty") and depth < 2 and u not in seen and "?" not in u:
                seen.add(u)
                queue.append((u, depth + 1))
            elif re.search(r"//[a-z0-9-]+\.knf\.gov\.pl", u) and "www.knf" not in u and u not in seen:
                seen.add(u)
                print(f"  SUBSITE {a} | {u}", flush=True)
        end()
    ext: dict[str, int] = {}
    for u in files:
        m = re.search(r"\.([A-Za-z0-9]{2,5})(\?|$)", u)
        e = m.group(1).lower() if m else "?"
        ext[e] = ext.get(e, 0) + 1
    data_files = [(a, u, src) for u, (a, src) in files.items() if re.search(r"\.(xlsx?|csv|zip|json|xml|ods)(\?|$)", u, re.I)]
    heads = []
    for a, u, src in data_files[:8]:
        h = get(u, method="HEAD")
        heads.append({"a": a[:45], "u": u[-60:], "on": src[-40:], "st": h.get("st") or h.get("err"), "len": h.get("len"), "lm": h.get("lm")})
    print(f"=== KNF crawl pages={pages_read} left={len(queue)} files={len(files)} ext={ext}\nDATA-FILES {json.dumps(heads, ensure_ascii=False)}\n=== END", flush=True)
    N["knf_pages"] = {"pages": pages_read, "left": len(queue), "file_links": len(files), "ext": ext, "data_files": heads}
    apps = {}
    for u in ("https://rpu.knf.gov.pl/", "https://wpn.knf.gov.pl/", "https://pok.knf.gov.pl/", "https://rpkip.knf.gov.pl/index.html?type=RPK", "https://e-rup.knf.gov.pl/", "https://rdl.knf.gov.pl/"):
        apps[urllib.parse.urlsplit(u).netloc.split(".")[0]] = app_shell(u, 3)
    N["knf_apps"] = apps


def eu_registers():
    out = {}
    u = "https://euclid.eba.europa.eu/register/pir/registerDownload"
    r = get(u, 150000000, 180)
    if "err" in r:
        fail(u, r)
        out["eba_pir"] = r["err"][:80]
    else:
        b = r.pop("body")
        info = {k: v for k, v in r.items() if v}
        info["bytes"] = len(b)
        info["magic"] = b[:4].hex()
        doc = None
        try:
            if b[:2] == b"PK":
                z = zipfile.ZipFile(io.BytesIO(b))
                info["zip"] = [(i.filename, i.file_size) for i in z.infolist()][:5]
                doc = json.loads(z.read(z.infolist()[0]).decode("utf-8", "replace"))
            elif b.lstrip()[:1] in (b"{", b"["):
                doc = json.loads(b.decode("utf-8", "replace"))
        except (ValueError, zipfile.BadZipFile) as e:
            info["parse"] = str(e)[:80]
        if doc is not None:
            info["shape"] = json.dumps(shape(doc), ensure_ascii=False)[:700]
            rows = doc
            while isinstance(rows, list) and len(rows) < 5 and rows and isinstance(rows[-1], list):
                rows = rows[-1]
            if isinstance(rows, dict):
                rows = max((v for v in rows.values() if isinstance(v, list)), key=len, default=[])
            info["rows"] = len(rows)
            pl = [x for x in rows if re.search(r'"PL"|"POL"|Poland', json.dumps(x)[:3000])]
            info["rows_pl"] = len(pl)
            types: dict[str, int] = {}
            for x in pl:
                m = re.search(r'"(PSD_[A-Z_]+)"', json.dumps(x))
                k = m.group(1) if m else "?"
                types[k] = types.get(k, 0) + 1
            info["pl_types"] = types
            if pl:
                info["pl_row_shape"] = json.dumps(shape(pl[0]), ensure_ascii=False)[:900]
        print(f"=== EBA PIR {json.dumps(info, ensure_ascii=False)}\n=== END", flush=True)
        out["eba_pir"] = {k: info.get(k) for k in ("st", "ct", "cd", "lm", "bytes", "zip", "rows", "rows_pl", "pl_types")}
    out["eba_cir"] = app_shell("https://euclid.eba.europa.eu/register/cir/disclaimer", 4)
    q = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select?q=*:*&fq=ae_homeMemberState:%22POLAND%22&fq=type_s:parent&rows=2&wt=json&facet=true&facet.field=ae_entityTypeCode&facet.field=ae_competentAuthority&facet.field=ae_status&facet.mincount=1"
    d = jget(q)
    if d:
        resp = d.get("response", {})
        fac = (d.get("facet_counts") or {}).get("facet_fields", {})
        docs = resp.get("docs", [])
        print(f"=== ESMA upreg PL numFound={resp.get('numFound')}\nFACETS {json.dumps(fac, ensure_ascii=False)[:1500]}\nDOC {json.dumps(docs[:1], ensure_ascii=False)[:1800]}\n=== END", flush=True)
        out["esma_upreg_pl"] = {"numFound": resp.get("numFound"), "types": fac.get("ae_entityTypeCode"), "status": fac.get("ae_status"), "keys": sorted(docs[0].keys())[:40] if docs else None}
    d = jget("https://registers.esma.europa.eu/solr/esma_registers_upreg/select?q=*:*&rows=0&wt=json&facet=true&facet.field=ae_homeMemberState&facet.limit=60&facet.mincount=1")
    if d:
        fac = (d.get("facet_counts") or {}).get("facet_fields", {}).get("ae_homeMemberState", [])
        print(f"=== ESMA upreg all numFound={d.get('response', {}).get('numFound')} homeMemberState={json.dumps(fac, ensure_ascii=False)[:1500]}\n=== END", flush=True)
        out["esma_states"] = [x for x in fac if isinstance(x, str) and re.search(r"(?i)pol|^PL$", x)] or fac[:10]
    got = page("https://register.eiopa.europa.eu/registers/register-of-insurance-undertakings", text=1500)
    if got:
        fl = [(a, u) for a, u in got[1] if re.search(r"(?i)\.(csv|xlsx?|zip|json)(\?|$)|export|download", u + " " + a)]
        for a, u in fl[:20]:
            print(f"  LINK {a} | {u}", flush=True)
        end()
        out["eiopa"] = {"chars": len(got[0]), "export_links": [(a[:40], u[-90:]) for a, u in fl[:5]]}
    N["eu"] = out
    legal = {}
    for name, u in (("eba", "https://www.eba.europa.eu/find-out-about-us/legal-notice"), ("esma", "https://www.esma.europa.eu/legal-notice"), ("eiopa", "https://www.eiopa.europa.eu/legal-notice_en"), ("epo_bulk", "https://www.epo.org/en/searching-for-patents/data/bulk-data-sets")):
        got = page(u, text=9000)
        if got:
            end()
            legal[name] = [k[:560] for k in around(got[0], r"reus|reproduc|copyright|free of charge|licen[cs]e|subscription|register", 100, 460, 2)]
    N["legal"] = legal


# ---------------------------------------------------------------- PLG-034


def rcl():
    out = {}
    for host in ("https://legislacja.rcl.gov.pl", "https://legislacja.gov.pl"):
        t0 = time.time()
        got = page(host + "/", timeout=90, text=3000, robots_timeout=90)
        key = urllib.parse.urlsplit(host).netloc
        if not got:
            out[key] = {"err": ERR[-1][-90:], "seconds": round(time.time() - t0)}
            continue
        feeds = re.findall(r'(?is)<link\b[^>]*type=["\']application/(?:rss|atom)[^>]*>', got[2])
        ll = got[1]
        for a, u in ll[:80]:
            print(f"  LINK {a} | {u}", flush=True)
        end()
        foot = around(got[0], r"ponown|prawa autorskie|licencj|regulamin|©|copyright", 100, 400, 3)
        out[key] = {"chars": len(got[0]), "robots": ROBOTS_TXT.get(host, "")[:200], "feeds": len(feeds), "links": len(ll), "export_links": [u for a, u in ll if re.search(r"(?i)rss|atom|export|csv|xml|json|api", u + a)][:5], "footer": [f[:300] for f in foot[:2]]}
        lst = next((u for a, u in ll if "/lista" in u), None)
        if lst:
            g2 = page(lst, timeout=90, text=2500)
            if g2:
                end()
                out[key]["list_page"] = {"u": lst[-60:], "chars": len(g2[0]), "tables": len(re.findall("(?i)<table", g2[2])), "project_links": len({u for a, u in g2[1] if "/projekt/" in u}), "export": [u for a, u in g2[1] if re.search(r"(?i)rss|export|csv|xml|json", u + a)][:4]}
    got = page("https://www.gov.pl/web/premier/wplip-rm", text=2500)
    if got:
        fl = [(a, u) for a, u in got[1] if re.search(FILE_RE + r"|/attachment/|rejestr|register", u, re.I)]
        for a, u in fl[:25]:
            print(f"  LINK {a} | {u}", flush=True)
        hints = sorted(set(re.findall(r"[\"']([^\"'\s]*(?:/api/|registers?/)[^\"'\s]{0,90})[\"']", got[2])))[:20]
        print(f"HINTS {hints}", flush=True)
        end()
        out["gov_wykaz"] = {"chars": len(got[0]), "tables": len(re.findall("(?i)<table", got[2])), "links": [(a[:40], u[-80:]) for a, u in fl[:6]], "hints": hints[:6], "text": got[0][:300]}
    d = jget("https://api.sejm.gov.pl/sejm/term10/processes?limit=3&sort_by=-changeDate")
    if d:
        rows = d if isinstance(d, list) else d.get("items", [])
        keys = sorted({k for x in rows for k in x})
        print(f"=== SEJM processes keys={keys}\nROW {json.dumps(rows[:1], ensure_ascii=False)[:1500]}\n=== END", flush=True)
        out["sejm_processes"] = {"keys": keys, "rcl": {k: rows[0].get(k) for k in keys if "rcl" in k.lower()} if rows else None}
    N["rcl"] = out


# ---------------------------------------------------------------- second pass ("more")


def excerpts(url, pattern, n=3000000, limit=6, after=600):
    r = get(url, n, 60)
    if "err" in r:
        fail(url, r)
        return None, []
    t = r["body"].decode("utf-8", "replace")
    p = plain(t)
    ex = around(p, pattern, 200, after, limit)
    print(f"=== EXCERPTS {url} final={r.get('final')} chars={len(p)}", flush=True)
    for e in ex:
        print(f"  … {e}", flush=True)
    print(f"TAIL {p[-1800:]}\n=== END", flush=True)
    return t, ex


def more_bdl():
    base = "https://bdl.stat.gov.pl/api/v1"
    out = {}
    d = jget(f"{base}/subjects?parent-id=G483&format=json&lang=pl&page-size=100")
    if d:
        out["G483_children"] = [(x.get("id"), x.get("name")) for x in d.get("results", [])]
    d = jget(f"{base}/variables?subject-id=P3005&format=json&lang=pl&page-size=100")
    if d:
        out["P3005_variables"] = [(x.get("id"), " / ".join(str(x.get(k)) for k in ("n1", "n2", "n3") if x.get(k))[:90], x.get("measureUnitName"), x.get("level")) for x in d.get("results", [])]
    print(f"=== BDL more {json.dumps(out, ensure_ascii=False)}\n=== END", flush=True)
    N["bdl"] = out


def more_knf():
    start = "https://www.knf.gov.pl/podmioty"
    queue, seen, pages, subsites = [(start, 0)], {start}, [], {}
    dumped = 0
    while queue and len(pages) < 135:
        url, depth = queue.pop(0)
        r = get(url)
        if "err" in r:
            fail(url, r)
            continue
        t = r["body"].decode("utf-8", "replace")
        main = re.search(r"(?is)<main\b.*?</main>", t)
        mt = main.group(0) if main else t
        tables = re.findall(r"(?is)<table\b.*?</table>", mt)
        rows = len(re.findall(r"(?i)<tr\b", mt))
        pager = sorted({u for a, u in links(mt, url) if re.search(r"(?i)[?&](page|p|strona|start|offset)=", u)})[:3]
        upd = re.findall(r"(?i)(?:stan na|aktualizac\w+|data aktualizacji)[^<]{0,40}", plain(mt))[:1]
        short = url.replace("https://www.knf.gov.pl/podmioty", "") or "/"
        pages.append({"p": short[-70:], "t": len(tables), "tr": rows, "pg": len(pager), "upd": upd[0][:50] if upd else None})
        print(f"=== KNF {short} depth={depth} tables={len(tables)} tr={rows} pager={pager} upd={upd}", flush=True)
        if tables and dumped < 6 and re.search(r"spolek_akcyjnych$|banki_spoldzielcze$|spolki_akcyjnej$|emerytaln|Firmy_inw|kas_spoldz", url):
            dumped += 1
            for tb in tables[:3]:
                print(f"  TABLE {plain(tb)[:700]}", flush=True)
            heads = [plain(h)[:40] for h in re.findall(r"(?is)<th\b.*?</th>", mt)[:12]]
            print(f"  TH {heads}", flush=True)
        for a, u in links(t, url):
            sp = urllib.parse.urlsplit(u)
            if sp.netloc == "www.knf.gov.pl" and sp.path.startswith("/podmioty") and depth < 3 and u not in seen and "?" not in u:
                seen.add(u)
                queue.append((u, depth + 1))
            elif re.search(r"//[a-z0-9-]+\.knf\.gov\.pl", u) and "www.knf" not in u:
                subsites.setdefault(sp.netloc, a[:60])
        end()
    with_tables = [x for x in pages if x["t"]]
    print(f"=== KNF more pages={len(pages)} left={len(queue)} with_tables={len(with_tables)} tr_total={sum(x['tr'] for x in with_tables)}\nSUBSITES {json.dumps(subsites, ensure_ascii=False)}\n=== END", flush=True)
    N["knf_pages"] = {"pages": len(pages), "left": len(queue), "with_tables": len(with_tables), "tr_total": sum(x["tr"] for x in with_tables), "paged": sum(1 for x in pages if x["pg"])}
    N["knf_tables"] = [f"{x['p'].split('/')[-1][:44]}:{x['t']}/{x['tr']}" for x in with_tables][:70]
    apps = {}
    lib = r"(?i)jquery|bootstrap|datatables|popper|moment|select2|polyfill|recaptcha|font|\.min\.js"
    for host in sorted(subsites):
        u = f"https://{host}/"
        r = get(u, 2000000, 40)
        if "err" in r:
            apps[host] = r["err"][:50]
            fail(u, r)
            continue
        t = r["body"].decode("utf-8", "replace")
        srcs = [urllib.parse.urljoin(u, html.unescape(x)) for x in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', t)]
        own = [x for x in srcs if urllib.parse.urlsplit(x).netloc == host and not re.search(lib, x)]
        info = {"st": r["st"], "chars": len(plain(t)), "scripts": len(srcs), "own": [x.rsplit("/", 1)[-1][:30] for x in own[:4]], "captcha": bool(re.search(r"(?i)recaptcha|captcha", t)), "wall": bool(re.search(WALL, t[:30000]))}
        print(f"=== APP {u} {json.dumps(info, ensure_ascii=False)}\nSCRIPTS {srcs}", flush=True)
        calls = []
        if host.split(".")[0] in ("e-rup", "rpkip", "wpn", "rdl", "pok", "zasi", "dystr"):
            for x in own[:3]:
                j = get(x, 3000000, 40)
                if "err" in j:
                    fail(x, j)
                    continue
                js = j["body"].decode("utf-8", "replace")
                info["captcha"] = info["captcha"] or bool(re.search(r"(?i)recaptcha|captcha", js))
                for m in re.finditer(r"(?i)\$\.(?:ajax|get|post|getJSON)\s*\(|\bfetch\s*\(|[\"']?url[\"']?\s*:|XMLHttpRequest|buttons\s*:", js):
                    c = re.sub(r"\s+", " ", js[max(0, m.start() - 60) : m.start() + 260])
                    if c not in calls:
                        calls.append(c)
                print(f"  OWN-JS {x} bytes={len(js)}", flush=True)
            for c in calls[:14]:
                print(f"  CALL {c}", flush=True)
            info["calls"] = [re.sub(r"^.{0,60}?(?=\$\.|fetch|url)", "", c)[:110] for c in calls[:3]]
        end()
        apps[host.split(".")[0]] = info
    N["knf_apps"] = apps


def csv_profile(url):
    import csv

    csv.field_size_limit(10**8)
    h = get(url, method="HEAD")
    r = get(url, 40000000, 180)
    if "err" in r:
        fail(url, r)
        return {"head": h.get("st") or h.get("err"), "err": r["err"][:80]}
    b = r["body"]
    for enc in ("utf-8-sig", "cp1250"):
        try:
            t = b.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        t, enc = b.decode("utf-8", "replace"), "utf-8?"
    first = t.split("\n", 1)[0]
    delim = max(";,\t|", key=first.count)
    rows = list(csv.reader(io.StringIO(t), delimiter=delim))
    header, body = rows[0], [x for x in rows[1:] if x]
    cols = {}
    for i, name in enumerate(header[:40]):
        vals = [x[i].strip() for x in body if i < len(x)]
        filled = [v for v in vals if v]
        dist = sorted(set(filled))
        dates = sorted(v[:10] for v in filled if re.match(r"\d{4}-\d\d-\d\d|\d\d\.\d\d\.\d{4}", v))
        c = {"filled": len(filled), "distinct": len(dist), "maxlen": max((len(v) for v in filled), default=0)}
        if dist and len(dist) <= 25:
            c["values"] = [v[:60] for v in dist[:25]]
        elif dist:
            c["sample"] = [v[:80] for v in dist[:2]]
        if dates:
            c["dates"] = [dates[0], dates[-1], len(dates)]
        cols[name.strip()[:50]] = c
    info = {"head": h.get("st") or h.get("err"), "len": h.get("len"), "lm": h.get("lm"), "etag": h.get("etag"), "ct": r["ct"], "cd": r.get("cd"), "bytes": len(b), "encoding": enc, "delimiter": delim, "columns": len(header), "rows": len(body), "widths": sorted({len(x) for x in body})[:6]}
    print(f"=== CSV {url}\n{json.dumps(info, ensure_ascii=False)}\nHEADER {header}", flush=True)
    for k, v in cols.items():
        print(f"  COL {k}: {json.dumps(v, ensure_ascii=False)[:900]}", flush=True)
    for x in body[:2]:
        print(f"  ROW {json.dumps([v[:160] for v in x], ensure_ascii=False)[:2500]}", flush=True)
    end()
    info["header"] = [x.strip()[:28] for x in header[:26]]
    info["cols"] = {k[:24]: {a: b2 for a, b2 in v.items() if a in ("filled", "distinct", "dates")} for k, v in list(cols.items())[:26]}
    return info


def more_gov():
    out = {}
    base = "https://www.gov.pl/web/premier/wplip-rm"
    got = page(base, text=300)
    if got:
        p, ll, t = got
        print(f"FOOT {p[-2500:]}", flush=True)
        end()
        out["footer"] = [x[:420] for x in around(p, r"Creative Commons|licencj|CC BY|prawa autorskie", 160, 420, 2)]
        tb = re.search(r"(?is)<table\b.*?</table>", t)
        if tb:
            print(f"=== GOV TABLE {plain(tb.group(0))[:1500]}\n=== END", flush=True)
        csvs = [u for a, u in ll if "register-file" in u]
        if csvs:
            out["rm_csv"] = csv_profile(csvs[0])
        reuse = next((u for a, u in ll if re.search(r"(?i)ponowne", a + u)), None)
        if reuse:
            _, ex = excerpts(reuse, r"ponown\w+ wykorzyst|warunk|licencj|źródł|bezpłatn|komercyj", after=700)
            out["kprm_reuse"] = {"u": reuse[-70:], "ex": [e[:600] for e in ex[:3]]}
        for label, pat in (("prm", r"Wykaz prac legislacyjnych Prezesa"), ("archive", r"ARCHIWUM")):
            u = next((u for a, u in ll if re.search(pat, a)), None)
            if not u:
                continue
            g2 = page(u, text=300)
            if g2:
                end()
                c2 = [x for a, x in g2[1] if "register-file" in x]
                hd = get(c2[0], method="HEAD") if c2 else {}
                out[label] = {"u": u[-50:], "chars": len(g2[0]), "csv": c2[0][-45:] if c2 else None, "csv_head": hd.get("st") or hd.get("err"), "csv_len": hd.get("len")}
    N["gov"] = out
    rcl = {}
    for host in ("https://legislacja.rcl.gov.pl", "https://legislacja.gov.pl"):
        t0 = time.time()
        r = get(host + "/", 500000, 60, robots_timeout=60)
        rcl[urllib.parse.urlsplit(host).netloc] = {"result": r.get("st") or r.get("err"), "seconds": round(time.time() - t0), "utc": time.strftime("%H:%M", time.gmtime())}
    N["rcl_again"] = rcl


def more_legal():
    out = {}
    _, ex = excerpts("https://www.esma.europa.eu/about-esma/legal-notice-and-data-protection", r"copyright|reproduc|re-?use|acknowledg", after=700)
    out["esma"] = [e[:700] for e in ex[:3]]
    got = page("https://registers.esma.europa.eu/publication/", text=1500)
    if got:
        end()
        out["esma_registers_page"] = [e[:400] for e in around(got[0], r"disclaimer|copyright|legal notice|reproduc", 100, 400, 2)]
    t, ex = excerpts("https://www.epo.org/en/searching-for-patents/data/bulk-data-sets", r"free of charge|subscription|registration|terms and conditions|DOCDB", limit=5)
    out["epo_bulk"] = [e[:420] for e in ex[:3]]
    if t:
        doc = next((u for a, u in links(t, "https://www.epo.org/en/searching-for-patents/data/bulk-data-sets") if re.search(r"(?i)docdb", u)), None)
        if doc:
            _, ex = excerpts(doc, r"free of charge|subscri|regist|terms|national offices|coverage", limit=6)
            out["epo_docdb"] = {"u": doc[-60:], "ex": [e[:420] for e in ex[:3]]}
    r = get("https://www.tmdn.org/tmview/", 500000, 40)
    out["tmview"] = {"robots": ROBOTS_TXT.get("https://www.tmdn.org", "")[:200], "result": r.get("st") or r.get("err"), "chars": len(plain(r["body"].decode("utf-8", "replace"))) if "body" in r else None}
    N["legal"] = out


def more_uprp():
    out = {}
    r = get("https://uprp.gov.pl/sites/default/files/2026-08/Raport%20roczny%202025.pdf", 90000000, 120)
    if "err" in r:
        fail("raport 2025 pl", r)
    else:
        b = r["body"]
        info = pdf_info(b)
        chunks = []
        for m in re.finditer(rb"stream\r?\n(.*?)endstream", b, re.S):
            try:
                d = zlib.decompressobj().decompress(m.group(1))
            except zlib.error:
                continue
            if b"BT" in d and (b"Tj" in d or b"TJ" in d):
                chunks.append(b"".join(re.findall(rb"\(((?:\\.|[^\\()])*)\)", d)).decode("latin-1", "replace"))
        joined = " ".join(chunks)
        words = {w: len(re.findall(w, joined, re.I)) for w in ("znak", "wzor", "patent", "zg.osze", "udzielon", "krajow", "zagraniczn", "tabela", "razem")}
        ex = around(joined, r"znak\w* towarow", 150, 380, 3) + around(joined, r"wzor\w* przemys", 150, 380, 2)
        print(f"=== UPRP PDF pl 2025 {json.dumps({k: info[k] for k in ('bytes', 'pages_marked', 'text_streams', 'literal_strings', 'hex_strings', 'images')})}\nWORDS {words}", flush=True)
        for e in ex:
            print(f"  … {e}", flush=True)
        end()
        out["report_pl_2025"] = {"bytes": info["bytes"], "pages": info["pages_marked"], "text_streams": info["text_streams"], "words": words, "ex": [e[:300] for e in ex[:2]]}
    got = page("https://uprp.gov.pl/pl/publikacje/biuletyn-i-wiadomosci-uprp", n=20000000, timeout=120, text=200)
    if got:
        end()
        fl = [(a, u) for a, u in got[1] if re.search(r"\.pdf(\?|$)", u, re.I)]
        groups: dict[str, list] = {}
        for a, u in fl:
            m = re.search(r"/files/((?:bup|wup)[^/]*/\d{4}(?:/[^/\d][^/]*)?)", u, re.I)
            key = re.sub(r"\d{4}", "YYYY", urllib.parse.unquote(m.group(1))) if m else "other"
            groups.setdefault(key, []).append(u)
        out["bulletin_groups"] = {k: [len(v), min(re.findall(r"(?:19|20)\d\d", " ".join(v)) or ["?"]), max(re.findall(r"(?:19|20)\d\d", " ".join(v)) or ["?"])] for k, v in groups.items()}
        print(f"=== BULLETIN GROUPS {json.dumps(out['bulletin_groups'], ensure_ascii=False)}\n=== END", flush=True)
        picks = {}
        for kind, pat in (("bup_inventions", r"/bup/2026/(?!Znaki)"), ("wup", r"/wup/2026/")):
            u = next((u for a, u in fl if re.search(pat, u)), None)
            if not u:
                continue
            r = get(u, 90000000, 120)
            if "err" in r:
                fail(u, r)
                continue
            b = r["body"]
            info = pdf_info(b)
            text = []
            for m in re.finditer(rb"stream\r?\n(.*?)endstream", b, re.S):
                try:
                    d = zlib.decompressobj().decompress(m.group(1))
                except zlib.error:
                    continue
                text.append(d)
            allb = b" ".join(text)
            inid = {c: len(re.findall(rb"\(\s*\\?\(?" + c.encode() + rb"\\?\)?\s*\)|\\\(" + c.encode() + rb"\\\)", allb)) for c in ("21", "22", "51", "54", "71", "72", "73", "11")}
            picks[kind] = {"u": u[-60:], "bytes": info["bytes"], "pages": info["pages_marked"], "text_streams": info["text_streams"], "literal": info["literal_strings"], "hex": info["hex_strings"], "images": info["images"], "inid_counts": inid}
            print(f"=== BULLETIN {kind} {json.dumps(picks[kind], ensure_ascii=False)}\n=== END", flush=True)
        out["bulletin_samples"] = picks
    N["uprp"] = out


def more():
    for step in (more_bdl, more_gov, more_legal, more_uprp, more_knf):
        try:
            step()
        except Exception as e:  # noqa: BLE001
            ERR.append(f"step {step.__name__}: {type(e).__name__}: {str(e)[:120]}")
            print(f"=== STEP {step.__name__} FAILED {type(e).__name__}: {e}", flush=True)
    emit("probe fix2 1 robots", {h.split("//")[1]: t[:200] for h, t in ROBOTS_TXT.items()})
    emit("probe fix2 2 EX-001 GUS BDL group G483", N.get("bdl"))
    emit("probe fix2 3 UPRP annual report (pl) and bulletins", N.get("uprp"))
    emit("probe fix2 4 PLG-034 gov.pl work list CSV", (N.get("gov") or {}).get("rm_csv"))
    emit("probe fix2 5 PLG-034 gov.pl terms, other lists, RCL again", {k: v for k, v in (N.get("gov") or {}).items() if k != "rm_csv"} | {"rcl_again": N.get("rcl_again")})
    emit("probe fix2 6 legal: ESMA, EPO, TMview", N.get("legal"))
    emit("probe fix2 7 PLG-016 KNF fixed pages", {"summary": N.get("knf_pages"), "tables/rows": N.get("knf_tables")})
    emit("probe fix2 8 PLG-016 KNF register sites", N.get("knf_apps"))
    emit("probe fix2 9 errors and totals", {"requests": COUNT[0], "skipped_hosts": DEAD, "errors": ERR[:30]})


def emit(title, data):
    msg = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    b = msg.encode("utf-8")
    if len(b) > LIMIT:
        msg = b[: LIMIT - 4].decode("utf-8", "ignore") + "…"
    msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{msg}", flush=True)


def urls_only(urls):
    for u in urls:
        r = get(u, 6000000, 60)
        if "err" in r:
            fail(u, r)
            N[tag(u)] = r["err"]
            continue
        b = r.pop("body")
        t = b.decode("utf-8", "replace")
        meta = {k: v for k, v in r.items() if v}
        is_html = "html" in r["ct"].lower()
        body = plain(t) if is_html else t
        print(f"=== URL {u}\nMETA {json.dumps(meta, ensure_ascii=False)} read={len(b)} magic={b[:4].hex()}\nBODY {body[:12000]}", flush=True)
        if is_html:
            for a, x in links(t, u)[:120]:
                print(f"  LINK {a} | {x}", flush=True)
        end()
        N[tag(u)] = {**meta, "bytes": len(b), "head": body[:420]}
    keys = list(N)
    for i in range(0, min(len(keys), 40), 5):
        emit(f"probe fix urls {i // 5 + 1}", {k: N[k] for k in keys[i : i + 5]})
    emit("probe fix urls robots and errors", {"robots": {h: t[:160] for h, t in ROBOTS_TXT.items()}, "errors": ERR[:20]})


def main():
    if sys.argv[1:] == ["more"]:
        more()
        return
    if len(sys.argv) > 1:
        urls_only(sys.argv[1:])
        return
    for step in (bdl, wdi, eurostat, uprp, dane, knf, eu_registers, rcl):
        try:
            step()
        except Exception as e:  # noqa: BLE001
            ERR.append(f"step {step.__name__}: {type(e).__name__}: {str(e)[:120]}")
            print(f"=== STEP {step.__name__} FAILED {type(e).__name__}: {e}", flush=True)
    emit("probe fix 1 robots", {h.split("//")[1]: t[:230] for h, t in ROBOTS_TXT.items()})
    emit("probe fix 2 EX-001 GUS BDL", N.get("bdl"))
    emit("probe fix 3 EX-001 WDI and Eurostat", {"wdi": N.get("wdi"), "eurostat": N.get("eurostat")})
    emit("probe fix 4 UPRP annual report and bulletins", N.get("uprp"))
    emit("probe fix 5 dane.gov.pl", N.get("dane"))
    emit("probe fix 6 PLG-016 KNF pages and register applications", {"pages": N.get("knf_pages"), "apps": N.get("knf_apps")})
    emit("probe fix 7 PLG-016 EU registers", N.get("eu"))
    emit("probe fix 8 PLG-034 RCL, gov.pl, Sejm", N.get("rcl"))
    emit("probe fix 9 legal notices", N.get("legal"))
    emit("probe fix 10 errors and totals", {"requests": COUNT[0], "skipped_hosts": DEAD, "errors": ERR[:30]})


if __name__ == "__main__":
    main()
