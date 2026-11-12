"""Read-only follow-up probe for the source review batch D, F, H (PR #2878, verdict "fix first", 12 Nov 2026).

Looks for legitimate alternative routes for sources the first probe could not reach: other official hosts of
the same producer, dane.gov.pl, Eurostat coverage, and one slower retry of hosts that only timed out.
GET/HEAD only; robots.txt honoured (generic agent and Claude agents); a host that refuses (403, TLS error,
robots, bot wall) is reported and never retried another way. One request at a time per host, a second apart.
"""

import concurrent.futures as cf
import html
import io
import json
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import zipfile

UA = "zohelo-data-research/1.0 (+https://zohelo.com)"
CTX = ssl.create_default_context()
ROBOTS: dict[str, object] = {}
LOCKS: dict[str, threading.Lock] = {}
LAST: dict[str, float] = {}
GLOBAL = threading.Lock()
LIMIT = 3900
LEGAL = r"nota prawna|prawa autorskie|prawn|ponown|regulamin|licenc|warunki korzystania|zastrze"
KW = r"ponown\w* wykorzyst|licencj|prawa autorskie|praw autorskich|wykorzyst\w+ (treści|materiał|dan|informacj)|podani\w+ źródł|wskazani\w+ źródł|zastrzeżone|Creative Commons|CC BY|CC0"


def host_lock(netloc):
    with GLOBAL:
        return LOCKS.setdefault(netloc, threading.Lock())


def raw_get(url, n=400000, method="GET", accept="*/*", timeout=40):
    netloc = urllib.parse.urlsplit(url).netloc
    with host_lock(netloc):
        wait = 1.0 - (time.time() - LAST.get(netloc, 0))
        if wait > 0:
            time.sleep(wait)
        try:
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
                    "etag": (h.get("etag") or "")[:24] or None,
                    "cd": (h.get("content-disposition") or "")[:70] or None,
                    "rl": {k: v for k, v in h.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"} or None,
                    "final": r.geturl() if r.geturl() != url else None,
                    "body": body,
                }
        finally:
            LAST[netloc] = time.time()


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
            ROBOTS[host] = f"robots.txt {type(e).__name__}: {str(e)[:60]}"
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return rp
    for agent in ("zohelo-data-research", "ClaudeBot", "Claude-User"):
        if not rp.can_fetch(agent, url):
            return f"robots disallow ({agent})"
    return None


def get(url, n=400000, method="GET", accept="*/*", timeout=40):
    """Fetch; returns a dict with 'err' instead of raising."""
    try:
        no = robots_ok(url)
        if no:
            return {"err": no}
        return raw_get(url, n, method, accept, timeout)
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"err": f"{type(e).__name__}: {str(e)[:70]}"}


def meta(r):
    if "err" in r:
        return r["err"]
    out = {k: r[k] for k in ("st", "ct", "len", "lm", "etag", "cd", "rl", "final") if r.get(k)}
    if r.get("body"):
        out["read"] = len(r["body"])
    return out


def head(url):
    r = get(url, method="HEAD")
    if "err" in r and r["err"].startswith("HTTP 4"):
        r = get(url, n=1)
    m = meta(r)
    if isinstance(m, dict):
        m.pop("read", None)
        m.pop("st", None) if m.get("st") == 200 else None
    return m


def text(r):
    return r["body"].decode("utf-8", "replace") if "body" in r else ""


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def links(t, pattern, limit=12, base="", cut=50):
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', t):
        h, a = html.unescape(m.group(1)), plain(m.group(2))[:cut]
        if re.search(pattern, h + " " + a, re.I):
            item = f"{a}|{urllib.parse.urljoin(base, h)}" if a else urllib.parse.urljoin(base, h)
            if item not in out:
                out.append(item)
            if len(out) >= limit:
                break
    return out


def around(t, pattern, before=60, after=300, limit=2):
    p = plain(t) if "<" in t[:3000] else re.sub(r"\s+", " ", t)
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
    return {"chars": len(plain(t)), "scripts": len(re.findall(r"(?i)<script", t)), "title": plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:60]}


def robots_txt(host):
    try:
        r = raw_get(host + "/robots.txt", 60000, timeout=20)
        t = r["body"].decode("utf-8", "replace")
        if "<html" in t[:300].lower():
            return "html page"
        dis = [x.split(":", 1)[1].strip() for x in t.splitlines() if x.lower().startswith("disallow") and x.split(":", 1)[1].strip()]
        delay = re.findall(r"(?im)^crawl-delay:\s*(\S+)", t)
        named = sorted(set(re.findall(r"(?im)^user-agent:\s*(\S*(?:claude|anthropic|gptbot|ccbot)\S*)", t)))
        return f"disallow {len(dis)}: {' '.join(dis[:4])[:90]}" + (f"; delay {delay[0]}" if delay else "") + (f"; names {','.join(named)[:60]}" if named else "")
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {str(e)[:50]}"


def short(u):
    return re.sub(r"https?://(www\.)?", "", u)


def page(url, data_pat=None, n_data=8, kw=None, kw_n=2, kw_after=300, legal_n=0, heads=0, cut=45, n=900000):
    """One page: status, data links, keyword snippets, and the same snippets from its legal links."""
    r = get(url, n=n)
    if "err" in r:
        return {"page": r["err"]}
    t = text(r)
    out = {"page": r["st"]}
    if r.get("final"):
        out["final"] = short(r["final"])
    if len(plain(t)) < 600:
        out["js"] = js_only(r)
    if "ncapsula" in t or "cf-challenge" in t or "captcha" in t.lower()[:20000]:
        out["bot_wall"] = re.findall(r"(?i)incapsula|cf-challenge|captcha", t)[:2]
    base = r.get("final") or url
    if data_pat:
        allm = links(t, data_pat, 400, base, cut)
        out["n"] = len(allm)
        out["links"] = [short(x) for x in allm[:n_data]]
        if heads:
            out["head"] = [head(x.split("|")[-1]) for x in allm[:heads]]
    if kw:
        out["kw"] = around(t, kw, 60, kw_after, kw_n)
    if legal_n:
        ll = links(t, LEGAL, 8, base, 30)
        out["legal_links"] = [short(x) for x in ll[:6]]
        seen = []
        for x in ll:
            u = x.split("|")[-1].split("#")[0]
            if u in seen or u.rstrip("/") == base.rstrip("/") or not u.startswith("http"):
                continue
            seen.append(u)
            lr = get(u)
            out["legal:" + short(u)[:60]] = around(text(lr), KW, 70, 330, 1) if "body" in lr else lr["err"]
            if len(seen) >= legal_n:
                break
    return out


def xlsx_peek(url, rows=5, maxlen=6000000, cell=22, cols=10):
    r = get(url, n=maxlen, timeout=120)
    if "err" in r:
        return r["err"]
    b = r["body"]
    out = {"bytes": len(b), "lm": r.get("lm"), "cd": r.get("cd")}
    if b[:2] != b"PK":
        out["magic"] = b[:8].hex()
        return out
    try:
        from ingestion.common.xlsx_cells import cells

        sheets, grid = [], {}
        for c in cells(b):
            s = c["sheet_name"]
            if s not in sheets:
                sheets.append(s)
            if s == sheets[0] and c["row_number"] <= 40 and c["column_number"] <= cols:
                grid.setdefault(c["row_number"], {})[c["column_number"]] = str(c["value_text"])[:cell]
        out["sheets"] = [s[:25] for s in sheets[:14]]
        out["n_sheets"] = len(sheets)
        keys = sorted(grid)[: rows + 3]
        out["rows"] = [" ¦ ".join(grid[k].get(i, "") for i in range(1, max(grid[k]) + 1)) for k in keys][: rows + 3]
    except Exception as e:  # noqa: BLE001
        out["xlsx_err"] = f"{type(e).__name__}: {str(e)[:80]}"
    return out


def zip_peek(url, maxlen=80000000, gtfs=False, names=14, header_of=None, max_declared=80000000):
    h = get(url, method="HEAD")
    if "body" in h and h.get("len") and int(h["len"]) > max_declared:
        return {"head": meta(h), "skipped": "larger than the probe's download limit"}
    r = get(url, n=maxlen, timeout=300)
    if "err" in r:
        return r["err"]
    b = r["body"]
    out = {k: v for k, v in {"bytes": len(b), "lm": r.get("lm"), "etag": r.get("etag"), "cd": r.get("cd"), "ct": r.get("ct")}.items() if v}
    if b[:2] != b"PK":
        out["magic"] = b[:8].hex()
        out["text"] = b[:150].decode("utf-8", "replace") if b[:1] in (b"<", b"{") else None
        return out
    try:
        z = zipfile.ZipFile(io.BytesIO(b))
        infos = z.infolist()
        out["n_files"] = len(infos)
        out["unzipped"] = sum(i.file_size for i in infos)
        ext = {}
        for i in infos:
            e = i.filename.rsplit(".", 1)[-1].lower() if "." in i.filename else "-"
            ext[e] = ext.get(e, 0) + 1
        out["ext"] = ext
        out["names"] = [f"{i.filename[-60:]}:{i.file_size}" for i in infos[:names]]
        if gtfs:
            def lines(name):
                return z.read(name).decode("utf-8-sig", "replace").splitlines() if name in z.namelist() else []
            fi = lines("feed_info.txt")
            out["feed_info"] = fi[:2]
            for name in ("agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "shapes.txt"):
                if name in z.namelist():
                    out[name[:-4]] = max(0, z.read(name).count(b"\n") - 1)
            ag = lines("agency.txt")
            out["agency"] = [x[:110] for x in ag[:3]]
            for name, cols in (("calendar.txt", ("start_date", "end_date")), ("calendar_dates.txt", ("date", "date"))):
                ls = lines(name)
                if len(ls) > 1:
                    hd = [c.strip().strip('"') for c in ls[0].split(",")]
                    try:
                        a, bcol = hd.index(cols[0]), hd.index(cols[1])
                        vals = [x.split(",") for x in ls[1:]]
                        out[name[:-4]] = [min(v[a].strip('"') for v in vals if len(v) > bcol), max(v[bcol].strip('"') for v in vals if len(v) > bcol), len(vals)]
                    except ValueError:
                        out[name[:-4]] = ls[0][:80]
        if header_of:
            for i in infos:
                if re.search(header_of, i.filename, re.I):
                    with z.open(i) as f:
                        first = f.readline(6000)
                        nrows = sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1 << 20), b""))
                    out["csv"] = i.filename[-50:]
                    out["header"] = first.decode("utf-8-sig", "replace").strip()[:900]
                    out["rows"] = nrows
                    break
    except Exception as e:  # noqa: BLE001
        out["zip_err"] = f"{type(e).__name__}: {str(e)[:80]}"
    return out


def jget(url, n=3000000, timeout=40):
    r = get(url, n=n, accept="application/json", timeout=timeout)
    if "err" in r:
        return None, r["err"]
    try:
        return json.loads(text(r)), meta(r)
    except Exception as e:  # noqa: BLE001
        return None, {"st": r["st"], "ct": r.get("ct"), "body": plain(text(r))[:160], "e": str(e)[:40]}


DGP_DOWN = []


def dgp(dataset_id):
    """Licence fields of a dane.gov.pl dataset record (API first, the HTML record as fallback)."""
    if not DGP_DOWN:
        j, m = jget(f"https://api.dane.gov.pl/1.4/datasets/{dataset_id}", timeout=25)
        if j:
            a = j.get("data", {}).get("attributes", {})
            out = {"title": str(a.get("title"))[:70], "modified": a.get("modified"), "freq": a.get("update_frequency")}
            out.update({k: (str(v)[:160] if v not in (None, "", False) else v) for k, v in a.items() if "licen" in k and v not in (None, "")})
            return out
        if isinstance(m, str) and ("timed out" in m or "URLError" in m or "Timeout" in m):
            DGP_DOWN.append(m)
        api_err = m
    else:
        api_err = "api.dane.gov.pl skipped: " + DGP_DOWN[0][:50]
    r = get(f"https://dane.gov.pl/pl/dataset/{dataset_id}", n=1500000)
    if "err" in r:
        return {"api": api_err, "html": r["err"]}
    return {"api": api_err, "html": meta(r).get("st"), "js": js_only(r), "kw": around(text(r), r"licencj|Warunki ponownego|CC BY|CC0", 60, 300, 2)}


def dgp_search(q):
    if DGP_DOWN:
        return "skipped"
    j, m = jget("https://api.dane.gov.pl/1.4/datasets?per_page=6&q=" + urllib.parse.quote(q), timeout=25)
    if not j:
        if isinstance(m, str) and ("timed out" in m or "URLError" in m or "Timeout" in m):
            DGP_DOWN.append(m)
        return m
    data = j.get("data") if isinstance(j, dict) else None
    if not isinstance(data, list):
        return {"shape": str(j)[:120]}
    return {"count": (j.get("meta") or {}).get("count"), "hits": [f"{d.get('id')}:{str((d.get('attributes') or {}).get('title'))[:55]}" for d in data[:6]]}


def step(out, key, fn, *a, **k):
    try:
        out[key] = fn(*a, **k)
    except Exception as e:  # noqa: BLE001
        out[key] = f"probe_error {type(e).__name__}: {str(e)[:120]}"




# ---------------------------------------------------------------- follow-up probe (PR #2878)

SLOW = 75


def robots_load(host, timeout=SLOW):
    """robots.txt with a long timeout; fills the cache used by get(). Returns a summary and the raw text."""
    rp = urllib.robotparser.RobotFileParser()
    try:
        r = raw_get(host + "/robots.txt", 200000, timeout=timeout)
        txt = r["body"].decode("utf-8", "replace")
        is_html = "<html" in txt[:300].lower()
        rp.parse([] if is_html else txt.splitlines())
        ROBOTS[host] = rp
        return ("html page" if is_html else "ok"), ("" if is_html else txt)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            ROBOTS[host] = f"robots.txt {e.code}"
        else:
            rp.parse([])
            ROBOTS[host] = rp
        return f"HTTP {e.code}", ""
    except Exception as e:  # noqa: BLE001
        ROBOTS[host] = f"robots.txt {type(e).__name__}: {str(e)[:60]}"
        return f"{type(e).__name__}: {str(e)[:60]}", ""


def robots_groups(txt):
    """Which agents a robots.txt names, and what the generic group disallows."""
    agents = re.findall(r"(?im)^user-agent:\s*(\S+)", txt)
    named = sorted({a for a in agents if re.search(r"(?i)claude|anthropic|gptbot|ccbot|ai", a)})[:8]
    star = []
    cur = None
    for line in txt.splitlines():
        m = re.match(r"(?i)\s*user-agent:\s*(\S+)", line)
        if m:
            cur = m.group(1)
            continue
        m = re.match(r"(?i)\s*disallow:\s*(\S*)", line)
        if m and cur == "*":
            star.append(m.group(1))
    return {"n_agents": len(set(agents)), "named": named, "star_disallow": star[:10], "n_star": len(star)}


def pdf_text(url, maxlen=6000000):
    """Does a PDF carry a text layer? Counts content streams with text operators; no text is printed."""
    import zlib

    r = get(url, n=maxlen, timeout=120)
    if "err" in r:
        return r["err"]
    b = r["body"]
    out = {"bytes": len(b), "ct": r.get("ct"), "lm": r.get("lm")}
    if b[:4] != b"%PDF":
        out["magic"] = b[:8].hex()
        return out
    n_text = n_stream = chars = 0
    for m in re.finditer(rb"stream\r?\n(.*?)endstream", b, re.S):
        n_stream += 1
        try:
            d = zlib.decompress(m.group(1).strip(b"\r\n"))
        except Exception:  # noqa: BLE001
            try:
                d = zlib.decompressobj().decompress(m.group(1))
            except Exception:  # noqa: BLE001
                continue
        if b"BT" in d and (b"Tj" in d or b"TJ" in d):
            n_text += 1
            chars += len(re.findall(rb"T[jJ]", d))
    out.update({"pages": len(re.findall(rb"/Type\s*/Page\b", b)), "streams": n_stream, "text_streams": n_text,
                "text_ops": chars, "fonts": len(re.findall(rb"/Font\b", b)), "tounicode": len(re.findall(rb"/ToUnicode", b)),
                "images": len(re.findall(rb"/Subtype\s*/Image", b))})
    return out


def dane_gov():
    o = {}
    for h in ("https://api.dane.gov.pl", "https://dane.gov.pl"):
        st, txt = robots_load(h)
        o["robots_" + h.split("//")[1]] = st if st != "ok" else robots_groups(txt)
    api_ok = not isinstance(ROBOTS.get("https://api.dane.gov.pl"), str)
    if api_ok:
        for q in ("Urząd Transportu Kolejowego", "Służba Więzienna", "UOKiK decyzje", "Najwyższa Izba Kontroli",
                  "Urząd Lotnictwa Cywilnego porty lotnicze", "Warszawa rozkłady jazdy", "GDDKiA"):
            j, m = jget("https://api.dane.gov.pl/1.4/datasets?per_page=5&q=" + urllib.parse.quote(q), timeout=SLOW)
            if not j:
                o["q:" + q[:18]] = m
                if isinstance(m, str) and "HTTP" not in m:
                    break
                continue
            data = j.get("data") or []
            o["q:" + q[:18]] = {"count": (j.get("meta") or {}).get("count"),
                                "hits": [f"{d.get('id')}:{str((d.get('attributes') or {}).get('title'))[:48]}" for d in data[:5]]}
        j, m = jget("https://api.dane.gov.pl/1.4/datasets/128", timeout=SLOW)
        if j:
            a = j.get("data", {}).get("attributes", {})
            o["ds128"] = {"title": str(a.get("title"))[:60], "modified": a.get("modified"),
                          "lic": {k: str(v)[:120] for k, v in a.items() if "licen" in k and v not in (None, "", False)}}
            j2, m2 = jget("https://api.dane.gov.pl/1.4/datasets/128/resources?per_page=5", timeout=SLOW)
            if j2:
                o["ds128_res"] = [{"t": str((d.get("attributes") or {}).get("title"))[:40], "f": (d.get("attributes") or {}).get("format"),
                                   "link": str((d.get("attributes") or {}).get("link") or (d.get("attributes") or {}).get("file_url") or (d.get("attributes") or {}).get("download_url"))[:110]}
                                  for d in (j2.get("data") or [])[:4]]
                files = [x["link"] for x in o["ds128_res"] if x["link"].startswith("http")]
                if files:
                    o["ds128_head"] = head(files[0])
            else:
                o["ds128_res"] = m2
        else:
            o["ds128"] = m
    else:
        r = get("https://dane.gov.pl/pl/dataset/128", n=300000, timeout=SLOW)
        o["html128"] = meta(r) if "err" in r else {"st": r["st"], "js": js_only(r)}
    return o


def uokik2():
    o = {}
    st, txt = robots_load("https://uokik.gov.pl", 40)
    o["robots_uokik"] = st if st != "ok" else robots_groups(txt)
    rp = ROBOTS.get("https://uokik.gov.pl")
    if not isinstance(rp, str):
        o["can_fetch"] = {a: rp.can_fetch(a, "https://uokik.gov.pl/koncentracje") for a in ("zohelo-data-research", "*", "ClaudeBot")}
    st, txt = robots_load("https://decyzje.uokik.gov.pl")
    o["robots_decyzje"] = st if st != "ok" else (robots_groups(txt) | {"raw": txt[:200]})
    if not isinstance(ROBOTS.get("https://decyzje.uokik.gov.pl"), str):
        r = get("https://decyzje.uokik.gov.pl/bp/dec_prez.nsf", timeout=SLOW)
        o["dec_prez"] = meta(r)
        if "body" in r:
            t = text(r)
            o["js"] = js_only(r)
            o["frames"] = re.findall(r'(?i)<i?frame[^>]+src=["\']([^"\']+)', t)[:5]
            o["links"] = [short(x) for x in links(t, r".", 10, "https://decyzje.uokik.gov.pl/bp/", 30)]
            views = re.findall(r'(?i)dec_prez\.nsf/([A-Za-z0-9_\-]+)\?OpenView', t)[:3]
            o["views"] = views
            for v in views[:1] or ["WWW-wszystkie"]:
                x = get(f"https://decyzje.uokik.gov.pl/bp/dec_prez.nsf/{v}?ReadViewEntries&count=3&OutputFormat=JSON", n=6000, timeout=SLOW)
                o["view_entries"] = meta(x) if "err" in x else {"st": x["st"], "ct": x.get("ct"), "start": plain(text(x))[:260]}
    return o


def nik2():
    o = {}
    for h in ("https://www.nik.gov.pl", "https://bip.nik.gov.pl"):
        st, txt = robots_load(h, 40)
        o["robots_" + h.split("//")[1][:7]] = st if st != "ok" else (robots_groups(txt) | {"raw": re.sub(r"\s+", " ", txt)[:160]})
    for key, u in (("wyniki", "https://www.nik.gov.pl/kontrole/wyniki-kontroli-nik/"), ("bip_wyniki", "https://bip.nik.gov.pl/kontrole/wyniki-kontroli-nik/")):
        r = get(u, n=1500000)
        if "err" in r:
            o[key] = r["err"]
            continue
        t = text(r)
        p = plain(t)
        recs = links(t, r"/kontrole/[A-Z]/\d+/\d+|kontrole,\d+|pobierz,|/plik/id", 400, u, 60)
        o[key] = {"st": r["st"], "chars": len(p), "wall": sorted(set(re.findall(r"(?i)incapsula|captcha|Request unsuccessful", t)))[:3],
                  "n_records": len(recs), "first": [short(x)[:120] for x in recs[:4]], "sig": re.findall(r"\b[A-Z]/\d{2}/\d{3}(?:/[A-Z]+)?\b", p)[:5],
                  "pager": [short(x)[:80] for x in links(t, r"strona|page|,\d+\.html", 4, u, 12)], "forms": len(re.findall(r"(?i)<form", t))}
    step(o, "pdf_head", head, "https://www.nik.gov.pl/plik/id,31066.pdf")
    return o


def sw2():
    o = {}
    for h in ("https://www.sw.gov.pl", "https://bip.sw.gov.pl"):
        st, txt = robots_load(h, 40)
        key = h.split("//")[1]
        o["robots_" + key] = st if st != "ok" else robots_groups(txt)
        if isinstance(ROBOTS.get(h), str):
            continue
        for sub in ("/strona/statystyka-miesieczna", "/strona/statystyka-roczna"):
            r = get(h + sub, n=1500000)
            if "err" in r:
                o[key + sub[-9:]] = r["err"]
                continue
            t = text(r)
            ls = links(t, r"\.(pdf|xlsx?|ods|zip)\b|/assets/", 900, h + "/", 40)
            ext = {}
            for x in ls:
                e = x.rsplit(".", 1)[-1].lower()[:5]
                ext[e] = ext.get(e, 0) + 1
            o[key + sub[-9:]] = {"final": r.get("final"), "n": len(ls), "ext": ext, "first": [short(x)[:110] for x in ls[:2]], "last": [x.split("|")[0] for x in ls[-1:]],
                                 "legal": [short(x) for x in links(t, LEGAL, 4, h + "/", 26)]}
            if ls and "pdf" not in o:
                step(o, "pdf", pdf_text, ls[0].split("|")[-1])
        step(o, key + "_reuse", lambda h=h: around(text(get(h + "/strona/ponowne-wykorzystywanie-informacji-sektora-publicznego")), KW, 60, 400, 1))
    return o


def utk2():
    o = {}
    h = "https://utk.gov.pl"
    st, txt = robots_load(h, 40)
    o["robots"] = st if st != "ok" else robots_groups(txt)
    if isinstance(ROBOTS.get(h), str):
        return o
    r = get(h + "/", n=900000)
    o["home"] = meta(r) if "err" in r else r["st"]
    if "body" in r:
        t = text(r)
        o["stat_links"] = [short(x)[:90] for x in links(t, r"statyst|dane\.utk|raport|analiz", 8, h + "/", 30)]
        o["legal"] = [short(x)[:90] for x in links(t, LEGAL, 5, h + "/", 30)]
        for x in links(t, r"ponown", 1, h + "/", 30):
            lr = get(x.split("|")[-1])
            o["reuse"] = around(text(lr), KW, 80, 500, 2) if "body" in lr else lr["err"]
    for u in ("https://utk.gov.pl/pl/raporty-i-analizy/analizy-i-monitoring/statystyki", "https://utk.gov.pl/pl/dokumenty-i-formularze/opracowania-urzedu-tran"):
        pr = get(u, n=900000)
        o[u.rsplit("/", 1)[-1][:14]] = pr["err"] if "err" in pr else {"st": pr["st"], "final": pr.get("final"),
                                                                     "files": [short(x)[:100] for x in links(text(pr), r"\.(xlsx?|csv|ods|zip|pdf)\b|/download/", 4, h + "/", 30)]}
    return o


def senat2():
    o = {}
    st, txt = robots_load("https://www.senat.gov.pl", 40)
    o["robots"] = st if st != "ok" else (robots_groups(txt) | {"raw": re.sub(r"\s+", " ", txt)[:300]})
    rp = ROBOTS.get("https://www.senat.gov.pl")
    if not isinstance(rp, str):
        o["can_fetch"] = {a: rp.can_fetch(a, "https://www.senat.gov.pl/prace/posiedzenia/") for a in ("zohelo-data-research", "*", "ClaudeBot")}
    r = get("https://www.senat.gov.pl/ponowne-wykorzystywanie-informacji-sektora-publicznego/", n=900000)
    if "err" in r:
        o["reuse"] = r["err"]
    else:
        t = re.sub(r"(?is)<(header|nav|footer)\b.*?</\1>", " ", text(r))
        p = plain(t)
        i = max(p.find("Warunk"), p.find("warunk"))
        o["reuse_chars"] = len(p)
        o["reuse"] = p[max(0, i - 200) : i + 1900] if i >= 0 else p[600:2200]
    return o


def eurostat2():
    o = {}
    want = r"^(mar_(mg|mp|mt|go)_[a-z0-9_]+|avia_(paoa|paoc|tf_apal|tf_ala)|rail_(pa|go)_(quartal|total)|crim_pris_[a-z]+)$"
    r = get("https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en", n=12000000, timeout=120)
    found = {}
    if "err" in r:
        o["toc"] = r["err"]
    else:
        for line in text(r).splitlines():
            c = [x.strip().strip('"') for x in line.split("\t")]
            if len(c) > 6 and re.match(want, c[1]) and c[1] not in found:
                found[c[1]] = f"{c[0][:38]}|{c[5]}..{c[6]}"
        keep = [k for k in found if re.search(r"^mar_(mg_aa_pwhd|mg_am_pwhc|mg_am_pvh|mp_aa_pphd|mt_am_pvh|go_qm_pl|go_qmc|go_am_pl)|^avia|^rail|^crim_pris_cap", k)]
        o["toc"] = {"n_match": len(found), "codes": {k: found[k] for k in keep[:16]}, "mar_all": sorted(k for k in found if k.startswith("mar_") and not re.search(r"_[a-z]{2}$", k))[:30]}
    j, m = jget("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/mar_mg_aa_pwhd?format=JSON&lang=EN&lastTimePeriod=1", n=30000000, timeout=120)
    if j:
        gd = {}
        for dim, d in (j.get("dimension") or {}).items():
            for code, lab in ((d.get("category") or {}).get("label") or {}).items():
                if re.search(r"(?i)gda|gdy|szczecin", str(lab)):
                    gd[code] = f"{dim}:{lab}"
        o["pwhd"] = {"dims": j.get("id"), "size": j.get("size"), "polish_ports": gd, "time": list(((j.get("dimension") or {}).get("time") or {}).get("category", {}).get("index", {}))[:2]}
    else:
        o["pwhd"] = m
    try:
        from ingestion.common import r2

        codes = ["mar_mg_aa_pwhd", "mar_go_qm_pl", "avia_paoa", "rail_pa_quartal", "rail_go_quartal", "crim_pris_cap"]
        loaded = {}
        for c in codes:
            try:
                rows = r2.sql(f"select count(*) as n from silver.eurostat_observations where dataset_id = '{c.upper()}' or dataset_id = '{c}'")
                loaded[c] = list(rows[0].values())[0] if rows else None
            except Exception as e:  # noqa: BLE001
                loaded[c] = f"{type(e).__name__}: {str(e)[:90]}"
                if "dataset_id" in str(e) or "not found" in str(e).lower():
                    break
        o["silver_rows"] = loaded
        try:
            rows = r2.sql("select dataset_id from silver.eurostat_dataflows where dataset_id like 'MAR_M%' or dataset_id like 'mar_m%' limit 40")
            o["dataflows_mar"] = sorted(str(list(x.values())[0]) for x in rows)[:40]
        except Exception as e:  # noqa: BLE001
            o["dataflows_mar"] = f"{type(e).__name__}: {str(e)[:120]}"
    except Exception as e:  # noqa: BLE001
        o["r2"] = f"{type(e).__name__}: {str(e)[:120]}"
    return o


def pdfs():
    o = {}
    step(o, "ulc_2026q1", pdf_text, "https://ulc.gov.pl/_download/2026/statystyki_2026/_pl/wg_portow_lotniczych_1kw2026.pdf")
    step(o, "ulc_2010q1", pdf_text, "https://ulc.gov.pl/_download/regulacja_rynku/statystyki/1ql_2010.pdf")
    step(o, "gddkia_pbdk", pdf_text, "https://www.gov.pl/attachment/be528ff9-8d45-4989-9833-531e6c48e881")
    r = get("https://www.gov.pl/web/gddkia/wykaz-parkingow-i-mop", n=900000)
    if "err" in r:
        o["mop"] = r["err"]
    else:
        ls = links(text(r), r"/attachment/", 6, "https://www.gov.pl/", 50)
        o["mop_files"] = [short(x)[:100] for x in ls[:3]]
        if ls:
            step(o, "mop_xlsx", xlsx_peek, ls[0].split("|")[-1], 4, 6000000, 18, 8)
    st, txt = robots_load("https://archiwum.gddkia.gov.pl")
    o["robots_archiwum"] = st if st != "ok" else robots_groups(txt)
    if not isinstance(ROBOTS.get("https://archiwum.gddkia.gov.pl"), str):
        step(o, "utrudnienia", lambda: page("https://www.gov.pl/web/gddkia/dane-o-utrudnieniach-na-drogach-krajowych", r"\.xml|\.xsd|archiwum", 4, heads=2))
    return o


def warszawa2():
    o = {}
    for h in ("https://api.um.warszawa.pl", "https://dane.um.warszawa.pl"):
        st, txt = robots_load(h)
        key = h.split("//")[1][:8]
        o["robots_" + key] = st if st != "ok" else robots_groups(txt)
        if isinstance(ROBOTS.get(h), str):
            continue
        step(o, "home_" + key, page, h + "/", r"regulamin|rejestr|apikey|zaloguj|gtfs|rozkład|licenc", 8, r"apikey|klucz|rejestrac|licenc|regulamin|warunk", 2, 260, 1, 0, 36)
    if not isinstance(ROBOTS.get("https://dane.um.warszawa.pl"), str):
        j, m = jget("https://dane.um.warszawa.pl/api/3/action/package_search?q=gtfs&rows=3", n=400000, timeout=SLOW)
        o["ckan"] = m if j is None else {"count": j.get("result", {}).get("count"), "hits": [f"{d.get('name')}|{d.get('license_title')}" for d in j.get("result", {}).get("results", [])[:3]]}
    return o


GROUPS = [
    ("dane.gov.pl (PL-ENV-025 and others)", dane_gov),
    ("PLG-020 PLG-021 UOKiK", uokik2),
    ("PLG-028 NIK", nik2),
    ("PLG-032 Prison Service", sw2),
    ("PL-ENV-025 UTK main site", utk2),
    ("PLG-025 Senate", senat2),
    ("Eurostat coverage (PL-ENV-027, -026, -025, PLG-032)", eurostat2),
    ("PDF text layer (PL-ENV-026, PL-ENV-023)", pdfs),
    ("PL-ENV-028 Warsaw", warszawa2),
]


def run_one(item):
    title, fn = item
    try:
        data = fn()
    except Exception as e:  # noqa: BLE001
        data = {"probe_error": f"{type(e).__name__}: {str(e)[:200]}"}
    return title, data


def main():
    with cf.ThreadPoolExecutor(len(GROUPS)) as ex:
        results = list(ex.map(run_one, GROUPS))
    # Nine notices (the limit is ten per level and step), each cut to the annotation size.
    for title, data in results:
        b = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(b) > LIMIT:
            b = b[: LIMIT - 4] + "…".encode()
        msg = b.decode("utf-8", "ignore").replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::notice title={title.replace(',', ' ')}::{msg}", flush=True)


if __name__ == "__main__":
    main()
