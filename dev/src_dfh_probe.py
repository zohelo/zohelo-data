"""Read-only reachability probe for the source review batch D, F, H (12 Nov 2026).

GET/HEAD only, one compact annotation per source (20: ten notices, ten warnings, the limit per level and
step). robots.txt is honoured (generic agent and Claude agents); a refused or failing host is reported,
never retried another way. One request at a time per host, at least a second apart. No personal data is
printed: files that may hold names are reported by header and row count only.
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


# ---------------------------------------------------------------- F: law, justice, elections


def uokik():
    o = {}
    step(o, "robots_decyzje", robots_txt, "https://decyzje.uokik.gov.pl")
    r = get("https://decyzje.uokik.gov.pl/bp/dec_prez.nsf")
    o["dec_prez"] = meta(r)
    if "body" in r:
        t = text(r)
        o["dec_js"] = js_only(r)
        o["dec_frames"] = re.findall(r'(?i)<i?frame[^>]+src=["\']([^"\']+)', t)[:5]
        o["dec_links"] = [short(x) for x in links(t, r".", 8, "https://decyzje.uokik.gov.pl/bp/", 30)]
        o["dec_export"] = sorted(set(re.findall(r"(?i)ReadViewEntries|OpenView|\.xml|\.csv|rss", t)))[:6]
    step(o, "view_xml", lambda: meta(get("https://decyzje.uokik.gov.pl/bp/dec_prez.nsf/WWW-wszystkie?ReadViewEntries&count=2", n=3000)))
    step(o, "koncentracje", page, "https://uokik.gov.pl/koncentracje", r"koncentrac|decyz|rejestr|\.xlsx?|\.csv|zgłosz", 8, KW, 1, 300, 2)
    step(o, "robots_uokik", robots_txt, "https://uokik.gov.pl")
    step(o, "sprawozdania", page, "https://uokik.gov.pl/sprawozdania-z-dzialalnosci-urzedu", r"sprawozdan|Download|\.pdf", 6)
    step(o, "bip_reuse", page, "https://bip.uokik.gov.pl/", r"ponown", 3, None, 0, 0, 0)
    step(o, "dane_gov", dgp_search, "UOKiK")
    return o


def senat():
    o = {}
    step(o, "robots", robots_txt, "https://www.senat.gov.pl")
    step(o, "home", page, "https://www.senat.gov.pl/", r"otwarte dane|\bapi\b|\.xml|\.json|\.csv|rss|dane publiczne", 8, None, 0, 0, 2)
    r = get("https://www.senat.gov.pl/prace/posiedzenia/")
    o["posiedzenia"] = meta(r)
    if "body" in r:
        t = text(r)
        ls = links(t, r"glosowan|głosowan", 3, "https://www.senat.gov.pl/")
        o["vote_links"] = [short(x) for x in ls]
        if ls:
            v = get(ls[0].split("|")[-1])
            if "body" in v:
                vt = text(v)
                o["vote_page"] = {"tr": len(re.findall(r"(?i)<tr", vt)), "export": [short(x) for x in links(vt, r"\.xml|\.csv|\.xlsx?|\.json|pobierz|eksport|drukuj", 5, "https://www.senat.gov.pl/")], "js": js_only(v)}
            else:
                o["vote_page"] = v["err"]
    step(o, "proces", page, "https://www.senat.gov.pl/prace/proces-legislacyjny-w-senacie/", r"\.xml|\.csv|\.json|rss|eksport", 5)
    step(o, "senatorowie", page, "https://www.senat.gov.pl/sklad/senatorowie/", r"senator,\d+|\.xml|\.csv", 3)
    step(o, "bip_reuse", page, "https://bip.senat.gov.pl/", r"ponown", 3, KW, 1)
    j, m = jget("https://api.sejm.gov.pl/senat/term11/senators", n=20000)
    o["api_sejm_senat"] = m if j is None else {"ok": True, "n": len(j) if isinstance(j, list) else list(j)[:5]}
    return o


def nik():
    o = {}
    step(o, "robots", robots_txt, "https://www.nik.gov.pl")
    step(o, "wyniki", page, "https://www.nik.gov.pl/kontrole/wyniki-kontroli-nik/", r"rss|\.xml|\bapi\b|dane\.gov|\.csv|\.xlsx?|pobierz", 8, KW, 1, 300, 2)
    step(o, "home", lambda: meta(get("https://www.nik.gov.pl/", n=50000)))
    step(o, "rss", lambda: meta(get("https://www.nik.gov.pl/rss/", n=5000)))
    step(o, "bip", page, "https://bip.nik.gov.pl/", r"ponown", 3, KW, 1)
    step(o, "robots_bip", robots_txt, "https://bip.nik.gov.pl")
    step(o, "dane_gov", dgp_search, "Najwyższa Izba Kontroli")
    return o


def pkw_site(base, names):
    o = {}
    r = get(base + "/pl/dane_w_arkuszach", n=1500000)
    o["page"] = meta(r) if "err" in r else r["st"]
    if "body" in r:
        t = text(r)
        if r.get("final"):
            o["final"] = short(r["final"])
        ls = links(t, r"\.(csv|xlsx|zip)\b|/data/", 400, base + "/pl/", 40)
        o["n"] = len(ls)
        o["links"] = [short(x) for x in ls[:6]]
        hits = sorted(set(re.findall(r'["\'/]([\w./-]*data/[\w./-]+\.(?:zip|csv|xlsx))', t)))
        o["data_strings"] = hits[:8]
        if not ls and not hits:
            o["js"] = js_only(r)
            srcs = [urllib.parse.urljoin(base + "/pl/", s) for s in re.findall(r'(?i)<script[^>]+src=["\']([^"\']+)', t)][:6]
            o["scripts"] = [short(s)[-50:] for s in srcs]
            found = set()
            for s in srcs[:4]:
                jr = get(s, n=6000000, timeout=60)
                if "body" in jr:
                    found |= set(re.findall(r'([\w./-]*(?:_csv|_xlsx)\.zip|[\w/-]*data/[\w./-]+)', text(jr)))
            o["bundle_hits"] = sorted(found)[:14]
    o["guess"] = {nm: head(f"{base}/data/csv/{nm}_csv.zip") for nm in names}
    return o


def pkw_a():
    o = {}
    for nm, u in (("woj_zip", "https://wybory.gov.pl/prezydent2025/data/csv/wyniki_gl_na_kandydatow_po_wojewodztwach_w_drugiej_turze_csv.zip"), ("gm_zip", "https://wybory.gov.pl/prezydent2025/data/csv/wyniki_gl_na_kandydatow_po_gminach_w_drugiej_turze_csv.zip")):
        step(o, nm, zip_peek, u, 30000000, False, 3, r"\.csv$")
        if isinstance(o.get(nm), dict) and o[nm].get("header"):
            o[nm]["header"] = o[nm]["header"][:420]
    step(o, "prezydent2025", pkw_site, "https://wybory.gov.pl/prezydent2025", ["wyniki_gl_na_kandydatow_po_wojewodztwach_w_drugiej_turze", "wyniki_gl_na_kandydatow_po_gminach_w_drugiej_turze", "wyniki_gl_na_kandydatow_po_obwodach_w_drugiej_turze"])
    step(o, "sejmsenat2023", pkw_site, "https://wybory.gov.pl/sejmsenat2023", ["wyniki_gl_na_listy_po_wojewodztwach_sejm", "wyniki_gl_na_listy_po_gminach_sejm"])
    step(o, "sejmsenat2023_own_host", lambda: meta(get("https://sejmsenat2023.pkw.gov.pl/sejmsenat2023/pl/dane_w_arkuszach", n=3000)))
    step(o, "pe2024", pkw_site, "https://wybory.gov.pl/pe2024", ["wyniki_gl_na_listy_po_wojewodztwach", "wyniki_gl_na_listy_po_gminach"])
    step(o, "samorzad2024", pkw_site, "https://wybory.gov.pl/samorzad2024", ["wyniki_gl_na_listy_po_wojewodztwach_sejmiki", "frekwencja_po_wojewodztwach"])
    return o


def pkw_b():
    o = {}
    for h in ("https://wybory.gov.pl", "https://pkw.gov.pl", "https://danewyborcze.kbw.gov.pl"):
        step(o, "robots_" + h.split("//")[1], robots_txt, h)
    step(o, "danewyborcze", page, "https://danewyborcze.kbw.gov.pl/", r"\.(csv|xlsx?|zip)\b|pobierz|dane|wybory|indexc|archiw", 14, KW, 2, 300, 2, 0, 40)
    r = get("https://danewyborcze.kbw.gov.pl/")
    if "body" in r:
        t = text(r)
        o["dw_js"] = js_only(r)
        ls = links(t, r"sejm|prezydent|samorz|referend|parlament", 30, "https://danewyborcze.kbw.gov.pl/", 30)
        o["dw_elections_n"] = len(ls)
        if ls:
            sub = get(ls[0].split("|")[-1])
            if "body" in sub:
                fl = links(text(sub), r"\.(csv|xlsx?|zip|ods)\b", 400, ls[0].split("|")[-1], 30)
                o["dw_first"] = {"url": short(ls[0]), "n_files": len(fl), "files": [short(x) for x in fl[:4]], "head": [head(x.split("|")[-1]) for x in fl[:1]]}
            else:
                o["dw_first"] = sub["err"]
    step(o, "pkw_home", page, "https://pkw.gov.pl/", r"dane w arkuszach|danewyborcze|archiw", 5, KW, 1, 300, 2)
    step(o, "wybory_home", page, "https://wybory.gov.pl/", r"prezydent|sejm|pe20|samorz|referend", 10, KW, 1, 300, 2, 0, 25)
    step(o, "pe2019_zip", head, "https://pe2019.pkw.gov.pl/pe2019/data/csv/obwody_glosowania_csv.zip")
    step(o, "kbw_bip_reuse", page, "https://www.kbw.gov.pl/", r"ponown", 3, KW, 1)
    return o


def isws():
    o = {}
    b = "https://isws.ms.gov.pl/pl/baza-statystyczna/"
    step(o, "robots", robots_txt, "https://isws.ms.gov.pl")
    step(o, "hub", page, b, r"baza-statystyczna/[a-z-]+/?$", 10, KW, 1, 300, 2, 0, 30)
    for sub in ("opracowania-wieloletnie/", "publikacje/", "opracowania-jednoroczne/"):
        r = get(b + sub)
        if "err" in r:
            o[sub] = r["err"]
            continue
        t = text(r)
        ls = links(t, r"download,\d+,\d+\.html", 600, b + sub, 60)
        o[sub] = {"n": len(ls), "first": [x.split("|")[0] + "|" + x.rsplit("/", 1)[-1] for x in ls[:7]], "years": sorted(set(re.findall(r"\b(?:19|20)\d\d\b", " ".join(x.split("|")[0] for x in ls))))[:1] + sorted(set(re.findall(r"\b(?:19|20)\d\d\b", " ".join(x.split("|")[0] for x in ls))))[-1:], "ext_in_text": sorted(set(re.findall(r"(?i)\b(xlsx?|pdf|docx?|ods|csv|zip)\b", plain(t))))[:6]}
        o[sub]["head"] = [head(x.split("|")[-1]) for x in ls[:3]]
    step(o, "dgp_625", dgp, 625)
    step(o, "ms_search", dgp_search, "Ministerstwo Sprawiedliwości statystyka sądowa")
    return o


def sw():
    o = {}
    step(o, "robots", robots_txt, "https://sw.gov.pl")
    step(o, "hub", page, "https://sw.gov.pl/strona/Statystyka", r"statystyka-|\.pdf|\.xlsx?", 8, KW, 1, 300, 2, 0, 35)
    for sub in ("statystyka-miesieczna", "statystyka-roczna", "statystyka-kwartalna"):
        r = get("https://sw.gov.pl/strona/" + sub)
        if "err" in r:
            o[sub] = r["err"]
            continue
        ls = links(text(r), r"\.(pdf|xlsx?|ods|zip)\b|/assets/", 900, "https://sw.gov.pl/", 40)
        ext = {}
        for x in ls:
            e = x.rsplit(".", 1)[-1].lower()[:5]
            ext[e] = ext.get(e, 0) + 1
        o[sub] = {"n": len(ls), "ext": ext, "first": [short(x)[:120] for x in ls[:3]], "last": [x.split("|")[0] for x in ls[-2:]], "head": [head(x.split("|")[-1]) for x in ls[:2]]}
    step(o, "dane_gov", dgp_search, "Służba Więzienna")
    return o


def policja():
    o = {}
    step(o, "robots", robots_txt, "https://statystyka.policja.pl")
    step(o, "terms", lambda: (lambda r: around(text(r), r"wykorzyst|źródł|zgod\w+ |komerc|autorsk", 80, 520, 3) if "body" in r else r["err"])(get("https://policja.pl/pol/wolnytekst/46616,dok.html")))
    r = get("https://statystyka.policja.pl/")
    o["home"] = meta(r) if "err" in r else r["st"]
    if "body" in r:
        t = text(r)
        o["sections"] = [short(x)[:70] for x in links(t, r"/st/[a-z-]+", 14, "https://statystyka.policja.pl/", 30)]
        o["nota"] = [short(x) for x in links(t, r"nota|prawn|wykorzysta", 3, "https://statystyka.policja.pl/", 40)]
    step(o, "ogolem", page, "https://statystyka.policja.pl/st/przestepstwa-ogolem", r"/download/", 6, r"aktualiz|Data publikacji", 1, 120, 0, 2, 45)
    step(o, "wypadki", page, "https://statystyka.policja.pl/st/ruch-drogowy/76562,Wypadki-drogowe-raporty-roczne.html", r"/download/", 4, None, 0, 0, 0, 1, 45)
    r2 = get("https://statystyka.policja.pl/st/przestepstwa-ogolem")
    if "body" in r2:
        fl = links(text(r2), r"/download/.*\.xlsx", 3, "https://statystyka.policja.pl/")
        if fl:
            step(o, "xlsx", xlsx_peek, fl[0].split("|")[-1], 6)
    step(o, "dane_gov", dgp_search, "Komenda Główna Policji")
    return o


def gov_attachments(url, pat=r"/attachment/", n=10, cut=48):
    r = get(url, n=1500000)
    if "err" in r:
        return None, {"page": r["err"]}
    t = text(r)
    ls = links(t, pat, 500, "https://www.gov.pl", cut)
    return t, {"page": r["st"], "n": len(ls), "files": [x.split("|")[0] + "|" + x.rsplit("/", 1)[-1][:13] for x in ls[:n]], "_urls": [x.split("|")[-1] for x in ls]}


def psp():
    o = {}
    t, a = gov_attachments("https://www.gov.pl/web/kgpsp/interwencje-psp", n=8)
    urls = a.pop("_urls", [])
    o["interwencje"] = a
    if t:
        o["licence"] = around(t, r"Rodzaj licencji|Uznanie autorstwa|LICENCJA", 120, 520, 3)
        o["dane_gov_links"] = [short(x) for x in links(t, r"dane\.gov\.pl", 4)]
        o["head"] = [head(u) for u in urls[:2]]
        tl = links(t, r"tabel|stałych", 40, "https://www.gov.pl", 60)
        o["tables_links"] = [x.split("|")[0] for x in tl[:3]]
        if tl:
            step(o, "tables_zip", zip_peek, tl[0].split("|")[-1], 40000000, False, 16)
    step(o, "ofiary", page, "https://www.gov.pl/web/kgpsp/ofiary-polska", r"/attachment/|\.csv|\.xlsx?", 4, r"aktualiz|stan na|ofiar śmiertelnych", 2, 200)
    step(o, "dgp_2080", dgp, 2080)
    return o


# ---------------------------------------------------------------- H: transport


def gpr():
    o = {}
    for key, u in (("gpr2025", "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025"), ("gpr2020", "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-20202021")):
        t, a = gov_attachments(u, n=12, cut=60)
        urls = a.pop("_urls", [])
        o[key] = a
        if t:
            o[key]["kw"] = around(t, r"licenc|wykorzyst\w+ dan|źródł", 60, 200, 1)
            o[key]["head"] = [head(x) for x in urls[:2]]
            if key == "gpr2025":
                sd = [x for x in links(t, r"SDRR", 6, "https://www.gov.pl", 80) if "attachment" in x]
                o["sdrr_links"] = [x.split("|")[0] for x in sd[:4]]
                xl = [x for x in sd if "pdf" not in x.lower()] or sd
                if xl:
                    step(o, "sdrr_xlsx", xlsx_peek, xl[0].split("|")[-1], 7, 6000000, 18, 14)
    step(o, "editions", page, "https://www.gov.pl/web/gddkia/przeprowadzone-pomiary", r"pomiar", 10, None, 0, 0, 0, 0, 40)
    return o


def gddkia():
    o = {}
    b = "https://www.gov.pl/web/gddkia/"
    pat = r"\.(xml|xsd|xlsx?|csv|zip|json)\b|/attachment/|archiwum\.gddkia|api|xml"
    for sub in ("dane-do-pobrania", "biezaca-realizacja-pbdk-na-lata-2014-2023", "wykaz-parkingow-i-mop", "dane-o-utrudnieniach-na-drogach-krajowych", "dane-xml"):
        step(o, sub[:22], page, b + sub, pat, 7, r"XML|aktualiz|licenc|wykorzyst", 2, 220, 0, 2, 50)
    step(o, "msbd", page, b + "msbd", r"pobierz|\.xlsx?|\.csv|/attachment/", 5, r"Pobierz dane|km dróg|w realizacji", 2, 200)
    step(o, "archiwum_xml", lambda: meta(get("https://www.archiwum.gddkia.gov.pl/dane/zima_html/utrdane.xml", n=3000)))
    return o


def cepik_api():
    o = {}
    r = get("https://www.gov.pl/web/cepik/api-dla-centralnej-ewidencji-pojazdow-i-kierowcow-api-do-cepik")
    o["gov_page"] = around(text(r), r"komercyj|źródł|ograniczen|limit|zapyta\w+ na", 150, 480, 3) if "body" in r else r["err"]
    step(o, "robots", robots_txt, "https://api.cepik.gov.pl")
    step(o, "doc", lambda: meta(get("https://api.cepik.gov.pl/doc", n=20000)))
    sw_j = None
    for cand in ("https://api.cepik.gov.pl/swagger/apicepik.json", "https://api.cepik.gov.pl/doc/swagger.json", "https://api.cepik.gov.pl/v2/api-docs"):
        j, m = jget(cand, n=4000000)
        if j:
            sw_j = j
            o["swagger"] = short(cand)
            break
        o["swagger_try:" + cand.rsplit("/", 1)[-1]] = m
    if isinstance(sw_j, dict):
        paths = sw_j.get("paths", {})
        o["info"] = {k: str(v)[:160] for k, v in sw_j.get("info", {}).items() if k in ("title", "version", "description", "termsOfService", "license")}
        o["paths"] = list(paths)[:30]
        pj = paths.get("/pojazdy", {}).get("get", {})
        o["pojazdy_params"] = [f"{p.get('name')}{'*' if p.get('required') else ''}:{str(p.get('description'))[:110]}" for p in pj.get("parameters", [])][:12]
    def call(u, n=3000000):
        j, m = jget(u, n=n)
        if j is None:
            return m
        d = j.get("data") if isinstance(j, dict) else None
        out = {"meta": j.get("meta"), "links": list(j.get("links") or {})[:6], "errors": str(j.get("errors"))[:300] if j.get("errors") else None} if isinstance(j, dict) else {"type": type(j).__name__}
        if isinstance(d, list) and d and isinstance(d[0], dict):
            out["n"] = len(d)
            out["ids"] = [str(x.get("id"))[:30] for x in d[:25]]
            out["attr_keys"] = len(d[0].get("attributes") or {})
        out["sample"] = json.dumps(j, ensure_ascii=False)[:220]
        out["hdr"] = m.get("rl") if isinstance(m, dict) else None
        return {k: v for k, v in out.items() if v not in (None, [], {})}

    step(o, "pojazdy_call", call, "https://api.cepik.gov.pl/pojazdy?wojewodztwo=14&data-od=20250101&data-do=20250102&limit=1&page=1")
    step(o, "pojazdy_3y_window", call, "https://api.cepik.gov.pl/pojazdy?wojewodztwo=14&data-od=20230101&data-do=20251231&limit=1")
    step(o, "slowniki", call, "https://api.cepik.gov.pl/slowniki")
    for stat in ("statystyki/pojazdy", "statystyki", "statystyki/pojazdy/14"):
        step(o, stat, call, "https://api.cepik.gov.pl/" + stat, 200000)
    return o


def cepik_stats():
    o = {}
    r = get("https://www.gov.pl/web/cepik/statystyki", n=1500000)
    o["stats"] = meta(r) if "err" in r else r["st"]
    if "body" in r:
        ls = links(text(r), r"/web/cepik/(pojazdy|prawa-jazdy|stacje|kierowcy|uprawnien)", 60, "https://www.gov.pl", 60)
        o["n_pages"] = len(ls)
        o["pages"] = [x.split("|")[0] + "|" + x.rsplit("/", 1)[-1] for x in ls[:9]]
    for key, u in (("veh2026", "https://www.gov.pl/web/cepik/pojazdy-zarejestrowane-w-2026-roku"),):
        t, a = gov_attachments(u, n=8, cut=70)
        urls = a.pop("_urls", [])
        o[key] = a
        if t:
            o[key]["kw"] = around(t, r"licenc|wykorzyst|źródł|stan na|aktualiz", 60, 200, 1)
            o[key]["head"] = [head(x) for x in urls[:1]]
            if urls:
                step(o, "xlsx", xlsx_peek, urls[0], 7, 3000000, 20, 9)
    for key, u in (("gov_copyright", "https://www.gov.pl/web/gov/prawa-autorskie"), ("gov_terms", "https://www.gov.pl/web/gov/warunki-korzystania")):
        rr = get(u)
        o[key] = around(text(rr), r"materiałów urzędowych|nie wymaga zgody|Creative Commons|ponown\w+ wykorzyst|osobom trzecim", 160, 520, 3) if "body" in rr else rr["err"]
    step(o, "dane_gov", dgp_search, "CEPiK pojazdy zarejestrowane")
    return o


def utk():
    o = {}
    b = "https://dane.utk.gov.pl"
    step(o, "robots", robots_txt, b)
    r = get(b + "/")
    o["home"] = meta(r) if "err" in r else r["st"]
    secs = []
    if "body" in r:
        t = text(r)
        o["js"] = js_only(r)
        secs = links(t, r"/sts/[a-z0-9-]+", 80, b + "/", 34)
        o["n_sections"] = len(secs)
        o["sections"] = [x.split("|")[0] + "|" + x.split("/sts/")[-1][:40] for x in secs[:16]]
        ll = links(t, LEGAL + r"|dostępno", 6, b + "/", 30)
        o["legal_links"] = [short(x) for x in ll[:5]]
        for x in ll[:2]:
            lr = get(x.split("|")[-1])
            o["legal:" + short(x.split("|")[-1])[:50]] = around(text(lr), KW, 80, 420, 2) if "body" in lr else lr["err"]
    done = 0
    for x in secs:
        u = x.split("|")[-1]
        if not re.search(r"przewozy-pasaz|przewozy-towar|punktualn|eksploatac", u) or done >= 3:
            continue
        done += 1
        pr = get(u, n=1500000)
        if "err" in pr:
            o["sec:" + u.split("/sts/")[-1][:30]] = pr["err"]
            continue
        pt = text(pr)
        fl = links(pt, r"\.(xlsx?|csv|ods|zip)\b|/download/", 300, b + "/", 44)
        sub = links(pt, r"/sts/.+/\d+,", 300, b + "/", 44)
        o["sec:" + u.split("/sts/")[-1][:30]] = {"files": len(fl), "first": [short(z)[:130] for z in fl[:2]], "subpages": len(sub), "sub_first": [short(z)[:110] for z in sub[:2]]}
        tgt = fl
        if not fl and sub:
            sr = get(sub[0].split("|")[-1], n=1500000)
            if "body" in sr:
                tgt = links(text(sr), r"\.(xlsx?|csv|ods|zip)\b|/download/", 300, b + "/", 44)
                o["sub:" + u.split("/sts/")[-1][:24]] = {"files": len(tgt), "first": [short(z)[:130] for z in tgt[:2]], "date": around(text(sr), r"Data publikacji|aktualiz", 10, 80, 1)}
        if tgt and "xlsx" not in o:
            xs = [z for z in tgt if re.search(r"\.xlsx", z)] or tgt
            step(o, "xlsx", xlsx_peek, xs[0].split("|")[-1], 6, 6000000, 20, 9)
    step(o, "dgp_128", dgp, 128)
    return o


def ulc():
    o = {}
    b = "https://ulc.gov.pl"
    step(o, "robots", robots_txt, b)
    r = get(b + "/statystyki-analizy/statystyki-wg-portow-lotniczych", n=1500000)
    o["hub"] = meta(r) if "err" in r else r["st"]
    if "body" in r:
        t = text(r)
        ls = links(t, r"_download/.*\.(pdf|xlsx?|csv)", 500, b + "/", 36)
        ext = {}
        for x in ls:
            e = x.rsplit(".", 1)[-1].lower()
            ext[e] = ext.get(e, 0) + 1
        o["n"], o["ext"] = len(ls), ext
        o["first"] = [short(x) for x in ls[:2]]
        o["last"] = [short(x) for x in ls[-1:]]
        o["head"] = [head(x.split("|")[-1]) for x in ls[:2]]
        o["updated"] = around(t, r"Aktualizacja", 5, 60, 1)
        ll = links(t, LEGAL + r"|dostępno|polityk", 8, b + "/", 30)
        o["legal_links"] = [short(x) for x in ll[:6]]
        for x in ll[:2]:
            lr = get(x.split("|")[-1])
            o["legal:" + short(x.split("|")[-1])[:50]] = around(text(lr), KW, 70, 330, 1) if "body" in lr else lr["err"]
    step(o, "other_stats", page, b + "/statystyki-analizy/statystyki-ruch-krajowy", r"\.xlsx?|\.csv", 4)
    step(o, "stats_root", page, b + "/statystyki-analizy", r"\.xlsx?|\.csv|statystyki-", 10, None, 0, 0, 0, 0, 34)
    step(o, "bip_reuse", page, "https://bip.ulc.gov.pl/", r"ponown", 3, KW, 1)
    return o


def port_waw():
    o = {}
    p = {}
    step(p, "robots", robots_txt, "https://www.portgdansk.pl")
    step(p, "facts", page, "https://www.portgdansk.pl/en/business/general-information/facts-and-figures/", r"\.(xlsx?|csv|pdf)\b|statyst", 5, r"rights reserved|prawa zastrzeżone|©", 1, 120)
    step(p, "stat_pl", page, "https://www.portgdansk.pl/biznes/informacje-ogolne/fakty-i-liczby/", r"\.(xlsx?|csv|pdf)\b|statyst", 5, r"prawa zastrzeżone|©", 1, 120)
    o["port"] = p
    w = {}
    for h in ("https://api.um.warszawa.pl", "https://dane.um.warszawa.pl"):
        step(w, "robots_" + h.split("//")[1][:8], robots_txt, h)
    step(w, "api_home", page, "https://api.um.warszawa.pl/", r"regulamin|rejestr|apikey|zaloguj|dane\.um", 6, r"apikey|rejestrac|zastąpi|nowy serwis", 2, 220, 0, 0, 36)
    step(w, "dane_home", page, "https://dane.um.warszawa.pl/", r"regulamin|rejestr|apikey|zaloguj|api|gtfs|rozkład", 8, r"apikey|klucz|rejestrac|licenc|regulamin", 2, 220, 1, 0, 36)
    j, m = jget("https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=ab75c33d-3a26-4342-b36a-6e5fef0a3ac3&name=Marszalkowska", n=3000)
    w["no_key_call"] = m if j is None else json.dumps(j, ensure_ascii=False)[:200]
    j, m = jget("https://dane.um.warszawa.pl/api/3/action/package_search?q=gtfs&rows=3", n=400000)
    w["ckan_new"] = m if j is None else {"count": j.get("result", {}).get("count"), "hits": [f"{d.get('name')}|{d.get('license_title')}" for d in j.get("result", {}).get("results", [])[:3]]}
    step(w, "ztm_files", page, "https://www.ztm.waw.pl/pliki-do-pobrania/dane-rozkladowe/", r"rozklady|\.7z|ftp|\.zip|gtfs", 5, r"wykorzyst|licenc|warunk|format", 2, 260)
    o["waw"] = w
    return o


def ckan_pkg(base, pkg, pat=r"gtfs|\.zip|regulamin|licen"):
    j, m = jget(f"{base}/api/3/action/package_show?id={pkg}", n=4000000)
    if j is None:
        return None, m
    res = j.get("result", {})
    rs = res.get("resources", [])
    out = {"licence": f"{res.get('license_id')}|{res.get('license_title')}|{res.get('license_url')}", "modified": res.get("metadata_modified"), "n_res": len(rs), "org": (res.get("organization") or {}).get("title"), "notes": plain(str(res.get("notes")))[:200]}
    out["res"] = [f"{str(x.get('name'))[:36]}|{x.get('format')}|{str(x.get('last_modified'))[:10]}|{short(str(x.get('url')))[-70:]}" for x in rs if re.search(pat, json.dumps(x), re.I)][:9]
    return rs, out


def gdansk():
    o = {}
    b = "https://ckan.multimediagdansk.pl"
    step(o, "robots", robots_txt, b)
    rs, o["ckan"] = ckan_pkg(b, "tristar", r"gtfs|regulamin")
    step(o, "gtfs_zip", zip_peek, b + "/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip", 80000000, True, 0)
    if rs:
        reg = [x for x in rs if re.search(r"regulamin", json.dumps(x, ensure_ascii=False), re.I)]
        if reg:
            u = reg[0].get("url")
            rr = get(u, n=600000)
            o["regulamin"] = {"url": short(u), "meta": meta(rr), "text": around(text(rr), KW + r"|bezpłat|komerc", 80, 420, 3) if "body" in rr and rr["body"][:4] != b"%PDF" else "pdf or unreadable"}
    step(o, "city_page", page, "https://www.gdansk.pl/otwarte-dane-w-gdansku", r"regulamin|licenc|ckan|ztm", 6, KW + r"|regulamin", 2, 380, 0, 0, 40)
    step(o, "ztm_page", page, "https://ztm.gda.pl/otwarty_ztm", r"regulamin|licenc|ckan|\.pdf|gtfs", 6, KW + r"|regulamin|bezpłat", 2, 420, 0, 0, 40)
    return o


def poznan():
    o = {}
    b = "https://www.ztm.poznan.pl"
    step(o, "robots", robots_txt, b)
    for key, u in (("terms_index", b + "/pl/dla-deweloperow/index"), ("otwarte_dane", b + "/otwarte-dane/")):
        r = get(u)
        if "err" in r:
            o[key] = r["err"]
            continue
        t = text(r)
        o[key] = {"final": short(r["final"]) if r.get("final") else None, "kw": around(t, KW + r"|regulamin|bezpłat|nieodpłat|zgod\w+ ZTM|komerc", 100, 480, 3), "links": [short(x) for x in links(t, r"regulamin|licen|gtfs|\.pdf|dokumentacja", 7, b + "/", 34)]}
    r = get(b + "/pl/dla-deweloperow/gtfsFiles", n=1500000)
    if "err" in r:
        o["gtfsFiles"] = r["err"]
    else:
        ls = links(text(r), r"getGTFSFile|\.zip", 2000, b + "/", 40)
        o["gtfsFiles"] = {"n": len(ls), "first": [short(x) for x in ls[:2]], "last": [short(x) for x in ls[-1:]]}
    step(o, "gtfs_zip", zip_peek, b + "/pl/dla-deweloperow/getGTFSFile", 80000000, True, 0)
    step(o, "gtfs_rt", page, b + "/otwarte-dane/gtfs-rt/", r"\.pb|vehicle|trip_updates|feeds|getGtfsRt", 5, r"aktualiz|co \d+ sekund|licenc|wykorzyst", 2, 240, 0, 1, 34)
    return o


def wroclaw():
    o = {}
    for base in ("https://opendata.cui.wroclaw.pl", "https://www.wroclaw.pl/open-data"):
        key = "cui" if "cui" in base else "wroclaw_pl"
        step(o, "robots_" + key, robots_txt, base.split("/open-data")[0])
        j, m = jget(f"{base}/api/3/action/package_search?q=gtfs&rows=4", n=3000000)
        if j is None:
            o[key] = m
            continue
        hits = j.get("result", {}).get("results", [])
        o[key] = {"count": j.get("result", {}).get("count"), "hits": []}
        zips = []
        for d in hits[:4]:
            rs = d.get("resources", [])
            zs = [x.get("url") for x in rs if re.search(r"\.zip|gtfs", str(x.get("url")) + str(x.get("format")), re.I)]
            zips += zs
            o[key]["hits"].append({"name": d.get("name"), "lic": f"{d.get('license_id')}|{d.get('license_title')}", "mod": str(d.get("metadata_modified"))[:10], "n_res": len(rs), "zip": [short(str(z))[-80:] for z in zs[:2]], "res_mod": [str(x.get("last_modified"))[:10] for x in rs[:2]]})
        if zips and "gtfs_zip" not in o:
            step(o, "gtfs_zip", zip_peek, zips[0], 80000000, True, 0)
    step(o, "hdb_51", page, "https://open-data.cui.wroclaw.pl/hdb/metadane/51/", r"\.csv|\.json|\.zip|licenc", 5, r"licenc|Creative|aktualiz|warunk", 2, 260)
    step(o, "portal_terms", page, "https://opendata.cui.wroclaw.pl/", r"regulamin|licenc|warunki|about|o-portalu", 6, KW + r"|regulamin", 2, 380, 1, 0, 34)
    step(o, "dgp_80", dgp, 80)
    return o


def rwdz():
    o = {}
    b = "https://wyszukiwarka.gunb.gov.pl"
    step(o, "robots", robots_txt, b)
    r = get(b + "/pobranie.html", n=1500000)
    o["pobranie"] = meta(r) if "err" in r else r["st"]
    urls = []
    if "body" in r:
        t = text(r)
        o["js"] = js_only(r)
        ls = links(t, r"\.(zip|csv|7z|gz)\b", 200, b + "/", 44)
        urls = [x.split("|")[-1] for x in ls]
        o["n_files"] = len(ls)
        o["files"] = [short(x) for x in ls[:6]]
        o["text"] = around(t, r"aktualiz|codzienn|RODO|osobow|licenc|wykorzyst|bezpłat|Pliki", 100, 420, 3)
    if not urls:
        hr = get(b + "/", n=1500000)
        o["home"] = meta(hr) if "err" in hr else {"st": hr["st"], "js": js_only(hr), "links": [short(x) for x in links(text(hr), r"pobra|\.zip|\.csv|api|regulamin|pomoc", 8, b + "/", 34)]}
        for cand in (b + "/pliki_pelne/wynik_zgloszenia.zip", b + "/pliki_pelne/wynik_dolnoslaskie.zip", b + "/pliki_pelne/wynik_opolskie.zip"):
            o["guess:" + cand.rsplit("/", 1)[-1]] = head(cand)
            if isinstance(o["guess:" + cand.rsplit("/", 1)[-1]], dict):
                urls.append(cand)
    sizes = []
    for u in urls[:20]:
        h = head(u)
        if isinstance(h, dict) and h.get("len"):
            sizes.append((int(h["len"]), u, h.get("lm")))
    if sizes:
        sizes.sort()
        o["sizes_mb"] = {s[1].rsplit("/", 1)[-1][:28]: round(s[0] / 1e6, 1) for s in sizes[:20]}
        o["lm"] = sorted({str(s[2]) for s in sizes})[:3]
        step(o, "smallest", zip_peek, sizes[0][1], 80000000, False, 3, r"\.csv$")
    step(o, "api_docs", lambda: meta(get("https://dev.wyszukiwarka.gunb.gov.pl/docs", n=20000)))
    j, m = jget("https://dev.wyszukiwarka.gunb.gov.pl/openapi.json", n=3000000)
    o["openapi"] = m if j is None else {"info": {k: str(v)[:200] for k, v in j.get("info", {}).items()}, "paths": list(j.get("paths", {}))[:16]}
    step(o, "terms", page, b + "/", r"regulamin|polityk|pomoc|instrukc", 5, KW + r"|regulamin|jawn", 2, 380, 2, 0, 34)
    return o


GROUPS = [
    [("PLG-020 PLG-021", uokik), ("PLG-025", senat)],
    [("PLG-028", nik)],
    [("PLG-029 a", pkw_a), ("PLG-029 b", pkw_b)],
    [("PLG-031", isws)],
    [("PLG-032", sw)],
    [("PLG-033", policja)],
    [("EX-003", psp), ("PL-ENV-022", gpr), ("PL-ENV-023", gddkia), ("PL-ENV-024 b", cepik_stats)],
    [("PL-ENV-024 a", cepik_api)],
    [("PL-ENV-025", utk)],
    [("PL-ENV-026", ulc)],
    [("PL-ENV-027 PL-ENV-028", port_waw)],
    [("PL-ENV-029", gdansk)],
    [("PL-ENV-030", poznan)],
    [("PL-ENV-032", wroclaw)],
    [("PL-ENV-033", rwdz)],
]


def run_group(group):
    res = []
    for sid, fn in group:
        try:
            data = fn()
        except Exception as e:  # noqa: BLE001
            data = {"probe_error": f"{type(e).__name__}: {str(e)[:200]}"}
        res.append((sid, data))
    return res


def emit(title, parts):
    """One notice for a group of results; each part is cut to its share of the annotation size."""
    blobs = [(sid, json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)) for sid, data in parts]
    room = LIMIT - 20 * len(blobs)
    sizes = [len(b.encode("utf-8")) for _, b in blobs]
    share = {i: sizes[i] for i in range(len(blobs))}
    while sum(share.values()) > room:
        big = max(share, key=share.get)
        others = sum(v for k, v in share.items() if k != big)
        share[big] = max(room - others, room // len(blobs))
        if all(v <= room // len(blobs) for v in share.values()):
            break
    out = []
    for i, (sid, b) in enumerate(blobs):
        raw = b.encode("utf-8")
        if len(raw) > share[i]:
            b = raw[: share[i] - 4].decode("utf-8", "ignore") + "…"
        out.append(f"[{sid}] {b}")
    msg = " ".join(out).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{msg}", flush=True)


def main():
    results = []
    with cf.ThreadPoolExecutor(len(GROUPS)) as ex:
        for res in ex.map(run_group, GROUPS):
            results += res
    # At most 10 annotations per step: 20 results in 10 notices, a long result paired with a short one.
    size = lambda item: len(json.dumps(item[1], default=str, ensure_ascii=False).encode("utf-8"))  # noqa: E731
    ordered = sorted(results, key=size, reverse=True)
    half = len(ordered) // 2
    pairs = [[ordered[i], ordered[-1 - i]] for i in range(half)]
    if len(ordered) % 2:
        pairs[-1].append(ordered[half])
    for pair in pairs[:10]:
        emit("probe " + " + ".join(sid for sid, _ in pair), pair)


if __name__ == "__main__":
    main()
