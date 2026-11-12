"""Read-only reachability probe for the source review batch PLG, PLS (PLG-016, PLG-034, PLS-022; 12 Nov 2026).

GET/HEAD only, grouped annotations (at most 10 notices). robots.txt is honoured (generic agent and Claude
agents); a refused or failing host is reported, never retried another way. One request at a time per host,
at least a second apart. No personal data is printed: register pages are reported by counts and link
targets only, never by entry text.
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



FILES = r"\.(xlsx?|csv|xml|zip|json|ods)(\?|\s|$)"


def robots_raw(host, n=700):
    try:
        r = raw_get(host + "/robots.txt", 60000, timeout=20)
        t = r["body"].decode("utf-8", "replace")
        if "<html" in t[:300].lower():
            return "html page, not a robots file"
        return re.sub(r"[ \t]+", " ", re.sub(r"\n+", " | ", t.strip()))[:n]
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {str(e)[:60]}"


def app_shell(url):
    """A register sub-site: status, whether it is a script shell, captcha markers, export links. No entry text."""
    r = get(url, n=600000)
    if "err" in r:
        return r["err"]
    t = text(r)
    out = {"st": r["st"], "chars": len(plain(t)), "scripts": len(re.findall(r"(?i)<script", t))}
    out["title"] = plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:50]
    cap = sorted(set(x.lower() for x in re.findall(r"(?i)recaptcha|hcaptcha|captcha|incapsula|cf-challenge", t)))
    if cap:
        out["bot_wall"] = cap
    ex = links(t, FILES + r"|eksport|export|pobierz|csv|xls", 5, r.get("final") or url, 30)
    if ex:
        out["export_links"] = [short(x) for x in ex]
    out["tables"] = len(re.findall(r"(?i)<tr\b", t))
    return out


def knf_main():
    out = {"robots": robots_raw("https://www.knf.gov.pl", 500)}
    r = get("https://www.knf.gov.pl/podmioty", n=900000)
    if "err" in r:
        out["podmioty"] = r["err"]
        return out, [], []
    t = text(r)
    base = r.get("final") or "https://www.knf.gov.pl/podmioty"
    files = links(t, FILES, 50, base, 40)
    subs = [x.split("|")[-1] for x in links(t, r"/podmioty/", 60, base, 40)]
    hosts = sorted(set(re.findall(r"https?://[a-z0-9.-]+\.knf\.gov\.pl", t)) - {"https://www.knf.gov.pl"})
    out["podmioty"] = {"st": r["st"], "chars": len(plain(t)), "files": len(files), "file_links": [short(x) for x in files[:4]],
                       "sub_pages": len(set(subs)), "hosts": [short(h) for h in hosts]}
    n = get("https://www.knf.gov.pl/o_nas/urzad_komisji/nota_prawna")
    out["nota_prawna"] = around(text(n), r"podanie źródła|wykorzystywani\w+ materiał|odpowiedzialno", 200, 420, 2) if "body" in n else n["err"]
    return out, list(dict.fromkeys(subs)), hosts


def knf_subpages(subs):
    out, seen_hosts = {}, set()
    for u in subs[:14]:
        if not u.startswith("https://www.knf.gov.pl/"):
            continue
        r = get(u, n=900000)
        key = short(u).replace("knf.gov.pl/podmioty/", "")[:48]
        if "err" in r:
            out[key] = r["err"]
            continue
        t = text(r)
        f = links(t, FILES, 200, r.get("final") or u, 30)
        seen_hosts |= set(re.findall(r"https?://[a-z0-9.-]+\.knf\.gov\.pl", t))
        item = {"files": len(f), "rows": len(re.findall(r"(?i)<tr\b", t))}
        if f:
            item["first"] = [short(x) for x in f[:2]]
            item["head"] = head(f[0].split("|")[-1])
        out[key] = item
    return out, sorted(seen_hosts - {"https://www.knf.gov.pl"})


def knf_sites(hosts):
    known = ["https://rpu.knf.gov.pl/", "https://wpn.knf.gov.pl/", "https://pok.knf.gov.pl/",
             "https://rpkip.knf.gov.pl/index.html?type=RPK"]
    todo = list(dict.fromkeys(known + [h + "/" for h in hosts if not any(k.startswith(h) for k in known)]))
    out = {}
    for u in todo[:9]:
        host = "{0}://{1}".format(*urllib.parse.urlsplit(u)[:2])
        out[short(u)[:40]] = {"robots": robots_raw(host, 120), "page": app_shell(u)}
    sh = page("https://www.knf.gov.pl/en/ENTITIES/capital_market_entities/register_of_shares", data_pat=r"\.xlsx?(\?|\s|$)", n_data=3, heads=2)
    out["register_of_shares"] = sh
    return out


def err(e):
    return {"probe_error": f"{type(e).__name__}: {str(e)[:300]}"}


def knf():
    res, subs, hosts, more = [], [], [], []
    try:
        a, subs, hosts = knf_main()
    except Exception as e:  # noqa: BLE001
        a = err(e)
    res.append(("PLG-016 main", a))
    try:
        b, more = knf_subpages(subs)
    except Exception as e:  # noqa: BLE001
        b = err(e)
    res.append(("PLG-016 sub-pages", b))
    try:
        c = knf_sites(sorted(set(hosts) | set(more)))
    except Exception as e:  # noqa: BLE001
        c = err(e)
    res.append(("PLG-016 register sites", c))
    return res


def rcl():
    host = "https://legislacja.rcl.gov.pl"
    a = {"robots": robots_raw(host, 1500), "alias_robots": robots_raw("https://legislacja.gov.pl", 200)}
    r = get(host + "/")
    if "err" in r:
        a["home"] = r["err"]
    else:
        t = text(r)
        a["home"] = {"st": r["st"], "chars": len(plain(t)), "final": r.get("final"),
                     "feeds": re.findall(r'(?i)<link[^>]+(?:rss|atom)[^>]*>', t)[:3],
                     "footer": [short(x) for x in links(t, LEGAL + r"|polityk|dostępno|api|rss|pomoc", 8, host, 30)]}
    b = {}
    for key, u in (("lista_bills", host + "/lista?pSize=100&typeId=2"), ("lista_all_p4", host + "/lista?pNumber=4&typeId=1")):
        r = get(u, n=1500000)
        if "err" in r:
            b[key] = r["err"]
            continue
        t = text(r)
        ids = re.findall(r"/projekt/(\d+)", t)
        b[key] = {"st": r["st"], "bytes": len(r["body"]), "project_links": len(set(ids)), "min_id": min(ids, default=None),
                  "max_id": max(ids, default=None), "count_text": around(t, r"znaleziono|liczba (projekt|wynik)|wyników|stron[ay]? \d+ z|\bz \d{3,}", 40, 120, 2),
                  "pager": sorted(set(int(x) for x in re.findall(r"pNumber=(\d+)", t)))[-3:]}
    r = get(host + "/projekt/12398957", n=900000)
    if "err" in r:
        b["projekt"] = r["err"]
    else:
        t = text(r)
        p = plain(t)
        b["projekt"] = {"st": r["st"], "bytes": len(r["body"]), "stages": len(set(re.findall(r"/katalog/(\d+)", t))),
                        "labels": [x for x in ("Tytuł", "Wnioskodawca", "Numer z wykazu", "Data utworzenia", "Status projektu", "Typ", "Dz.U", "Dziennik Ustaw", "druk", "Sejm", "Organ odpowiedzialny") if x.lower() in p.lower()],
                        "ext": sorted(set(urllib.parse.urlsplit(h).netloc for h in re.findall(r'href="(https?://[^"]+)"', t)))[:8],
                        "lm": r.get("lm"), "etag": r.get("etag")}
    terms = "https://bip.rcl.gov.pl/rcl/ponowne-wykorzystywanie/3122,Warunki-ponownego-wykorzystywania-informacji-publicznej.html"
    r = get(terms)
    c = {"robots": robots_raw("https://bip.rcl.gov.pl", 200)}
    if "err" in r:
        c["terms"] = r["err"]
    else:
        t = text(r)
        c["st"] = r["st"]
        c["right"] = around(t, r"każdemu przysługuje prawo", 10, 520, 1)
        c["commercial"] = around(t, r"komercyjnych", 160, 200, 1)
        c["conditions"] = around(t, r"obowiązku każdorazowego", 120, 520, 1)
        c["liability"] = around(t, r"nie ponosi odpowiedzialności", 60, 380, 1)
        c["rpl"] = around(t, r"Rządowy Proces Legislacyjny", 200, 160, 1)
    return [("PLG-034 robots and home", a), ("PLG-034 list and project", b), ("PLG-034 RCL reuse terms", c)]


def cie():
    host = "https://cie.gov.pl"
    a = {"robots": robots_raw(host, 500)}
    r = get(host + "/archiwalne-dane-statystyczne/")
    files, per_page = [], {}
    if "err" in r:
        a["index"] = r["err"]
        subs = []
    else:
        t = text(r)
        subs = list(dict.fromkeys(x.split("|")[-1] for x in links(t, r"/ads-", 20, host + "/archiwalne-dane-statystyczne/", 30)))
        a["index"] = {"st": r["st"], "lm": r.get("lm"), "sub_pages": [short(s) for s in subs], "files_on_index": len(links(t, FILES, 99, host, 20))}
    for s in subs[:8]:
        r = get(s, n=900000)
        if "err" in r:
            per_page[short(s)[-40:]] = r["err"]
            continue
        f = [x.split("|")[-1] for x in links(text(r), r"\.xlsx?(\?|\s|$)", 300, r.get("final") or s, 10)]
        per_page[short(s)[-40:]] = len(f)
        files += [x for x in f if x not in files]
    a["files_per_page"] = per_page
    total, by_ext, fails, lms, biggest = 0, {}, {}, set(), ("", 0)
    for u in files[:130]:
        h = get(u, method="HEAD")
        ext = u.rsplit(".", 1)[-1].lower()
        if "err" in h:
            fails[h["err"]] = fails.get(h["err"], 0) + 1
            continue
        n = int(h.get("len") or 0)
        total += n
        by_ext[ext] = by_ext.get(ext, 0) + 1
        if h.get("lm"):
            lms.add(h["lm"][5:16])
        if n > biggest[1]:
            biggest = (u.rsplit("/", 1)[-1], n)
    a["files"] = {"n": len(files), "bytes": total, "by_ext": by_ext, "fails": fails, "last_modified": sorted(lms)[:4], "biggest": biggest,
                  "dirs": sorted(set(short(u).rsplit("/", 1)[0] for u in files))[:3]}
    names = sorted(u.rsplit("/", 1)[-1] for u in files)
    b = {"names": " ".join(names)[:2400]}
    up = host + "/content/uploads/2020/09/"
    pick = lambda pat: next((u for u in files if re.search(pat, u, re.I)), None)  # noqa: E731
    c = {}
    for key, pat in (("pupils_2018", r"Szkoly_i_uczniowie_wg_organow"), ("graduates_2018", r"Absolwenci_30092018"), ("pupils_woj_xlsx", r"wojew.*201[678].*\.xlsx$")):
        u = pick(pat)
        c[key] = {"file": u.rsplit("/", 1)[-1], **(lambda x: x if isinstance(x, dict) else {"err": x})(xlsx_peek(u, rows=7, cell=26, cols=8))} if u else "no such link"
    for key, pat in (("teachers_xls", r"NauczEtatyOsoby2006-2018"), ("oldest_xls", r"30092010.*\.xls$")):
        u = pick(pat)
        if not u:
            c[key] = "no such link"
            continue
        r = get(u, n=3000000, timeout=90)
        c[key] = r["err"] if "err" in r else {"file": u.rsplit("/", 1)[-1], "bytes": len(r["body"]), "magic": r["body"][:8].hex(), "lm": r.get("lm"), "etag": r.get("etag")}
    d = page(host + "/", kw=KW + r"|©", kw_n=2, kw_after=200, legal_n=3)
    return [("PLS-022 pages and sizes", a), ("PLS-022 file names", b), ("PLS-022 workbooks", c), ("PLS-022 terms on cie.gov.pl", d)]


GROUPS = [knf, rcl, cie]


def run_group(fn):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        return [(fn.__name__, {"probe_error": f"{type(e).__name__}: {str(e)[:300]}"})]


def emit(title, data):
    b = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    raw = b.encode("utf-8")
    if len(raw) > LIMIT:
        b = raw[: LIMIT - 4].decode("utf-8", "ignore") + "…"
    msg = b.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{msg}", flush=True)


def main():
    results = []
    with cf.ThreadPoolExecutor(len(GROUPS)) as ex:
        for res in ex.map(run_group, GROUPS):
            results += res
    for title, data in results[:10]:
        emit("probe " + title, data)


if __name__ == "__main__":
    main()
