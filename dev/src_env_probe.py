"""Read-only reachability probe for the source review batch PL-ENV (PL-ENV-021, PL-ENV-034), 12 Nov 2026.

GET/HEAD only, one request at a time per host and at least a second apart, robots.txt honoured (generic
agent and Claude agents). A refused or failing host is reported, never retried another way. Five notices.
"""

import collections
import html
import json
import re
import ssl
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
LIMIT = 3900


def raw_get(url, n=400000, method="GET", accept="*/*", timeout=40):
    netloc = urllib.parse.urlsplit(url).netloc
    wait = 1.2 - (time.time() - LAST.get(netloc, 0))
    if wait > 0:
        time.sleep(wait)
    t0 = time.time()
    try:
        req = urllib.request.Request(url, method=method, headers={"User-Agent": UA, "Accept": accept, "Accept-Language": "pl,en"})
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            body = b"" if method == "HEAD" else r.read(n)
            h = r.headers
            return {
                "st": r.status,
                "ct": (h.get("content-type") or "")[:40],
                "len": h.get("content-length"),
                "lm": h.get("last-modified"),
                "etag": (h.get("etag") or "")[:24] or None,
                "cd": (h.get("content-disposition") or "")[:90] or None,
                "rl": {k: v for k, v in h.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"} or None,
                "final": r.geturl() if r.geturl() != url else None,
                "sec": round(time.time() - t0, 1),
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
            is_html = "<html" in txt[:300].lower() or "<!doctype" in txt[:300].lower()
            ROBOTS_TXT[host] = "html page instead of robots.txt" if is_html else re.sub(r"[ \t]+", " ", txt.strip())
            rp.parse([] if is_html else txt.splitlines())
            ROBOTS[host] = rp
        except urllib.error.HTTPError as e:
            ROBOTS_TXT[host] = f"HTTP {e.code}"
            if e.code in (401, 403):
                ROBOTS[host] = f"robots.txt {e.code}"
            else:
                rp.parse([])
                ROBOTS[host] = rp
        except Exception as e:  # noqa: BLE001
            ROBOTS_TXT[host] = f"{type(e).__name__}: {str(e)[:60]}"
            ROBOTS[host] = "robots.txt " + ROBOTS_TXT[host]
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return rp
    for agent in ("zohelo-data-research", "ClaudeBot", "Claude-User"):
        if not rp.can_fetch(agent, url):
            return f"robots disallow ({agent})"
    return None


def get(url, n=400000, method="GET", accept="*/*", timeout=40):
    try:
        no = robots_ok(url)
        if no:
            return {"err": no}
        return raw_get(url, n, method, accept, timeout)
    except urllib.error.HTTPError as e:
        try:
            body = e.read(300).decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            body = ""
        return {"err": f"HTTP {e.code}", "errbody": re.sub(r"\s+", " ", body)[:160]}
    except Exception as e:  # noqa: BLE001
        return {"err": f"{type(e).__name__}: {str(e)[:70]}"}


def meta(r):
    if "err" in r:
        return r["err"] + (" " + r["errbody"] if r.get("errbody") else "")
    out = {k: r[k] for k in ("st", "ct", "len", "lm", "etag", "cd", "rl", "final", "sec") if r.get(k)}
    if r.get("body"):
        out["read"] = len(r["body"])
    return out


def head(url):
    r = get(url, method="HEAD")
    if "err" in r and r["err"].startswith("HTTP 4"):
        r = get(url, n=1)
    m = meta(r)
    if isinstance(m, dict):
        for k in ("read", "sec", "ct"):
            m.pop(k, None)
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
            item = (a, urllib.parse.urljoin(base, h))
            if item not in out:
                out.append(item)
            if len(out) >= limit:
                break
    return out


def around(t, pattern, before=80, after=320, limit=2):
    p = plain(t)
    out, last = [], -9999
    for m in re.finditer(pattern, p, re.I):
        if m.start() - last < after:
            continue
        last = m.start()
        out.append(p[max(0, m.start() - before) : m.start() + after])
        if len(out) >= limit:
            break
    return out or "no match"


def robots_text(host, cut=420):
    robots_ok(host + "/")
    return ROBOTS_TXT.get(host, "?")[:cut]


def shell(r):
    t = text(r)
    out = {"chars": len(plain(t)), "scripts": len(re.findall(r"(?i)<script", t)), "title": plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:70]}
    wall = re.findall(r"(?i)incapsula|cf-challenge|captcha|cannot be displayed", t)[:2]
    if wall:
        out["bot_wall"] = wall
    return out



# Second pass (same batch): what the first run's annotations cut off or could not answer.
TP = "https://transparency.entsog.eu"
API = TP + "/api/v1/operationaldatas?operatorKey=PL-TSO-0002&indicator=Physical%20Flow&periodType=day&timezone=WET&limit=-1"
LEGAL = r"nota prawna|prawa autorskie|prawn|legal|terms|copyright|regulamin|licenc|warunki korzystania|zastrze"
KW = (r"ponown\w* wykorzyst|licencj|prawa autorskie|praw autorskich|wykorzyst\w+ (treści|materiał|dan|informacj)"
      r"|podani\w+ źródł|zastrzeżone|copyright|reproduc|re-?use|permission|consent|all rights")


def entsog_history():
    out = {}
    for day in ("2020-01-01", "2019-01-01", "2018-01-01", "2017-01-01", "2016-01-01", "2015-01-01", "2014-10-01", "2013-10-01"):
        r = get(f"{API}&from={day}&to={day}", n=20000000, accept="application/json", timeout=90)
        if "err" in r:
            out[day] = meta(r)[:150]
            continue
        try:
            d = json.loads(r["body"])
            rows = d.get("operationaldatas") or []
            m = d.get("meta") or {}
            out[day] = {"st": r["st"], "bytes": len(r["body"]), "rows": len(rows), "total": m.get("total"),
                        "with_value": sum(1 for x in rows if x.get("value") not in (None, "")),
                        "flowStatus": dict(collections.Counter(str(x.get("flowStatus")) for x in rows)),
                        "archived": dict(collections.Counter(str(x.get("isArchived")) for x in rows))}
        except Exception as e:
            out[day] = {"st": r["st"], "parse": f"{type(e).__name__}", "head": text(r)[:120]}
    return out


def legal_pages(home, base, n_pages=4, after=380):
    r = get(home, n=1500000)
    if "err" in r:
        return {"home": meta(r)}
    t = text(r)
    ll = links(t, LEGAL, 10, base, 40)
    out = {"home": r["st"], "legal_links": [f"{a}|{u}" for a, u in ll[:8]]}
    seen = []
    for a, u in ll:
        u = u.split("#")[0]
        if u in seen or urllib.parse.urlsplit(u).netloc != urllib.parse.urlsplit(base).netloc or re.search(r"\.pdf$", u):
            continue
        seen.append(u)
        if len(seen) > n_pages:
            break
        p = get(u, n=1500000)
        out[u.replace(base, "/")[:70]] = meta(p) if "err" in p else {"st": p["st"], "chars": len(plain(text(p))), "kw": around(text(p), KW, 120, after, 3)}
    return out


def gaz_system_legal():
    return legal_pages("https://www.gaz-system.pl/en.html", "https://www.gaz-system.pl/")


def gaz_system_data():
    out = {}
    u = "https://www.gaz-system.pl/en/for-customers/market-information/data-transparency.html"
    r = get(u, n=1500000)
    if "err" in r:
        out["transparency_page"] = meta(r)
    else:
        t = text(r)
        out["transparency_page"] = {
            "st": r["st"], "chars": len(plain(t)),
            "files": [f"{a}|{x}" for a, x in links(t, r"\.(csv|xlsx?|xml|zip|json)(\?|$)", 8, u, 40)],
            "swi_links": [f"{a}|{x}" for a, x in links(t, r"swi\.gaz-system|entsog|transparency", 10, u, 40)],
            "kw": around(t, r"ENTSOG|Transparency Platform", 100, 300, 2),
        }
    r = get("https://swi.gaz-system.pl/mir/", n=300000)
    out["swi_mir"] = meta(r) if "err" in r else {"st": r["st"], **shell(r), "read": len(r["body"])}
    return out


def internet_gov():
    host = "https://internet.gov.pl"
    out = {}
    scripts = []
    for path in ("/stats/", "/faq/"):
        r = get(host + path, n=1500000)
        if "err" in r:
            out[path] = meta(r)
            continue
        t = text(r)
        scripts += [urllib.parse.urljoin(host + path, s) for s in re.findall(r'(?i)<script[^>]+src=["\']([^"\']+)', t)]
        out[path] = {"st": r["st"], "read": len(r["body"]), **shell(r),
                     "text": plain(t)[:260],
                     "links": [f"{a}|{x.replace(host, '')}" for a, x in links(t, r"regulamin|licenc|api|pobierz|eksport|csv|xlsx|docs|swagger|statyst|dane", 10, host, 30)]}
    scripts = [s for s in dict.fromkeys(scripts) if urllib.parse.urlsplit(s).netloc == "internet.gov.pl"]
    out["own_scripts"] = len(scripts)
    apis, ctx = set(), []
    for s in scripts[:8]:
        b = get(s, n=10000000, timeout=90)
        j = text(b)
        apis |= set(re.findall(r"""["'`](/?api/[\w/\-.${}:?=&]{2,80})""", j))
        for m in re.finditer(r"(?i)regulamin|licencj|swagger|openapi|redoc|creative commons", j):
            ctx.append(re.sub(r"\s+", " ", j[max(0, m.start() - 70) : m.start() + 130]))
            if len(ctx) >= 4:
                break
    out["api_paths_in_scripts"] = sorted(apis)[:45]
    out["ctx"] = ctx[:4]
    tried = {}
    for pth in sorted(apis):
        if re.search(r"stat", pth, re.I) and not re.search(r"[${}:?]", pth) and len(tried) < 3:
            u = host + "/" + pth.lstrip("/")
            r = get(u, n=3000000, accept="application/json", timeout=60)
            tried[pth] = meta(r) if "err" in r else {**{k: v for k, v in meta(r).items() if k in ("st", "ct", "read", "sec", "rl")}, "head": text(r)[:420]}
    out["public_stats_calls"] = tried
    return out


def emit(title, data):
    msg = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    raw = msg.encode("utf-8")
    if len(raw) > LIMIT:
        msg = raw[: LIMIT - 4].decode("utf-8", "ignore") + "…"
    msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{msg}", flush=True)


def main():
    for title, fn in (
        ("probe2 PL-ENV-021 ENTSOG history depth", entsog_history),
        ("probe2 PL-ENV-021 GAZ-SYSTEM legal pages", gaz_system_legal),
        ("probe2 PL-ENV-021 GAZ-SYSTEM data pages", gaz_system_data),
        ("probe2 PL-ENV-034 internet.gov.pl", internet_gov),
    ):
        try:
            data = fn()
        except Exception as e:
            data = {"probe_error": f"{type(e).__name__}: {str(e)[:300]}"}
        if title.endswith("internet.gov.pl") and len(json.dumps(data, ensure_ascii=False).encode()) > LIMIT:
            emit(title + " (calls)", {k: data.pop(k) for k in ("public_stats_calls", "ctx") if k in data})
        emit(title, data)


if __name__ == "__main__":
    main()
