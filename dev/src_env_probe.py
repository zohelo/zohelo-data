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


# ---------------------------------------------------------------- PL-ENV-021
TP = "https://transparency.entsog.eu"
API = TP + "/api/v1/operationaldatas?operatorKey=PL-TSO-0002&indicator=Physical%20Flow&periodType=day&timezone=WET&limit=-1"


def entsog_site():
    out = {"robots": {h: robots_text(h) for h in (TP, "https://www.gaz-system.pl", "https://swi.gaz-system.pl")}}
    r = get(TP + "/", n=2000000)
    out["home"] = meta(r)
    t = text(r)
    pdfs, terms = set(re.findall(r"[\w/.\-]+\.pdf", t)), []
    scripts = [urllib.parse.urljoin(TP + "/", s) for s in re.findall(r'(?i)<script[^>]+src=["\']([^"\']+)', t)]
    scripts = [s for s in scripts if urllib.parse.urlsplit(s).netloc == "transparency.entsog.eu"]
    out["scripts"] = len(scripts)
    sized = []
    for s in scripts[:12]:
        h = get(s, method="HEAD")
        sized.append((int(h.get("len") or 0) if "err" not in h else 0, s))
    for _, s in sorted(sized, reverse=True)[:4]:
        b = get(s, n=12000000, timeout=90)
        j = text(b)
        pdfs |= set(re.findall(r"[\w/.\-]+\.pdf", j))
        for m in re.finditer(r"(?i)terms[ _&a-z]{0,30}(conditions|use)", j):
            terms.append(re.sub(r"\s+", " ", j[max(0, m.start() - 110) : m.start() + 170]))
            if len(terms) >= 40:
                break
    out["pdf_refs"] = sorted(pdfs)[:25]
    out["terms_ctx"] = [x for x in terms if ".pdf" in x or "href" in x][:3] or terms[:2]
    for name in ("TRA0394_20161115_ENTSOG_TP_TC_of_Use_Rev_2_KNS_LG_KNS.pdf", "Terms_and_Conditions_of_Use.pdf"):
        out[name[:12]] = head(f"{TP}/pdf/{name}")
    out["api_manual_v3"] = head(TP + "/api/archiveDirectories/8/api-manual/ENTSOG_TP_API_UserManual_v3.0.pdf")
    out["gaz_system_home"] = (lambda x: meta(x) if "err" in x else shell(x))(get("https://www.gaz-system.pl/"))
    return out


def entsog_api():
    out = {}
    for label, a, b in (
        ("day 2026-11-01", "2026-11-01", "2026-11-01"),
        ("month 2026-10", "2026-10-01", "2026-10-31"),
        ("day 2021-01-01", "2021-01-01", "2021-01-01"),
        ("day 2018-01-01", "2018-01-01", "2018-01-01"),
        ("day 2014-10-01", "2014-10-01", "2014-10-01"),
    ):
        r = get(f"{API}&from={a}&to={b}", n=60000000, accept="application/json", timeout=120)
        if "err" in r:
            out[label] = meta(r)
            continue
        res = {"st": r["st"], "bytes": len(r["body"]), "sec": r["sec"], "rl": r.get("rl")}
        try:
            d = json.loads(r["body"])
            rows = d.get("operationaldatas") or []
            res["meta"] = d.get("meta")
            res["rows"] = len(rows)
            if rows:
                res["keys"] = sorted(rows[0].keys())[:40] if label.startswith("day 2026") else None
                res["points_x_dir"] = len({(x.get("pointKey"), x.get("directionKey")) for x in rows})
                res["flowStatus"] = dict(collections.Counter(str(x.get("flowStatus")) for x in rows))
                res["units"] = dict(collections.Counter(str(x.get("unit")) for x in rows))
                res["empty_value"] = sum(1 for x in rows if x.get("value") in (None, ""))
                res["isNA"] = dict(collections.Counter(str(x.get("isNA")) for x in rows))
                k = collections.Counter((x.get("pointKey"), x.get("directionKey"), x.get("periodFrom")) for x in rows)
                res["repeated_keys"] = sum(1 for v in k.values() if v > 1)
                res["period_min_max"] = [min(str(x.get("periodFrom")) for x in rows), max(str(x.get("periodFrom")) for x in rows)]
                if label.startswith("month"):
                    by = collections.defaultdict(collections.Counter)
                    for x in rows:
                        by[str(x.get("periodFrom"))[:10]][str(x.get("flowStatus"))] += 1
                    days = sorted(by)
                    res["status_first_last_days"] = {d_: dict(by[d_]) for d_ in days[:2] + days[-2:]}
                    res["lastUpdate_max"] = max(str(x.get("lastUpdateDateTime")) for x in rows)
                if label == "day 2026-11-01":
                    res["sample"] = {k_: rows[0].get(k_) for k_ in ("pointKey", "pointLabel", "directionKey", "unit", "value", "flowStatus", "periodFrom", "periodTo", "lastUpdateDateTime", "isNA")}
        except Exception as e:  # noqa: BLE001
            res["parse"] = f"{type(e).__name__}: {str(e)[:80]}"
            res["head"] = text(r)[:150]
        out[label] = {k: v for k, v in res.items() if v is not None}
    return out


# ---------------------------------------------------------------- PL-ENV-034
GOV = "https://www.gov.pl"
RELEASES = (
    ("q3_2026", GOV + "/web/cyfryzacja/ponad-8-mln-punktow-adresowych-w-zasiegu-internetu-stacjonarnego"),
    ("q2_2026", GOV + "/web/cyfryzacja/coraz-wiecej-punktow-adresowych-w-zasiegu-szybkiego-internetu-stacjonarnego"),
)


def broadband_terms():
    out = {"robots": {h: robots_text(h, 380) for h in ("https://internet.gov.pl", GOV, "https://www.uke.gov.pl")}}
    r = get("https://internet.gov.pl/", n=1500000)
    if "err" in r:
        out["internet_gov_home"] = meta(r)
    else:
        t = text(r)
        out["internet_gov_home"] = {**shell(r), "st": r["st"], "read": len(r["body"]),
                                    "links": [f"{a}|{u}" for a, u in links(t, r"regulamin|dane|statyst|api|pobierz|faq|instrukc", 10, "https://internet.gov.pl/", 30)]}
    r = get(GOV + "/web/gov/prawa-autorskie")
    out["prawa_autorskie"] = meta(r) if "err" in r else {
        "st": r["st"],
        "urzedowe": around(text(r), r"materiał\w* urzędow", 200, 420, 1),
        "licencja": around(text(r), r"Creative Commons", 120, 330, 2),
        "pliki": around(text(r), r"załącznik|plik[ói]|dokument\w* do pobrania|ponown\w* wykorzyst", 100, 250, 2),
    }
    r = get(GOV + "/web/gov/warunki-korzystania")
    out["warunki"] = meta(r) if "err" in r else {"st": r["st"], "cc": around(text(r), r"Creative Commons", 150, 300, 1),
                                                 "pliki": around(text(r), r"załącznik|plik[ói]|ponown\w* wykorzyst", 100, 250, 1)}
    return out


def broadband_releases():
    out = {}
    for label, url in RELEASES:
        r = get(url, n=1500000)
        if "err" in r:
            out[label] = meta(r)
            continue
        t = text(r)
        att = links(t, r"/attachment/", 60, url, 60)
        res = {"st": r["st"], "published": re.findall(r"\d{2}\.\d{2}\.\d{4}", plain(t))[:2], "attachments": len(att),
               "footer_cc": bool(re.search(r"CC BY-SA 4\.0", t))}
        sizes, first = [], None
        for a, u in att[:20]:
            h = head(u)
            if isinstance(h, dict):
                sizes.append(int(h.get("len") or 0))
                if first is None:
                    first = {"name": a, "url": u.replace(GOV, ""), **h}
            else:
                res.setdefault("head_errors", []).append(h)
        res["first"] = first
        res["names"] = [a[:22] for a, _ in att[:18]]
        res["bytes_total"] = sum(sizes)
        res["bytes_max"] = max(sizes) if sizes else None
        res["bytes_min"] = min(sizes) if sizes else None
        out[label] = res
    r = get(GOV + "/web/cyfryzacja/wiadomosci", n=1500000)
    if "err" in r:
        r = get(GOV + "/web/cyfryzacja", n=1500000)
    out["news_list"] = meta(r) if "err" in r else {
        "st": r["st"], "final": r.get("final"),
        "report_links": [f"{a}|{u.replace(GOV, '')}" for a, u in links(text(r), r"zasi[eę]g|internet\w* stacjonarn|punkt\w* adresow", 6, GOV, 60)],
        "news_links": len(links(text(r), r"/web/cyfryzacja/", 400, GOV, 10)),
    }
    return out


def broadband_other():
    out = {}
    out["uke_report_pdf"] = head("https://www.uke.gov.pl/download/gfx/uke/pl/defaultaktualnosci/36/670/11/raport_o_stanie_rynku_komunikacji_elektronicznej_za_2025.pdf")
    r = get("https://www.uke.gov.pl/akt/raport-o-stanie-rynku-komunikacji-elektronicznej-w-2025-roku,670.html")
    out["uke_report_page"] = meta(r) if "err" in r else {"st": r["st"], **shell(r), "files": [f"{a}|{u[-60:]}" for a, u in links(text(r), r"\.(pdf|xlsx?|csv|zip)", 6, "https://www.uke.gov.pl/", 40)]}
    for name, u in (("faq_pdf", "https://internet.gov.pl/media/public/docs/faq.pdf"), ("instrukcja_pdf", "https://internet.gov.pl/media/public/docs/instrukcja.pdf")):
        out[name] = head(u)
    r = get("https://dane.gov.pl/pl/dataset/588", n=600000, timeout=25)
    out["dane_gov_588"] = meta(r) if "err" in r else {"st": r["st"], "final": r.get("final"), **shell(r)}
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
        ("probe PL-ENV-021 site and terms", entsog_site),
        ("probe PL-ENV-021 ENTSOG API", entsog_api),
        ("probe PL-ENV-034 terms and robots", broadband_terms),
        ("probe PL-ENV-034 report releases", broadband_releases),
        ("probe PL-ENV-034 UKE and other", broadband_other),
    ):
        try:
            data = fn()
        except Exception as e:  # noqa: BLE001
            data = {"probe_error": f"{type(e).__name__}: {str(e)[:300]}"}
        emit(title, data)


if __name__ == "__main__":
    main()
