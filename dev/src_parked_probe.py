"""Read-only probe for the re-review of parked register entries, batch 1 (26 Nov 2026).

PLS-008, PLS-016, PLS-018, PLS-019, PLS-024, PLS-025, PLS-030, PLS-033, PLG-013, PLG-015, PLG-019, PLG-027.
GET and HEAD only, one request at a time, at least a second apart per host, robots.txt honoured; a refused,
walled or timed-out host is reported and not asked again. Page text and links go to the job log; at most ten
notices carry the summary.
"""

import html
import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
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



def emit(title, data):
    msg = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    b = msg.encode("utf-8")
    if len(b) > LIMIT:
        msg = b[: LIMIT - 4].decode("utf-8", "ignore") + "…"
    msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{msg}", flush=True)


LIC = r"(?i)licencj|licenc|license|prawa autorskie|wszelkie prawa|all rights|copyright|ponowne wykorzyst|creative commons|cc by|warunki korzystania|regulamin|kopiowanie|rozpowszechnian"
DATA = r"(?i)pobierz|do pobrania|download|eksport|export|api|dane\.gov\.pl|otwarte dane|xlsx|csv"


def probe_page(url, text=2500, follow_files=3):
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
    return {"chars": len(p), "scripts": len(re.findall("(?i)<script", t)), "ext": ext, "tab_files": [u[-90:] for _, u in tab[:6]],
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


ENTRIES = {
    "PLS-008": {"pages": ["https://api.stat.gov.pl/Home/RegonApi", "https://api.stat.gov.pl/Home/RegonApi?lang=en"], "dane": ["REGON"]},
    "PLS-016": {"pages": ["https://ezdrowie.gov.pl/portal/home/badania-i-dane", "https://ezdrowie.gov.pl/portal/artykul/e-zdrowie-w-liczbach-e-recepty"], "dane": ["Centrum e-Zdrowia", "e-recepty"], "inst": ["e-Zdrowia"]},
    "PLS-018": {"pages": ["https://www.gov.pl/web/gis/raport---stan-sanitarny-kraju"], "dane": ["Główny Inspektorat Sanitarny", "stan sanitarny"], "inst": ["Inspektorat Sanitarny"]},
    "PLS-019": {"pages": ["https://wwwold.pzh.gov.pl/oldpage/epimeld/index_p.html", "https://www.pzh.gov.pl/"], "dane": ["choroby zakaźne", "zachorowania"], "inst": ["Zdrowia Publicznego"]},
    "PLS-024": {"pages": ["https://ela.nauka.gov.pl/pl"], "app": "https://ela.nauka.gov.pl/pl", "dane": ["absolwentów", "ELA"]},
    "PLS-025": {"pages": ["https://cke.gov.pl/egzamin-osmoklasisty/wyniki/", "https://cke.gov.pl/egzamin-maturalny/egzamin-w-nowej-formule/wyniki/", "https://mapa.wyniki.edu.pl/"],
                "dane": ["egzaminu ósmoklasisty", "egzamin maturalny"], "inst": ["Komisja Egzaminacyjna"],
                "heads": ["https://stat.gov.pl/download/gfx/portalinformacyjny/pl/defaultaktualnosci/5488/18/1/1/srednie_wyniki_uczniow_na_egzaminie_osmoklasisty.csv"]},
    "PLS-030": {"pages": ["https://barometrzawodow.pl/", "https://barometrzawodow.pl/en/module/forecasts-on-maps", "https://barometrzawodow.pl/forecast-card-zip/2026/"], "app": "https://barometrzawodow.pl/en/module/forecasts-on-maps", "dane": ["barometr zawodów"]},
    "PLS-033": {"pages": ["https://www.gov.pl/web/kowr/Rynek-zboz", "https://www.gov.pl/web/kowr/Rynek-miesa"], "dane": ["Krajowy Ośrodek Wsparcia Rolnictwa"], "inst": ["Wsparcia Rolnictwa"]},
    "PLG-013": {"pages": ["https://rdf-przegladarka.ms.gov.pl/"], "dane": ["sprawozdania finansowe", "Repozytorium Dokumentów Finansowych"]},
    "PLG-015": {"pages": ["https://www.podatki.gov.pl/crbr/", "https://crbr.podatki.gov.pl/"], "dane": ["beneficjentów rzeczywistych", "CRBR"]},
    "PLG-019": {"pages": ["https://www.knf.gov.pl/dla_konsumenta/ostrzezenia_publiczne"], "dane": ["ostrzeżeń publicznych", "Komisja Nadzoru Finansowego"]},
}


def main():
    res: dict[str, dict] = {}
    for eid, spec in ENTRIES.items():
        out: dict[str, object] = {}
        try:
            for u in spec.get("pages", []):
                out[tag(u)[:60]] = probe_page(u)
            if spec.get("app"):
                out["app"] = app_scan(spec["app"])
            for u in spec.get("heads", []):
                h = get(u, method="HEAD")
                out["head " + u[-50:]] = {"st": h.get("st") or h.get("err"), "len": h.get("len"), "lm": h.get("lm"), "ct": h.get("ct")}
            dn = {}
            for q in spec.get("dane", []):
                dn[q] = dane(q)
            for q in spec.get("inst", []):
                dn["inst:" + q] = dane(q, "institutions")
            out["dane"] = dn
            top = []
            for q, v in dn.items():
                if isinstance(v, dict) and not q.startswith("inst:"):
                    top += [x["id"] for x in v.get("rows", [])[:2] if x.get("id")]
            out["dane_ds"] = {i: dane_ds(i) for i in list(dict.fromkeys(top))[:3]}
        except Exception as e:  # noqa: BLE001
            ERR.append(f"{eid}: {type(e).__name__}: {str(e)[:120]}")
            print(f"=== {eid} FAILED {type(e).__name__}: {e}", flush=True)
        res[eid] = out
    ids = list(res)
    groups = [ids[0:2], ids[2:3], ids[3:4], ids[4:5], ids[5:6], ids[6:7], ids[7:8], ids[8:10], ids[10:11]]
    for g in groups:
        emit("parked " + ",".join(g), {k: res[k] for k in g})
    emit("parked robots errors totals", {"robots": {h.split("//")[1]: t[:160] for h, t in ROBOTS_TXT.items()}, "requests": COUNT[0], "dead": DEAD, "errors": ERR[:25]})


def app_scan(url, bundles=4):
    """JavaScript application: address-like strings in its own bundles (API routes, download paths)."""
    got = page(url, text=300)
    if not got:
        return {"err": "failed"}
    t = got[2]
    host = urllib.parse.urlsplit(url).netloc
    srcs = [urllib.parse.urljoin(url, html.unescape(s)) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', t)]
    srcs = [s for s in srcs if urllib.parse.urlsplit(s).netloc == host]
    hints: list[str] = []
    for s in srcs[:bundles]:
        r = get(s, 15000000, 60)
        if "err" in r:
            continue
        b = r["body"].decode("utf-8", "replace")
        for m in re.findall(r'["\'`](/?(?:api|rest|download|export|pobierz|files?|dane|data)[A-Za-z0-9_\-/\.{}$?=&]*)["\'`]', b):
            if m not in hints:
                hints.append(m)
        for m in re.findall(r'https?://[A-Za-z0-9\.\-]+(?:/[A-Za-z0-9_\-/\.]*)?', b):
            if m not in hints and not re.search(r"w3\.org|reactjs|github|mozilla|googleapis|schema\.org", m):
                hints.append(m)
    print(f"=== APP {url} scripts={srcs}\nHINTS {hints[:200]}\n=== END", flush=True)
    return {"scripts": [s[-60:] for s in srcs[:6]], "hints": hints[:40]}


if __name__ == "__main__":
    main()
