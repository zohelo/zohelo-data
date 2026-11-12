"""Read-only probe for the source review of EX-001 and EX-002 (UPRP), 12 Nov 2026.

GET only, one request at a time, at least a second apart per host, robots.txt honoured (generic agent and
Claude agents); a refused or failing host is reported, never retried another way. Starts from the two register
endpoints, follows only links that look like the statistics API, a search service, or legal/reuse pages, at
most two steps deep and 70 requests. Page text goes to the job log; nine notices carry the summary. No
record-level data is printed: API answers are reported by shape (keys, counts) and a 600-character head.
"""

import html
import json
import os
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
BUDGET = [45]
LIMIT = 3800
FOLLOW = (
    r"api|swagger|openapi|regulamin|ponown|praw\w*[-_ ]autor|otwart\w*[-_ ]dan|licenc|nota[-_ ]prawn|zastrze"
    r"|warunki|polityka|statysty|ewyszukiwarka|e-wyszukiwarka|dane\.gov\.pl|bip\."
)
LEGAL = r"regulamin|ponown|praw\w*[-_ ]autor|licenc|nota[-_ ]prawn|zastrze|warunki[-_ ]korzyst|polityka"
KW = (
    r"ponown\w* wykorzyst|licencj|prawa autorskie|praw autorskich|wykorzyst\w+ (treści|materiał|dan|informacj)"
    r"|podani\w+ źródł|wskazani\w+ źródł|zastrzeżone|Creative Commons|CC BY|CC0|komercyj|robot|automat|masow|hurtow"
)
START = [
    "https://uprp.gov.pl/pl/warunki-korzystania-z-portalu-informacyjnego-urzedu-patentowego-rp-i-platformy-uslug-elektronicznych-urzedu-patentowego-rp",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/zgl_kraj_zag",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/zgl_rodz_pod",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/zgl_woj",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/udziel_kraj_zag",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/udziel_rodz_pod",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/udziel_woj",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/zt_zgl_kraj_zag",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/zt_udz_kraj_zag",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/wp_zgl_kraj_zag",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/wp_udz_kraj_zag",
    "https://statystyka.pue.uprp.gov.pl/team/uprp/zgl_woj",
    "https://statystyka.pue.uprp.gov.pl/",
    "https://uprp.gov.pl/pl/o-urzedzie/statystyki/modul-statystyczny/wynalazki-i-wzory-uzytkowe",
    "https://uprp.gov.pl/pl/o-urzedzie/statystyki/modul-statystyczny/znaki-towarowe",
    "https://uprp.gov.pl/pl/o-urzedzie/statystyki/modul-statystyczny/wzory-przemyslowe",
    "https://uprp.gov.pl/pl/o-urzedzie/statystyki/modul-statystyczny/statystyka-pueup",
    "https://uprp.gov.pl/pl/o-urzedzie/statystyki",
    "https://uprp.gov.pl/pl",
    "https://uprp.gov.pl/pl/uslugi-online",
    "https://ewyszukiwarka.pue.uprp.gov.pl/search/simple-search?lng=pl",
    "https://pue.uprp.gov.pl/",
    "https://ebazawiedzy.pue.uprp.gov.pl/",
    "https://api.uprp.gov.pl/",
]
ALLOWED = ("uprp.gov.pl",)
SECOND = {}
NOTES: dict[str, list] = {}


def raw_get(url, n=3000000, timeout=40):
    netloc = urllib.parse.urlsplit(url).netloc
    wait = 1.0 - (time.time() - LAST.get(netloc, 0))
    if wait > 0:
        time.sleep(wait)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "pl,en"})
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            h = r.headers
            return {
                "st": r.status,
                "ct": (h.get("content-type") or "")[:50],
                "len": h.get("content-length"),
                "lm": h.get("last-modified"),
                "etag": (h.get("etag") or "")[:30] or None,
                "rl": {k: v for k, v in h.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"} or None,
                "final": r.geturl() if r.geturl() != url else None,
                "hdr": {k: v[:90] for k, v in h.items() if k.lower() not in ("set-cookie", "content-security-policy")},
                "body": r.read(n),
            }
    finally:
        LAST[netloc] = time.time()


def robots_ok(url):
    p = urllib.parse.urlsplit(url)
    host = f"{p.scheme}://{p.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            txt = raw_get(host + "/robots.txt", 100000, 20)["body"].decode("utf-8", "replace")
            ROBOTS_TXT[host] = txt[:3000]
            rp.parse(txt.splitlines() if "<html" not in txt[:300].lower() else [])
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


def get(url, n=3000000, timeout=40):
    if BUDGET[0] <= 0:
        return {"err": "probe budget used"}
    BUDGET[0] -= 1
    try:
        no = robots_ok(url)
        if no:
            return {"err": no}
        return raw_get(url, n, timeout)
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"err": f"{type(e).__name__}: {str(e)[:90]}"}


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def links(t, base):
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', t):
        h, a = html.unescape(m.group(1)).strip(), plain(m.group(2))[:70]
        if h.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        u = urllib.parse.urljoin(base, h).split("#")[0]
        if (a, u) not in out:
            out.append((a, u))
    return out


def around(p, pattern, before=120, after=420, limit=4):
    out, last = [], -9999
    for m in re.finditer(pattern, p, re.I):
        if m.start() - last < after:
            continue
        last = m.start()
        out.append(p[max(0, m.start() - before) : m.start() + after])
        if len(out) >= limit:
            break
    return out


def shape(v, depth=0):
    if isinstance(v, dict):
        if depth > 3:
            return f"dict({len(v)})"
        return {str(k)[:40]: shape(x, depth + 1) for k, x in list(v.items())[:25]}
    if isinstance(v, list):
        return [f"list({len(v)})", shape(v[0], depth + 1)] if v else "list(0)"
    return type(v).__name__ + ":" + str(v)[:40]


def rows_of(v, depth=0):
    """The largest list of dicts in a JSON answer."""
    best = []
    if isinstance(v, list):
        if v and all(isinstance(x, dict) for x in v):
            best = v
        for x in v[:50]:
            c = rows_of(x, depth + 1) if depth < 5 and isinstance(x, (dict, list)) else []
            best = c if len(c) > len(best) else best
    elif isinstance(v, dict):
        for x in v.values():
            c = rows_of(x, depth + 1) if depth < 5 and isinstance(x, (dict, list)) else []
            best = c if len(c) > len(best) else best
    return best


def summary(rows):
    out = {"rows": len(rows)}
    keys = []
    for r in rows[:200]:
        for k in r:
            if k not in keys:
                keys.append(k)
    for k in keys[:20]:
        vals = [r.get(k) for r in rows]
        dist = sorted({str(x)[:30] for x in vals if not isinstance(x, (dict, list))})
        nulls = sum(1 for x in vals if x is None or x == "")
        out[str(k)[:30]] = {"n": len(dist), "nulls": nulls, "vals": dist[:22] if len(dist) <= 60 else dist[:4] + ["…"] + dist[-4:]}
    full = [json.dumps(r, sort_keys=True, ensure_ascii=False, default=str) for r in rows]
    out["dup_rows"] = len(full) - len(set(full))
    but_last = [json.dumps({k: r.get(k) for k in keys[:-1]}, sort_keys=True, ensure_ascii=False, default=str) for r in rows]
    out["dup_all_but_last_key"] = len(but_last) - len(set(but_last))
    return out


def note(key, value):
    NOTES.setdefault(key, []).append(value)


def visit(url, depth, queue, seen):
    r = get(url)
    tag = re.sub(r"https?://(www\.)?", "", url)[:110]
    if "err" in r:
        print(f"=== PAGE {url}\nERR {r['err']}\n=== END", flush=True)
        note("errors", f"{tag}: {r['err']}")
        return
    body = r.pop("body")
    hdr = r.pop("hdr", {})
    meta = {k: v for k, v in r.items() if v}
    t = body.decode("utf-8", "replace")
    ct = r["ct"].lower()
    print(f"=== PAGE {url}\nMETA {json.dumps(meta, ensure_ascii=False)} read={len(body)}", flush=True)
    if "json" in ct or t.lstrip()[:1] in ("{", "["):
        sm = None
        try:
            doc = json.loads(t)
            sh = shape(doc)
            rows = rows_of(doc)
            sm = summary(rows) if rows else None
        except ValueError:
            sh = "not json"
        print(f"HEADERS {json.dumps(hdr, ensure_ascii=False)}", flush=True)
        print(f"JSON-SHAPE {json.dumps(sh, ensure_ascii=False)[:2500]}\nSUMMARY {json.dumps(sm, ensure_ascii=False)[:6000]}", flush=True)
        print(f"HEAD {t[:1500]}\nTAIL {t[-700:]}\n=== END", flush=True)
        if url in SECOND:
            note("api", {"u": tag, "second_get_same_bytes": SECOND[url] == body})
            return
        SECOND[url] = body
        note("api", {"u": tag.split("/")[-1], "st": r["st"], "ct": ct[:30], "bytes": len(body), "lm": r.get("lm"), "etag": r.get("etag"), "rl": r.get("rl"), "sum": json.dumps(sm, ensure_ascii=False)[:520]})
        return
    if "html" not in ct and "xml" not in ct and "text" not in ct:
        print(f"BINARY magic={body[:8].hex()}", flush=True)
        out = os.environ.get("DEV_OUTPUT_DIR")
        if out and body[:4] == b"%PDF" and re.search(LEGAL, url, re.I):
            name = re.sub(r"[^A-Za-z0-9._-]+", "_", urllib.parse.unquote(url.rsplit("/", 1)[-1]))[:80]
            with open(os.path.join(out, name), "wb") as fh:
                fh.write(body)
            print(f"SAVED {name}", flush=True)
        print("=== END", flush=True)
        note("files", {"u": tag, "ct": ct[:40], "bytes": len(body), "lm": r.get("lm")})
        return
    if "xml" in ct and "html" not in ct or "csv" in ct or ("text/plain" in ct):
        print(f"HEADERS {json.dumps(hdr, ensure_ascii=False)[:1500]}", flush=True)
        print(f"TEXT-HEAD {t[:4000]}\n=== END", flush=True)
        note("api", {"u": tag, "st": r["st"], "ct": ct[:30], "bytes": len(body), "head": t[:200]})
        return
    p = plain(t)
    title = plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:90]
    main = re.search(r"(?is)<main\b.*?</main>", t)
    pm = plain(main.group(0)) if main else p
    wall = re.findall(r"(?i)incapsula|cf-challenge|captcha|access denied", t[:30000])[:2]
    print(f"TITLE {title}\nCHARS {len(p)} scripts={len(re.findall('(?i)<script', t))} wall={wall}", flush=True)
    print(f"HEADERS {json.dumps(hdr, ensure_ascii=False)[:1500]}", flush=True)
    print(f"TEXT {pm[:30000]}", flush=True)
    for fr in re.findall(r'(?is)<iframe\b[^>]*src=["\']([^"\']+)', t)[:6]:
        print(f"IFRAME {html.unescape(fr)[:300]}", flush=True)
    ll = links(t, r.get("final") or url)
    print("LINKS", flush=True)
    for a, u in ll:
        if re.search(FOLLOW + r"|BUP|WUP|raport|wyszukiwar|publikac|\.(xlsx?|csv|zip|json|xml|pdf)(\?|$)", u + " " + a, re.I):
            print(f"  {a} | {u}", flush=True)
    print("=== END", flush=True)
    kws = around(pm, KW)
    if re.search(LEGAL, url, re.I) or kws:
        note("legal", {"u": tag, "title": title[:50], "kw": [k[:520] for k in kws[:2]]})
    if len(p) < 600 or wall:
        note("thin", {"u": tag, "chars": len(p), "wall": wall, "title": title})
    if depth >= 1:
        return
    for a, u in ll:
        host = urllib.parse.urlsplit(u).netloc
        if not u.startswith("http") or not host.endswith(ALLOWED) or u in seen or " " in u:
            continue
        api_like = re.search(r"/api|api\.|swagger|openapi|otwart\w*[-_ ]dan", u + " " + a, re.I) and not re.search(r"\.(pdf|docx?)(\?|$)|api-do-statystyk|bot_", u, re.I)
        legal_like = re.search(LEGAL, u + " " + a, re.I) and not re.search(r"\.(docx?|zip)(\?|$)", u, re.I)
        stat_like = False
        if api_like or legal_like or stat_like:
            seen.add(u)
            queue.append((u, depth + 1, 0 if api_like else 1 if legal_like else 2))


def emit(title, data):
    msg = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    raw = msg.encode("utf-8")
    if len(raw) > LIMIT:
        msg = raw[: LIMIT - 4].decode("utf-8", "ignore") + "…"
    msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{msg}", flush=True)


def main():
    seen = set(START) | {"https://uprp.gov.pl/pl/form/napisz-do-nas", "https://uprp.gov.pl/pl/polityka-srodowiskowa-urzedu-patentowego-rzeczypospolitej-polskiej"}
    queue = [(u, 0, i) for i, u in enumerate(START)]
    n = 0
    while queue and BUDGET[0] > 0:
        queue.sort(key=lambda x: (x[1], x[2]))
        url, depth, _ = queue.pop(0)
        visit(url, depth, queue, seen)
        n += 1
    left = [u for u, _, _ in queue]
    print(f"=== LEFT {len(left)}\n" + "\n".join(left[:80]), flush=True)
    emit("probe EX robots", {h: t[:700] for h, t in ROBOTS_TXT.items()})
    api = NOTES.get("api", [])
    for i in range(0, min(len(api), 30), 10):
        emit(f"probe EX-001 api {i // 10 + 1}", api[i : i + 10])
    legal = NOTES.get("legal", [])
    for i in range(0, min(len(legal), 12), 4):
        emit(f"probe EX legal {i // 4 + 1}", legal[i : i + 4])
    emit("probe EX files, thin pages, errors", {k: NOTES.get(k, [])[:14] for k in ("files", "thin", "errors")})
    emit("probe EX totals", {"visited": n, "left": len(left), "api": len(api), "legal": len(legal)})


if __name__ == "__main__":
    main()
