"""Read-only reachability probe for the CD source-review batch (stage 1). GET and HEAD only.

robots.txt of every host is read first and honoured for the generic agent and for the Claude agents; a refused
URL is recorded, never retried another way. One request at a time per host. One annotation per group.
"""
import concurrent.futures as cf
import csv
import html
import io
import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser

UA = "Mozilla/5.0 (compatible; zohelo-data-research/1.0; +https://zohelo.com)"
AGENTS = ["zohelo-data-research", "ClaudeBot", "Claude-User", "anthropic-ai"]
OWID = ["consumption-co2-per-capita", "human-development-index", "electoral-democracy", "gdp-per-capita-maddison",
        "life-expectancy", "happiness-cantril-ladder", "ti-corruption-perception-index"]
Q = "?csvType=full&useColumnShortNames=true"

# group -> list of (url, mode, arg)
GROUPS = {
    "CD-034 otomoto infogram": [
        ("https://infogram.com/0826otomotoinsights-1hxj48mj9m8o52v", "infogram", ""),
    ],
}
ROBOTS = {}


def fetch(url, method="GET", n=900000):
    req = urllib.request.Request(url, method=method, headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "pl,en"})
    with urllib.request.urlopen(req, timeout=30, context=ssl.create_default_context()) as r:
        raw = b"" if method == "HEAD" else r.read(n)
        return r, raw


def decode(r, raw):
    m = re.search(r"charset=([\w-]+)", r.headers.get("content-type", ""))
    try:
        return raw.decode(m.group(1) if m else "utf-8", "replace")
    except LookupError:
        return raw.decode("utf-8", "replace")


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def robots(url):
    """Returns (allowed, note) for url; reads the host's robots.txt once."""
    p = urllib.parse.urlsplit(url)
    host = f"{p.scheme}://{p.netloc}"
    if host not in ROBOTS:
        try:
            r, raw = fetch(host + "/robots.txt", n=300000)
            txt = decode(r, raw)
            if "<html" in txt[:500].lower():
                ROBOTS[host] = (None, "robots.txt is an HTML page (no rules)")
            else:
                rp = urllib.robotparser.RobotFileParser()
                rp.parse(txt.splitlines())
                named = sorted(set(re.findall(r"(?im)^user-agent:\s*(\S*(?:claude|anthropic|gptbot|ccbot|ai)\S*)", txt)))
                ROBOTS[host] = (rp, f"robots.txt {r.status}, {len(txt)} chars, AI agents named: {named[:8] or 'none'}")
        except urllib.error.HTTPError as e:
            ROBOTS[host] = (None if e.code in (404, 410) else False, f"robots.txt HTTP {e.code}")
        except Exception as e:  # noqa: BLE001
            ROBOTS[host] = (False, f"robots.txt {type(e).__name__}: {str(e)[:120]}")
    rp, note = ROBOTS[host]
    if rp is False:
        return False, note
    if rp is None:
        return True, note
    refused = [a for a in AGENTS if not rp.can_fetch(a, url)]
    return (not refused), note + (f"; DISALLOWED for {refused}" if refused else "")


def one(url, mode, arg):
    out = [f"== {mode} {url}"]
    ok, note = robots(url)
    out.append("   " + note)
    if not ok:
        out.append("   NOT FETCHED (robots.txt or host refusal)")
        return "\n".join(out)
    try:
        r, raw = fetch(url, "HEAD" if mode == "head" else "GET", 12000000 if mode in ("owid", "infogram") else 900000)
        h = r.headers
        out.append(f"   status={r.status} final={r.geturl()[:120]} ctype={h.get('content-type')} len={h.get('content-length')} "
                   f"lastmod={h.get('last-modified')} etag={h.get('etag')} read={len(raw)}")
        txt = decode(r, raw)
        if mode == "raw":
            out.append("   " + re.sub(r"\s+", " ", txt)[: int(arg)])
        elif mode == "full":
            out.append("   " + plain(txt)[: int(arg)])
        elif mode == "text":
            t = plain(txt) if "<" in txt[:300] else re.sub(r"\s+", " ", txt)
            n, last = 0, -9999
            for m in re.finditer(arg, t, re.I):
                if m.start() - last < 500:
                    continue
                last = m.start()
                out.append("   >> " + t[max(0, m.start() - 160): m.start() + 340])
                n += 1
                if n >= 3:
                    break
            if n == 0:
                out.append(f"   (no match; page text {len(t)} chars: {t[:200]})")
        elif mode == "rawgrep":
            found = []
            for m in re.finditer(arg, txt, re.I):
                s = m.group(0)[:160]
                if s not in found:
                    found.append(s)
                if len(found) >= 10:
                    break
            out.append(f"   matches: {found or 'none'}; html {len(txt)} chars")
        elif mode == "links":
            n = 0
            for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', txt):
                href, a = m.group(1), plain(m.group(2))
                if re.search(arg, href + " " + a, re.I):
                    out.append(f"   {a[:50]} -> {href[:110]}")
                    n += 1
                    if n >= 9:
                        break
            if n == 0:
                out.append(f"   (no links; text: {plain(txt)[:200]})")
        elif mode == "owid":
            rows = list(csv.reader(io.StringIO(txt)))
            head = rows[0] if rows else []
            ci = next((i for i, c in enumerate(head) if c.lower() in ("code", "entity_code")), 1)
            pol = [x for x in rows[1:] if len(x) > ci and x[ci] == "POL"]
            yrs = sorted(x[ci + 1] for x in pol)
            empty = sum(1 for x in rows[1:] for v in x[ci + 2:] if v == "")
            out.append(f"   bytes={len(raw)} rows={len(rows) - 1} header={head[:6]} POL rows={len(pol)} "
                       f"POL years={yrs[0] if yrs else None}..{yrs[-1] if yrs else None} empty value cells={empty} "
                       f"last POL row={pol[-1][:6] if pol else None}")
        elif mode == "infogram":
            m = re.search(r"window\.infographicData\s*=\s*(\{.*?\});\s*</script>", txt, re.S)
            out.append(f"   html {len(txt)} chars; title={plain(''.join(re.findall(r'(?is)<title>.*?</title>', txt)))[:120]}; "
                       f"infographicData found={bool(m)}; download/csv/xlsx words={sorted(set(re.findall(r'(?i)download|\.csv|\.xlsx', txt)))[:6]}")
            if m:
                d = json.loads(m.group(1))
                tables = []

                def walk(o, path):
                    if isinstance(o, dict):
                        for k, v in o.items():
                            if k == "data" and isinstance(v, list) and v and isinstance(v[0], list) and v[0] and isinstance(v[0][0], list):
                                tables.append((path, v))
                            else:
                                walk(v, path + "/" + str(k))
                    elif isinstance(o, list):
                        for i, v in enumerate(o):
                            walk(v, path + f"[{i}]")

                def cell(c):
                    return str(c.get("value", "") if isinstance(c, dict) else c)[:22]

                walk(d, "")
                out.append(f"   json {len(m.group(1))} chars; chart data blocks={len(tables)}; sheets={sum(len(t) for _, t in tables)}; "
                           f"cells={sum(len(r) for _, t in tables for sh in t for r in sh)}")
                for path, t in tables[:12]:
                    sh = t[0]
                    out.append(f"   ..{path[-46:]}: sheets={len(t)} rows={len(sh)} head={[cell(c) for c in sh[0][:5]]} "
                               f"row2={[cell(c) for c in (sh[1][:5] if len(sh) > 1 else [])]} last={[cell(c) for c in sh[-1][:5]]}")
        elif mode == "owidmeta":
            d = json.loads(txt)
            out.append(f"   top keys={list(d)[:8]}")
            for name, c in list((d.get("columns") or {}).items())[:2]:
                out.append(f"   col {name}: keys={list(c)[:14]} unit={c.get('unit')} lastUpdated={c.get('lastUpdated')} "
                           f"nextUpdate={c.get('nextUpdate')} citationShort={str(c.get('citationShort'))[:140]}")
            out.append(f"   'licen' occurrences in metadata={len(re.findall('licen', txt, re.I))}")
    except urllib.error.HTTPError as e:
        out.append(f"   HTTP {e.code} {e.reason}; body: {plain(e.read(600).decode('utf-8', 'replace'))[:160]}")
    except Exception as e:  # noqa: BLE001
        out.append(f"   ERROR {type(e).__name__}: {str(e)[:200]}")
    return "\n".join(out)


def group(item):
    name, urls = item
    res = []
    for u in urls:
        res.append(one(*u))
        time.sleep(1.5)
    return name, "\n".join(res)


if __name__ == "__main__":
    with cf.ThreadPoolExecutor(len(GROUPS)) as ex:
        results = list(ex.map(group, GROUPS.items()))
    for name, body in results:
        print(f"##### {name}\n{body}\n", flush=True)
    for name, body in results:
        parts = [body[:3800]] if "owid" not in name else [body[:3800], body[3800:7600]]
        for i, part in enumerate(p for p in parts if p):
            msg = part.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
            print(f"::notice title={name} ({i + 1})::{msg}", flush=True)
