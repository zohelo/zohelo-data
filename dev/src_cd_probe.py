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
    "CD-004 nasdaq": [
        ("https://data.nasdaq.com/", "links", r"terms|legal|licen|pricing"),
        ("https://docs.data.nasdaq.com/docs/getting-started", "text", r"premium|free|retired|API key"),
        ("https://docs.data.nasdaq.com/docs/rate-limits-1", "text", r"calls per"),
    ],
    "CD-009 stooq": [
        ("https://stooq.pl/", "links", r"regulamin|warunki|zastrze|terms|polityk|kontakt|db"),
        ("https://stooq.pl/db/h/", "text", r"captcha|apikey|api key|limit|regulamin|kod z obrazka"),
        ("https://stooq.pl/q/d/l/?s=wig20&i=d", "raw", "300"),
    ],
    "CD-006 CD-007 knf gpw": [
        ("https://www.knf.gov.pl/dla_rynku/espi", "links", r"oam|moam|espi|raport"),
        ("http://moam.knf.gov.pl/moam.nsf", "full", "500"),
        ("https://www.gpw.pl/espi-ebi-reports", "rawgrep", r"rss|\.csv|\.xlsx|export|ajaxindex|geru_id=\d+"),
        ("https://www.gpw.pl/rss", "full", "500"),
        ("https://www.gpw.pl/disclaimer", "full", "900"),
        ("https://www.gpw.pl/ebi", "head", ""),
    ],
    "CD-013 filings.xbrl.org": [
        ("https://filings.xbrl.org/api/filings?filter%5Bcountry%5D=PL&page%5Bsize%5D=1", "raw", "900"),
        ("https://filings.xbrl.org/indexes/filings-2025.json", "head", ""),
        ("https://filings.xbrl.org/about.html", "text", r"restrictions|licen|terms"),
    ],
    "CD-014 companies house": [
        ("https://download.companieshouse.gov.uk/en_output.html", "links", r"\.zip"),
        ("https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2026-11-01.zip", "head", ""),
        ("https://download.companieshouse.gov.uk/en_pscdata.html", "text", r"free of charge|licen"),
        ("https://www.gov.uk/guidance/companies-house-data-products", "text", r"no rules or requirements|Open Government"),
    ],
    "CD-028 openalex": [
        ("https://api.openalex.org/works?filter=authorships.countries:PL&group_by=publication_year", "raw", "700"),
        ("https://api.openalex.org/works?filter=authorships.countries:PL,open_access.is_oa:true&group_by=publication_year", "raw", "300"),
        ("https://openalex.s3.amazonaws.com/LICENSE.txt", "raw", "700"),
        ("https://openalex.s3.amazonaws.com/RELEASE_NOTES.txt", "raw", "300"),
        ("https://help.openalex.org/access/pricing/", "text", r"CC0|per day|API key"),
    ],
    "CD-031 owid": [("https://ourworldindata.org/faqs", "text", r"CC BY|third-party|license terms")]
    + [(f"https://ourworldindata.org/grapher/{s}.csv{Q}", "owid", s) for s in OWID]
    + [(f"https://ourworldindata.org/grapher/{s}.metadata.json", "owidmeta", s) for s in OWID],
    "CD-033 otodom": [
        ("https://www.otodom.pl/regulamin", "text", r"agregowani|pisemnej zgody|automat|robot|kopiowa"),
        ("https://www.otodom.pl/wiadomosci/dane/aktualne-dane/", "rawgrep", r"<iframe[^>]+src=[^>]+|\.csv|\.xlsx|infogram|datawrapper|flourish|tableau|powerbi|lookerstudio"),
        ("https://www.otodom.pl/wiadomosci/dane-otodom-aktualne-ceny-ofertowe-mieszkan-i-domow/", "rawgrep", r"<iframe[^>]+src=[^>]+|\.csv|\.xlsx|infogram|datawrapper|flourish|tableau|powerbi|lookerstudio"),
    ],
    "CD-034 otomoto": [
        ("https://www.otomoto.pl/news/raport-rynkowy-otomoto", "rawgrep", r"<iframe[^>]+src=[^>]+|\.csv|\.xlsx|infogram\.com/[\w-]+|regulamin[^\"']*|datawrapper"),
        ("https://www.otomoto.pl/regulamin", "text", r"agregowani|pisemnej zgody|automat|robot|kopiowa"),
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
        r, raw = fetch(url, "HEAD" if mode == "head" else "GET", 12000000 if mode == "owid" else 900000)
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
