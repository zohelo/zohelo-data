"""Read-only probe for parked TP-012 (NBP calendars), TP-013 (MF calendar) and GL-030 (IOM terms), 27 Nov 2026.

GET only, one request at a time, at least 2 s apart per host, robots.txt honoured; nothing is written anywhere.
"""
import html
import json
import re
import time
import urllib.parse
import urllib.robotparser
from collections import Counter

import requests

from ingestion.common.xlsx_cells import cells

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics research)"
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept-Language": "pl,en"})
LAST: dict[str, float] = {}
ROBOTS: dict[str, object] = {}
WALL = r"(?i)pardon our interruption|incapsula|cf-challenge|captcha|access denied"


def _get(url, timeout=40):
    host = urllib.parse.urlsplit(url).netloc
    wait = 2.0 - (time.time() - LAST.get(host, 0))
    if wait > 0:
        time.sleep(wait)
    try:
        return S.get(url, timeout=timeout, allow_redirects=True)
    finally:
        LAST[host] = time.time()


def robots(url):
    p = urllib.parse.urlsplit(url)
    base = f"{p.scheme}://{p.netloc}"
    if base not in ROBOTS:
        try:
            r = _get(base + "/robots.txt", 25)
            txt = r.text
            if re.search(WALL, txt[:5000]):
                ROBOTS[base] = f"bot-protection page on robots.txt (HTTP {r.status_code}, {len(r.content)} B)"
            elif r.status_code in (401, 403):
                ROBOTS[base] = f"robots.txt HTTP {r.status_code}"
            else:
                rp = urllib.robotparser.RobotFileParser()
                rp.parse(txt.splitlines() if r.status_code == 200 and "<html" not in txt[:300].lower() else [])
                ROBOTS[base] = rp
            print(f"=== ROBOTS {base} HTTP {r.status_code}\n{txt[:1500]}\n=== END", flush=True)
        except Exception as e:
            ROBOTS[base] = f"robots.txt {type(e).__name__}: {str(e)[:80]}"
    rp = ROBOTS[base]
    if isinstance(rp, str):
        return rp
    return None if rp.can_fetch("zohelo-data", url) else "robots disallow"


def get(url, timeout=40):
    no = robots(url)
    if no:
        return None, no
    try:
        r = _get(url, timeout)
    except Exception as e:
        return None, f"{type(e).__name__}: {str(e)[:100]}"
    return r, None


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def links(t, base):
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', t):
        u = urllib.parse.urljoin(base, html.unescape(m.group(1)).strip()).split("#")[0]
        out.append((plain(m.group(2))[:80], u))
    return out


def emit(title, data):
    msg = json.dumps(data, ensure_ascii=False)
    msg = msg.replace("%", "%25").replace("\r", "").replace("\n", " ")[:3900]
    print(f"::notice title={title}::{msg}", flush=True)


def page(url, grep=None, show=6000):
    r, err = get(url)
    if err:
        return {"url": url, "err": err}, None
    t = r.text
    p = plain(t)
    wall = re.findall(WALL, t[:20000])[:1]
    print(f"=== PAGE {url} HTTP {r.status_code} final={r.url} bytes={len(r.content)} lm={r.headers.get('last-modified')}\nTEXT {p[:show]}\n=== END", flush=True)
    out = {"url": url, "st": r.status_code, "bytes": len(r.content), "wall": wall or None,
           "title": plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:80]}
    if grep and not wall:
        out["hits"] = [p[max(0, m.start() - 80): m.start() + 260] for m in re.finditer(grep, p, re.I)][:3]
    return out, t


def arc(page_url, rx):
    """SDDS advance release calendar: find the XLSX on the GUS page, read every cell."""
    info, t = page(page_url, show=3000)
    if t is None or info.get("st") != 200:
        return info
    xl = [u for a, u in links(t, page_url) if u.lower().endswith(".xlsx") and re.search(rx, u, re.I)]
    info["xlsx"] = xl[:2]
    if not xl:
        return info
    r, err = get(xl[0], 60)
    if err:
        info["xlsx_err"] = err
        return info
    info.update({"xlsx_st": r.status_code, "xlsx_bytes": len(r.content), "xlsx_lm": r.headers.get("last-modified"),
                 "xlsx_ct": r.headers.get("content-type", "")[:50]})
    rows: dict[tuple, dict] = {}
    for c in cells(r.content):
        rows.setdefault((c["sheet_index"], c["row_number"]), {})[c["column_number"]] = c.get("value_text") or str(c.get("value") or "")
    print(f"=== XLSX {xl[0]} rows={len(rows)}", flush=True)
    for k in sorted(rows):
        print(k, json.dumps([rows[k][i] for i in sorted(rows[k])], ensure_ascii=False)[:400], flush=True)
    print("=== END", flush=True)
    texts = [" | ".join(rows[k][i] for i in sorted(rows[k])) for k in sorted(rows)]
    info["rows"] = len(rows)
    info["sheets"] = sorted({k[0] for k in rows})
    info["head"] = texts[:6]
    for name, pat in (("nbp", r"NBP|Narodowy Bank|National Bank"), ("mf", r"Ministerstw\w* Finans|Ministry of Finance|\bMF\b"),
                      ("gus", r"GUS|Statistics Poland|Główny Urząd")):
        hit = [x for x in texts if re.search(pat, x)]
        info[f"{name}_rows"] = len(hit)
        info[f"{name}_ex"] = [x[:180] for x in hit[:4]]
    dates = Counter(m for x in texts for m in re.findall(r"\b(20\d\d-\d\d-\d\d|\d\d[.-]\d\d[.-]20\d\d)\b", x))
    info["dates_n"] = len(dates)
    info["dates_ex"] = sorted(dates)[:3] + sorted(dates)[-3:]
    return info


def main():
    res = {}
    res["arc_pl"] = arc("https://stat.gov.pl/banki-i-bazy-danych/sdds/kalendarz-upowszechniania-danych", r"kalendarz")
    res["arc_en"] = arc("https://stat.gov.pl/en/databases/sdds", r"calendar")
    for k in ("arc_pl", "arc_en"):
        emit(f"calendars {k}", res[k])
    nbp = {}
    for u in ("https://nbp.pl/en/monetary-policy/schedule-of-mpc-meetings",
              "https://nbp.pl/polityka-pieniezna/decyzje-rpp/",
              "https://nbp.pl/en/statistic-and-financial-reporting/calendar"):
        nbp[u.split("nbp.pl")[1]] = page(u, r"2026|January|stycz|Termin")[0]
    nbp["robots"] = str(ROBOTS.get("https://nbp.pl"))[:120]
    emit("calendars TP-012 nbp.pl", nbp)
    mf = {}
    for u in ("https://www.gov.pl/web/finanse/zapowiedzi",
              "https://www.gov.pl/web/finanse/zadluzenie",
              "https://www.gov.pl/web/finanse/dane-fiskalne-na-potrzeby-nadzoru-budzetowego-ue"):
        info, t = page(u, r"harmonogram|kalendarz|termin\w* publikacji")
        if t:
            info["cal_links"] = [f"{a} | {x}" for a, x in links(t, u) if re.search(r"harmonogram|kalendarz|calendar", a + x, re.I)][:6]
        mf[u.split("finanse/")[1]] = info
    mf["robots"] = str(ROBOTS.get("https://www.gov.pl"))[:120]
    emit("calendars TP-013 gov.pl finanse", mf)
    iom = {}
    for u in ("https://www.migrationdataportal.org/terms-use", "https://migrationdataportal.org/terms-use"):
        iom[u] = page(u, r"permission|non-commercial|licen|reproduc|Creative Commons")[0]
    emit("calendars GL-030 IOM terms", iom)


if __name__ == "__main__":
    main()
