"""Dev probe (read-only): reachability, format and terms of the topics sources (docs/topics.md, O32).

python -m dev.probe_topics <group>   group: news | calendars | signals
No writes anywhere; results are annotations (one per outlet / calendar / signal).
"""

from __future__ import annotations

from hashlib import sha256
from html.parser import HTMLParser
import json
import re
import sys
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import xml.etree.ElementTree as ET

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
LEVELS = ["notice"] * 10 + ["warning"] * 10 + ["error"] * 10
COUNT = {"n": 0}


def emit(title: str, data) -> None:
    annotate(LEVELS[min(COUNT["n"], 29)], title, data)
    COUNT["n"] += 1


class Links(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.alt, self.a, self.text = [], [], []
        self._in_a = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "link" and (a.get("type") or "").endswith(("rss+xml", "atom+xml")):
            self.alt.append(a.get("href"))
        if tag == "a" and a.get("href"):
            self.a.append(a["href"])


def get(url: str, **kw):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=30, allow_redirects=True, **kw)
        return r, None
    except requests.RequestException as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:150]}"


ROBOTS: dict[str, RobotFileParser | str] = {}


def robots(url: str) -> dict:
    host = urlparse(url).scheme + "://" + urlparse(url).netloc
    if host not in ROBOTS:
        r, err = get(host + "/robots.txt")
        if r is None or r.status_code != 200:
            ROBOTS[host] = f"robots.txt {err or r.status_code}"
        else:
            rp = RobotFileParser()
            rp.parse(r.text.splitlines())
            ROBOTS[host] = rp
            ROBOTS[host + "#lines"] = [
                ln.strip() for ln in r.text.splitlines() if re.search(r"rss|feed|xml|crawl-delay|ai|gpt|claude", ln, re.I)
            ][:12]
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return {"robots": rp}
    return {"allowed_ua": rp.can_fetch(UA, url), "allowed_star": rp.can_fetch("*", url), "lines": ROBOTS[host + "#lines"]}


def feed_summary(url: str) -> dict:
    r, err = get(url)
    if r is None:
        return {"url": url, "error": err}
    out = {"url": url, "status": r.status_code, "final": r.url if r.url != url else None,
           "ctype": r.headers.get("Content-Type"), "bytes": len(r.content)}
    if r.status_code != 200:
        return out
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as exc:
        out["parse"] = str(exc)[:80]
        out["head"] = r.text[:160]
        return out
    tag = root.tag.split("}")[-1]
    out["root"] = tag
    if tag == "rss":
        ch = root.find("channel")
        items = ch.findall("item") if ch is not None else []
        out["n"] = len(items)
        out["chan"] = {k: (ch.findtext(k) or "")[:120] for k in ("title", "language", "copyright", "ttl") if ch is not None and ch.find(k) is not None}
        if items:
            it = items[0]
            out["fields"] = sorted({c.tag.split("}")[-1] for c in it})
            out["sample"] = [((i.findtext("title") or "")[:90], i.findtext("pubDate")) for i in items[:3]]
            out["guid_eq_link"] = it.findtext("guid") == it.findtext("link")
            desc = it.findtext("description") or ""
            out["desc_len"] = len(desc)
            dates = [i.findtext("pubDate") for i in items if i.findtext("pubDate")]
            out["oldest_newest"] = [dates[-1], dates[0]] if dates else None
    elif tag == "feed":
        ns = {"a": "http://www.w3.org/2005/Atom"}
        entries = root.findall("a:entry", ns)
        out["n"] = len(entries)
        out["rights"] = (root.findtext("a:rights", default="", namespaces=ns))[:120]
        out["sample"] = [((e.findtext("a:title", namespaces=ns) or "")[:90], e.findtext("a:updated", namespaces=ns)) for e in entries[:3]]
    out.update(robots(url))
    return out


NEWS = {
    "bankier": {"pages": ["https://www.bankier.pl/rss", "https://www.bankier.pl/"],
                "feeds": ["https://www.bankier.pl/rss/wiadomosci.xml", "https://www.bankier.pl/rss/gospodarka.xml"]},
    "money": {"pages": ["https://www.money.pl/rss/", "https://www.money.pl/"],
              "feeds": ["https://www.money.pl/rss/rss.xml", "https://www.money.pl/rss/main.xml"]},
    "business_insider": {"pages": ["https://businessinsider.com.pl/rss", "https://businessinsider.com.pl/"],
                         "feeds": ["https://businessinsider.com.pl/.feed", "https://businessinsider.com.pl/gospodarka.feed"]},
    "rzeczpospolita": {"pages": ["https://www.rp.pl/rss", "https://www.rp.pl/ekonomia"],
                       "feeds": ["https://www.rp.pl/rss_main", "https://www.rp.pl/rss/1019-ekonomia"]},
    "puls_biznesu": {"pages": ["https://www.pb.pl/rss", "https://www.pb.pl/"],
                     "feeds": ["https://www.pb.pl/rss/najnowsze.xml", "https://www.pb.pl/rss/gospodarka.xml"]},
    "forsal": {"pages": ["https://forsal.pl/rss", "https://forsal.pl/"],
               "feeds": ["https://forsal.pl/.feed", "https://forsal.pl/gospodarka.feed"]},
    "pap": {"pages": ["https://www.pap.pl/rss", "https://www.pap.pl/"],
            "feeds": ["https://www.pap.pl/rss.xml", "https://www.pap.pl/biznes/rss.xml"]},
    "notes_from_poland": {"pages": ["https://notesfrompoland.com/"],
                          "feeds": ["https://notesfrompoland.com/feed/"]},
}
TERMS = re.compile(r"[^.<>]{0,160}(regulamin|warunk|zasady korzystania|terms|copyright|licenc|prawa autorskie)[^.<>]{0,200}", re.I)


def news() -> None:
    for outlet, spec in NEWS.items():
        found, terms, pages = [], [], []
        for page in spec["pages"]:
            r, err = get(page)
            if r is None:
                pages.append({"url": page, "error": err})
                continue
            pages.append({"url": page, "status": r.status_code, "final": r.url})
            if r.status_code == 200 and "html" in (r.headers.get("Content-Type") or ""):
                p = Links()
                try:
                    p.feed(r.text)
                except Exception as exc:  # noqa: BLE001 - probe
                    pages[-1]["html_error"] = str(exc)[:80]
                found += [urljoin(r.url, h) for h in p.alt if h]
                found += [urljoin(r.url, h) for h in p.a if re.search(r"rss|\.feed|/feed", h, re.I)]
                if "rss" in page:
                    text = re.sub(r"<[^>]+>", " ", r.text)
                    terms += [re.sub(r"\s+", " ", m.group(0)).strip()[:300] for m in TERMS.finditer(text)][:3]
        seen = []
        for u in spec["feeds"] + found:
            if u not in seen:
                seen.append(u)
        feeds = [feed_summary(u) for u in seen[:7]]
        for f in feeds:
            if f.get("status") == 200 and f.get("n"):
                f.pop("lines", None) if feeds.index(f) > 0 else None
        emit(f"news {outlet}", {"pages": pages, "discovered": seen[:20], "terms": terms, "feeds": feeds})
        time.sleep(1)


CALENDARS = {
    "eurostat_ics": ["https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_EN.ics"],
    "gus": ["https://stat.gov.pl/kalendarium/11-2026.html", "https://stat.gov.pl/kalendarium/12-11-2026.html",
            "https://stat.gov.pl/en/release-calendar/"],
    "nbp": ["https://nbp.pl/polityka-pieniezna/decyzje-rpp/terminarz-posiedzen-rpp/",
            "https://nbp.pl/polityka-pieniezna/decyzje-rpp/",
            "https://nbp.pl/statystyka-i-sprawozdawczosc/kalendarz-publikacji/"],
    "mf": ["https://www.gov.pl/web/finanse/harmonogram-publikacji", "https://www.gov.pl/web/finanse/kalendarz-publikacji",
           "https://www.gov.pl/web/finanse/dlug-publiczny"],
    "ecb": ["https://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html",
            "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html"],
}


def calendars() -> None:
    for name, urls in CALENDARS.items():
        rows = []
        for url in urls:
            r, err = get(url)
            if r is None:
                rows.append({"url": url, "error": err})
                continue
            row = {"url": url, "status": r.status_code, "final": r.url if r.url != url else None,
                   "ctype": r.headers.get("Content-Type"), "bytes": len(r.content), "lm": r.headers.get("Last-Modified")}
            text = r.text
            if text.startswith("BEGIN:VCALENDAR"):
                events = text.split("BEGIN:VEVENT")[1:]
                row["events"] = len(events)
                row["sample"] = [e[:400] for e in events[:2]]
                row["cats"] = sorted({m for e in events for m in re.findall(r"CATEGORIES:(.*)", e)})[:30]
            else:
                p = Links()
                p.feed(text)
                row["links"] = [h for h in p.a if re.search(r"ics|ical|xml|csv|xls|json|kalend|terminarz|harmonogram|calendar", h, re.I)][:15]
                body = re.sub(r"\s+", " ", re.sub(r"<script.*?</script>|<style.*?</style>", " ", text, flags=re.S))
                i = max(0, body.find("<main"))
                row["text"] = re.sub(r"<[^>]+>", " ", body[i:])[:900]
            row.update(robots(url))
            rows.append(row)
            time.sleep(1)
        emit(f"calendar {name}", rows)


def signals() -> None:
    gd = "https://api.gdeltproject.org/api/v2/doc/doc"
    tests = [
        ("gdelt volraw lang", f"{gd}?query=inflacja%20sourcelang:polish&mode=timelinevolraw&format=json&timespan=3m"),
        ("gdelt vol country", f"{gd}?query=inflation%20sourcecountry:PL&mode=timelinevol&format=json&timespan=1w"),
        ("gdelt tone lang", f"{gd}?query=(inflacja%20OR%20drożyzna)%20sourcelang:polish&mode=timelinetone&format=json&timespan=1w"),
    ]
    for title, url in tests:
        r, err = get(url)
        row = {"url": url}
        if r is None:
            row["error"] = err
        else:
            row.update({"status": r.status_code, "ctype": r.headers.get("Content-Type"), "bytes": len(r.content),
                        "sha": sha256(r.content).hexdigest()[:12], "head": r.text[:700]})
            try:
                data = r.json()
                tl = data.get("timeline") or []
                row["series"] = [s.get("series") for s in tl]
                row["points"] = [len(s.get("data") or []) for s in tl]
                row["first_last"] = [(s.get("data") or [{}])[0] for s in tl][:2] + [(s.get("data") or [{}])[-1] for s in tl][:2]
                row.pop("head")
            except ValueError:
                pass
        row.update(robots(url))
        emit(title, row)
        time.sleep(6)
    wm = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    for title, url in [
        ("wiki pl", f"{wm}/pl.wikipedia.org/all-access/user/Inflacja/daily/20250801/20261109"),
        ("wiki en", f"{wm}/en.wikipedia.org/all-access/user/Economy_of_Poland/daily/20261001/20261109"),
        ("wiki missing", f"{wm}/pl.wikipedia.org/all-access/user/Nie_ma_takiego_artyku%C5%82u_xyz/daily/20261001/20261109"),
    ]:
        r, err = get(url)
        row = {"url": url}
        if r is None:
            row["error"] = err
        else:
            row.update({"status": r.status_code, "bytes": len(r.content)})
            try:
                items = r.json().get("items") or []
                row.update({"n": len(items), "first": items[:1], "last": items[-1:]})
            except ValueError:
                row["head"] = r.text[:300]
            if r.status_code != 200:
                row["head"] = r.text[:300]
        emit(title, row)


if __name__ == "__main__":
    {"news": news, "calendars": calendars, "signals": signals}[sys.argv[1]]()
    print(json.dumps({"annotations": COUNT["n"]}))
