"""Quality Engineer check (read-only): live zohelo.com pages and published values vs providers.

Reads 06_published/v1/ from R2 (GET only) and public pages/APIs; writes nothing.
"""

from __future__ import annotations

import json
import os
import re
import urllib.request
from html.parser import HTMLParser

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-quality-check/1.0 (+https://zohelo.com)"}
PAGES = ["/", "/en", "/changes", "/en/changes", "/calendar.ics", "/rss.xml", "/en/rss.xml", "/portfolio", "/en/portfolio",
         "/robots.txt", "/sitemap.xml", "/indicators", "/en/indicators", "/indicators/unemployment_rate",
         "/en/indicators/unemployment_rate", "/indicators/fx_rate", "/regions", "/en/regions", "/sources", "/en/sources"]
TEXT = ("/", "/changes", "/indicators/unemployment_rate", "/regions", "/sources")


def get(url: str, timeout: int = 30) -> tuple[int, bytes, str]:
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(), r.geturl()
    except urllib.error.HTTPError as e:
        return e.code, e.read() or b"", url
    except Exception as e:  # noqa: BLE001
        return -1, str(e).encode(), url


class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.lang = None
        self.title = ""
        self._t = False
        self.links: set[str] = set()
        self.assets: set[str] = set()
        self.img_no_alt = 0
        self.imgs = 0
        self.h1 = 0
        self.hreflang: list[str] = []
        self.text: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag in ("script", "style"):
            self._skip += 1
        if tag == "html":
            self.lang = a.get("lang")
        elif tag == "title":
            self._t = True
        elif tag == "a" and a.get("href"):
            self.links.add(a["href"])
        elif tag == "img":
            self.imgs += 1
            if a.get("alt") is None:
                self.img_no_alt += 1
            if a.get("src"):
                self.assets.add(a["src"])
        elif tag == "h1":
            self.h1 += 1
        elif tag == "link":
            if a.get("rel") == "alternate" and a.get("hreflang"):
                self.hreflang.append(f"{a['hreflang']}={a.get('href')}")
            if a.get("rel") in ("stylesheet", "modulepreload", "icon") and a.get("href"):
                self.assets.add(a["href"])
        elif tag == "script" and a.get("src"):
            self.assets.add(a["src"])

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = max(0, self._skip - 1)
        if tag == "title":
            self._t = False

    def handle_data(self, data):
        if not self._skip and data.strip():
            self.text.append(data.strip())
        if self._t:
            self.title += data


def site() -> None:
    base = "https://zohelo.com"
    pages = {}
    checked: dict[str, int] = {}
    broken: list[str] = []
    for path in PAGES:
        status, body, final = get(base + path)
        info = {"status": status, "bytes": len(body), "final": final}
        if path.endswith((".txt", ".xml", ".ics")):
            info["head"] = body[:200].decode("utf-8", "replace")
            if path == "/sitemap.xml":
                info["locs"] = re.findall(r"<loc>([^<]+)</loc>", body.decode("utf-8", "replace"))[:20]
        else:
            p = Page()
            p.feed(body.decode("utf-8", "replace"))
            info.update(lang=p.lang, title=p.title.strip(), h1=p.h1, imgs=p.imgs, img_no_alt=p.img_no_alt,
                        hreflang=p.hreflang, links=sorted(p.links)[:12])
            if path in TEXT:
                annotate("notice", f"text {path}", " | ".join(p.text)[:3800])
            for href in sorted(p.links | p.assets):
                if href.startswith(("mailto:", "#", "tel:")):
                    continue
                url = href if href.startswith("http") else base + (href if href.startswith("/") else "/" + href)
                if url not in checked:
                    checked[url] = get(url)[0]
                    if checked[url] != 200:
                        broken.append(f"{path} -> {href}: {checked[url]}")
        pages[path] = info
    status, _, _ = get(base + "/no-such-page-qe")
    items = list(pages.items())
    for i in range(0, len(items), 3):
        annotate("notice", f"pages {i // 3 + 1}", dict(items[i:i + 3]))
    annotate("warning" if broken else "notice", "site links",
             {"checked": len(checked), "broken": broken, "404_page_status": status,
              "www": get("https://www.zohelo.com/")[0]})


def bi() -> None:
    base = "https://bi.zohelo.com"
    out = {}
    broken = []
    for path in ("/", "/explore", "/en/explore", "/en", "/dashboards", "/catalog.json", "/no-such-page-qe"):
        status, body, final = get(base + path)
        info = {"status": status, "bytes": len(body), "final": final}
        if b"<html" in body[:2000].lower():
            p = Page()
            p.feed(body.decode("utf-8", "replace"))
            info.update(lang=p.lang, title=p.title.strip(), h1=p.h1, imgs=p.imgs, img_no_alt=p.img_no_alt,
                        links=sorted(p.links)[:15], text=" | ".join(p.text)[:400])
            for href in sorted(p.assets):
                url = href if href.startswith("http") else base + (href if href.startswith("/") else "/" + href)
                st, b, _ = get(url)
                if st != 200:
                    broken.append(f"{path} -> {href}: {st}")
                elif url.endswith(".js"):
                    info.setdefault("js_kb", 0)
                    info["js_kb"] += len(b) // 1024
        out[path] = info
    items = list(out.items())
    for i in range(0, len(items), 3):
        annotate("notice", f"bi {i // 3 + 1}", dict(items[i:i + 3]))
    annotate("warning" if broken else "notice", "bi assets", {"broken": broken})


def browser() -> None:
    import shutil
    import subprocess

    chrome = shutil.which("google-chrome") or shutil.which("chromium") or shutil.which("chromium-browser")
    if not chrome:
        annotate("error", "browser", "no chrome on runner")
        return
    paths = ["/explore?lang=pl", "/explore?lang=en", "/d/labour-market?lang=pl", "/d/labour-market?lang=en",
             "/d/population?lang=pl", "/d/prices-money?lang=pl", "/d/regional-comparison?lang=pl",
             "/d/poland-overview?lang=en"]
    for path in paths:
        try:
            dom = subprocess.run([chrome, "--headless=new", "--no-sandbox", "--disable-gpu", "--virtual-time-budget=20000",
                                  "--dump-dom", "https://bi.zohelo.com" + path], capture_output=True, text=True,
                                 timeout=90).stdout
        except Exception as e:  # noqa: BLE001
            annotate("error", f"browser {path}", repr(e)[:500])
            continue
        p = Page()
        p.feed(dom)
        text = " | ".join(p.text)
        info = {"bytes": len(dom), "lang": p.lang, "title": p.title.strip(), "h1": p.h1,
                "svg": dom.count("<svg"), "canvas": dom.count("<canvas"), "img_no_alt": p.img_no_alt,
                "bad": [w for w in ("NaN", "undefined", "null", "Error", "Błąd", "[object") if w in text],
                "text": text[:2600]}
        annotate("notice", f"browser {path}", info)


def jget(url: str):
    status, body, _ = get(url, 60)
    if status != 200:
        return {"_status": status, "_body": body[:200].decode("utf-8", "replace")}
    return json.loads(body)


def eurostat(dataset: str, period: str, **filters) -> float | str | None:
    q = "&".join(f"{k}={v}" for k, v in filters.items())
    d = jget(f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}?geo=PL&time={period}&{q}")
    if "_status" in d:
        return f"http {d['_status']}"
    vals = d.get("value", {})
    return list(vals.values())[0] if len(vals) == 1 else f"{len(vals)} values"


def worldbank(indicator: str, year: str):
    d = jget(f"https://api.worldbank.org/v2/country/POL/indicator/{indicator}?format=json&date={year}")
    try:
        return d[1][0]["value"]
    except Exception:  # noqa: BLE001
        return f"n/a {str(d)[:120]}"


def nbp(code: str, day: str):
    d = jget(f"https://api.nbp.pl/api/exchangerates/rates/a/{code.lower()}/{day}/?format=json")
    try:
        return d["rates"][0]["mid"]
    except Exception:  # noqa: BLE001
        return f"n/a {str(d)[:120]}"


def provider(kpi: str, source: str, period: str):
    if source == "eurostat":
        if kpi == "population":
            return eurostat("demo_pjan", period, age="TOTAL", sex="T", unit="NR")
        if kpi == "gdp_current_prices":
            return eurostat("nama_10_gdp", period, unit="CP_MEUR", na_item="B1GQ")
        if kpi == "inflation":
            return eurostat("prc_hicp_manr", period, unit="RCH_A", coicop="CP00")
        if kpi == "unemployment_rate":
            if "-" in period:
                return eurostat("une_rt_m", period, unit="PC_ACT", s_adj="SA", age="Y15-74", sex="T")
            return eurostat("une_rt_a", period, unit="PC_ACT", age="Y15-74", sex="T")
    if source == "world_bank_wdi":
        ind = {"population": "SP.POP.TOTL", "gdp_current_prices": "NY.GDP.MKTP.CD",
               "inflation": "FP.CPI.TOTL.ZG", "unemployment_rate": "SL.UEM.TOTL.ZS"}[kpi]
        return worldbank(ind, period[:4])
    return None


def numbers() -> None:
    from ingestion.common.r2 import catalog
    from warehouse import publish as pub

    cat = catalog()
    measures = pub.seed("kpi_measure")
    checked_at = pub.checked(pub.gold(cat, "gold", "coverage"))
    rows = []
    for kpi in ("population", "gdp_current_prices", "inflation", "unemployment_rate"):
        unit = next(k["unit_key"] for k in pub.seed("kpi") if k["kpi_key"] == kpi)
        metrics = pub.gold(cat, "gold", f"metrics_{kpi}")
        latest = pub.kpi_latest(metrics, measures, checked_at, kpi, unit, "now")
        country = [v for v in latest["values"] if v["geography_key"] == "PL"]
        voiv = [v for v in latest["values"] if v["geography_level"] == "voivodeship"]
        for v in country:
            rows.append({"kpi": kpi, "unit": unit, "src": v["source_key"], "period": v["period"],
                         "zohelo": v["value"], "provider": provider(kpi, v["source_key"], v["period"]),
                         "prev": [v.get("previous_period"), v.get("previous_value")]})
        by_src: dict[str, list] = {}
        for v in voiv:
            by_src.setdefault(v["source_key"], []).append(v)
        rows.append({"kpi": kpi, "metrics_columns": metrics.column_names[:20],
                     "voivodeship": {s: {"n": len(x), "periods": sorted({y["period"] for y in x})[-3:],
                                         "sum": round(sum(y["value"] for y in x), 2)} for s, x in by_src.items()}})
    for i in range(0, len(rows), 5):
        annotate("notice", f"numbers {i // 5 + 1}", rows[i:i + 5])

    fx = pub.gold(cat, "gold", "metrics_fx_rate")
    cols = fx.column_names
    out = {"columns": cols, "rows": fx.num_rows}
    cur = next((c for c in cols if "currency" in c), None)
    tcol = next((c for c in cols if c in ("date_key", "period_start", "period", "date")), None)
    if cur and tcol:
        import pyarrow.compute as pc
        for code in ("EUR", "USD", "CHF"):
            t = fx.filter(pc.equal(pc.utf8_upper(pc.cast(fx[cur], "string")), code))
            if t.num_rows:
                rows_ = t.to_pylist()
                last = max(rows_, key=lambda r: str(r[tcol]))
                day = str(last[tcol])[:10]
                out[code] = {"day": day, "row": {k: str(v) for k, v in last.items()}, "nbp": nbp(code, day)}
    annotate("notice", "numbers fx", out)


def fresh() -> None:
    import pyarrow.compute as pc

    from ingestion.common.r2 import catalog
    from warehouse import publish as pub

    cat = catalog()
    out = {}
    for ds, q in (("prc_hicp_manr", "unit=RCH_A&coicop=CP00"), ("une_rt_m", "unit=PC_ACT&s_adj=SA&age=Y15-74&sex=T"),
                  ("une_rt_a", "unit=PC_ACT&age=Y15-74&sex=T"), ("demo_pjan", "age=TOTAL&sex=T&unit=NR"),
                  ("nama_10_gdp", "unit=CP_MEUR&na_item=B1GQ")):
        d = jget(f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{ds}?geo=PL&{q}&lastTimePeriod=2")
        if "_status" in d:
            out[ds] = d
            continue
        times = list(d["dimension"]["time"]["category"]["index"].keys())
        out[ds] = {"provider_last": times, "values": d.get("value"), "updated": d.get("updated")}
    for kpi in ("inflation", "unemployment_rate", "population", "gdp_current_prices"):
        m = pub.gold(cat, "gold", f"metrics_{kpi}")
        m = m.filter(pc.equal(m["geography_key"], "PL"))
        per = {}
        for r in m.select(["source_system", "measure_key", "frequency", "date_key", "unit"]).to_pylist():
            k = f"{r['source_system']}|{r['measure_key']}|{r['frequency']}|{r['unit']}"
            per[k] = max(per.get(k, ""), str(r["date_key"]))
        out[f"gold_{kpi}"] = per
    annotate("notice", "freshness", out)
    fx = pub.gold(cat, "gold", "metrics_fx_rate")
    last = pc.max(fx["date_key"]).as_py()
    t = fx.filter(pc.equal(fx["date_key"], last)).select(["measure_key", "breakdown_json", "date_key", "value", "unit"]).to_pylist()
    pick = [r for r in t if any(c in str(r["breakdown_json"]) for c in ('"EUR"', '"USD"', '"CHF"'))][:6]
    day = str(last)[:10]
    annotate("notice", "fx", {"day": day, "rows": [{k: str(v) for k, v in r.items()} for r in pick],
                              "nbp": {c: nbp(c, day) for c in ("EUR", "USD", "CHF")},
                              "nbp_today": jget("https://api.nbp.pl/api/exchangerates/tables/a/?format=json")[0].get("effectiveDate")
                              if isinstance(jget("https://api.nbp.pl/api/exchangerates/tables/a/?format=json"), list) else None})


def portal() -> None:
    base = "https://data.zohelo.com"
    out = {}
    for path in ("/", "/index.html", "/catalog.json", "/no-such-page-qe"):
        status, body, final = get(base + path)
        info = {"status": status, "bytes": len(body), "final": final}
        if b"<html" in body[:3000].lower():
            p = Page()
            p.feed(body.decode("utf-8", "replace"))
            info.update(lang=p.lang, title=p.title.strip(), h1=p.h1, links=sorted(p.links)[:25],
                        text=" | ".join(p.text)[:1500])
            bad = []
            for href in sorted(p.assets | {x for x in p.links if not x.startswith(("http", "mailto:", "#"))}):
                url = href if href.startswith("http") else base + (href if href.startswith("/") else "/" + href)
                st = get(url)[0]
                if st != 200:
                    bad.append(f"{href}: {st}")
            info["broken"] = bad[:20]
        else:
            info["head"] = body[:300].decode("utf-8", "replace")
        annotate("notice", f"portal {path}", info)


def main() -> None:
    import sys
    fns = {"portal": portal, "browser": browser, "bi": bi, "site": site, "numbers": numbers, "fresh": fresh}
    for fn in [fns[a] for a in (sys.argv[1:] or ["site"])]:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            annotate("error", f"qe {fn.__name__}", repr(e)[:1500])


if __name__ == "__main__":
    main()
