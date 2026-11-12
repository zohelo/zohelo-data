"""Read-only reachability probe for the source review batch D, F, H (12 Nov 2026, #2860).

GET/HEAD only, at most 10 annotations (two register ids each). robots.txt is honoured (for the generic
agent and for Claude agents); a refused or failing host is reported, never retried another way.
"""

import concurrent.futures as cf
import html
import json
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser

from ingestion.common.report import annotate

UA = "zohelo-data-research/1.0 (+https://zohelo.com)"
CTX = ssl.create_default_context()
ROBOTS: dict[str, object] = {}


def raw_get(url, n=400000, method="GET", accept="*/*", timeout=40):
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
            "etag": (h.get("etag") or "")[:40] or None,
            "rl": {k: v for k, v in h.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"} or None,
            "final": r.geturl() if r.geturl() != url else None,
            "body": body,
        }


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
            ROBOTS[host] = f"robots.txt {type(e).__name__}"
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return rp
    for agent in ("zohelo-data-research", "ClaudeBot", "Claude-User"):
        if not rp.can_fetch(agent, url):
            return f"robots disallow ({agent})"
    return None


def get(url, n=400000, method="GET", accept="*/*"):
    """Fetch; returns a dict with 'err' instead of raising."""
    try:
        no = robots_ok(url)
        if no:
            return {"err": no}
        return raw_get(url, n, method, accept)
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"err": f"{type(e).__name__}: {str(e)[:80]}"}


def meta(r):
    if "err" in r:
        return r["err"]
    out = {k: r[k] for k in ("st", "ct", "len", "lm", "etag", "rl", "final") if r.get(k)}
    if r.get("body"):
        out["read"] = len(r["body"])
    return out


def head(url):
    r = get(url, method="HEAD")
    if "err" in r and r["err"].startswith("HTTP 4"):
        r = get(url, n=1)
    return meta(r)


def text(r):
    return r["body"].decode("utf-8", "replace") if "body" in r else ""


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def links(t, pattern, limit=12, base=""):
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', t):
        h, a = html.unescape(m.group(1)), plain(m.group(2))[:50]
        if re.search(pattern, h + " " + a, re.I):
            item = f"{a}|{urllib.parse.urljoin(base, h)}" if a else urllib.parse.urljoin(base, h)
            if item not in out:
                out.append(item)
            if len(out) >= limit:
                break
    return out


def around(t, pattern, before=60, after=300, limit=2):
    p = plain(t) if "<" in t[:2000] else re.sub(r"\s+", " ", t)
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
    return {"plain_chars": len(plain(t)), "scripts": len(re.findall(r"(?i)<script", t)), "title": plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:60]}


def file_urls(page, pattern=r"\.(xlsx?|zip|csv)(\?|$)|static\.nbp\.pl/dane", limit=6):
    r = get(page)
    if "err" in r:
        return {"page": r["err"]}
    ls = links(text(r), pattern, limit, page)
    out = {"page": r["st"], "n": len(ls), "files": ls}
    out["head"] = [head(u.split("|")[-1]) for u in ls[:2]]
    return out

FILES = r"\.(xlsx?|zip|csv|ods|json|xml)(\?|$)"


def page(url, pattern=FILES, limit=5, words=None, heads=1, n=600000):
    """One page: status, how it renders, matching links, optional text around words, HEAD of first files."""
    r = get(url, n)
    if "err" in r:
        return r["err"]
    t = text(r)
    ls = links(t, pattern, 400, url)
    out = {"st": r["st"], "chars": len(plain(t)), "scripts": len(re.findall(r"(?i)<script", t)), "n": len(ls), "links": [x[-150:] for x in ls[:limit]]}
    if r.get("final"):
        out["final"] = r["final"]
    if len(ls) > limit:
        out["last"] = ls[-1][-150:]
    if words:
        out["text"] = around(t, words, 60, 260, 1)
    if heads and ls:
        out["head"] = [head(u.split("|")[-1]) for u in ls[:heads]]
    return out


def robots(host):
    r = robots_ok(host + "/")
    rp = ROBOTS.get(host)
    return r or ("ok" if not isinstance(rp, str) else rp)


def jget(url, n=200000):
    r = get(url, n, accept="application/json")
    if "err" in r:
        return r["err"]
    out = meta(r)
    try:
        d = json.loads(text(r))
        out["keys"] = list(d)[:8] if isinstance(d, dict) else f"list[{len(d)}]"
        out["json"] = d
    except Exception:  # noqa: BLE001
        out["body"] = text(r)[:200]
    return out


def plg_020():
    return {
        "robots": robots("https://decyzje.uokik.gov.pl"),
        "db": page("https://decyzje.uokik.gov.pl/bp/dec_prez.nsf", r"OpenView|OpenDocument|\.pdf|nsf/", 4, heads=0),
    }


def plg_021():
    u = "https://uokik.gov.pl/bip/koncentracje"
    a, b = get(u), get(u + "?page=2")
    pa = links(text(a), r"/bip/[a-z0-9-]{25,}", 3, u) if "body" in a else a["err"]
    pb = links(text(b), r"/bip/[a-z0-9-]{25,}", 3, u) if "body" in b else b["err"]
    t = get("https://uokik.gov.pl/bip/wnioskowanie-o-dostep-do-informacji-sektora-publicznego-w-celu-jej-ponownego-wykorzystywania")
    return {
        "robots": robots("https://uokik.gov.pl"),
        "p1": pa,
        "p2_differs": pa != pb,
        "pager": around(text(a), r"pagination|stron[ay] \d+ z|\bz \d{2,4}\b", 40, 120, 1) if "body" in a else None,
        "files": len(links(text(a), FILES, 50, u)) if "body" in a else None,
        "terms": around(text(t), r"wyraża zgodę", 10, 330, 1) if "body" in t else t["err"],
    }


def plg_025():
    return {
        "robots": robots("https://www.senat.gov.pl"),
        "list": page("https://www.senat.gov.pl/prace/proces-legislacyjny-w-senacie/", r"rss|xml|json|api|\.xlsx?|\.csv", 4, heads=0),
        "legal": page("https://www.senat.gov.pl/o-senacie/informacje-prawne/", r"$^", 0, r"ponown\w+ wykorzyst|licenc|prawa autorskie", 0),
        "sejm_api_processes": meta(get("https://api.sejm.gov.pl/sejm/term10/processes?limit=1", 3000)),
    }


def plg_028():
    u = "https://www.nik.gov.pl/kontrole/wyniki-kontroli-nik/"
    r = get(u)
    t = text(r)
    forms = re.findall(r'(?is)<form\b[^>]*action=["\']([^"\']*)["\'][^>]*>', t)[:3]
    names = sorted(set(re.findall(r'(?is)<(?:input|select)\b[^>]*name=["\']([^"\']+)["\']', t)))[:14]
    c = get("https://www.nik.gov.pl/zasady/prawa-autorskie.html")
    return {
        "robots": robots("https://www.nik.gov.pl"),
        "page": r.get("st") or r["err"],
        "forms": forms,
        "inputs": names,
        "feeds": links(t, r"rss|\.xml|api|json|\.csv|\.xlsx?", 4, u),
        "results": links(t, r"kontrole/[A-Z]/\d\d/\d+|wyniki-kontroli-nik/[^\"]+,\d+", 3, u),
        "terms": around(text(c), r"nie wymaga uzyskania zgody", 80, 330, 1) if "body" in c else c["err"],
    }


def plg_029():
    return {
        "robots": {h: robots("https://" + h) for h in ("danewyborcze.kbw.gov.pl", "wybory.gov.pl", "sejmsenat2023.pkw.gov.pl", "pkw.gov.pl")},
        "kbw": page("https://danewyborcze.kbw.gov.pl/", FILES + r"|arkusz|dane", 4, r"licenc|ponown\w+ wykorzyst", 1),
        "sejm2023": page("https://sejmsenat2023.pkw.gov.pl/sejmsenat2023/pl/dane_w_arkuszach", FILES, 4, r"licenc|ponown\w+ wykorzyst"),
        "prez2025": page("https://wybory.gov.pl/prezydent2025/pl/dane_w_arkuszach", FILES, 3, heads=1),
    }


def plg_031():
    b = "https://isws.ms.gov.pl/pl/baza-statystyczna/"
    return {
        "robots": robots("https://isws.ms.gov.pl"),
        "multi_year": page(b + "opracowania-wieloletnie/", r"download,\d+,\d+", 3, r"licenc|ponown\w+ wykorzyst|Copyright", 2),
        "one_year": page(b + "opracowania-jednoroczne/", r"download,\d+,\d+|opracowania-jednoroczne/[^\"]+", 3, heads=0),
        "sections": links(text(get(b)), r"baza-statystyczna/[a-z0-9-]+/?$", 8, b),
    }


def plg_032():
    return {
        "robots": robots("https://sw.gov.pl"),
        "robots_www": robots("https://www.sw.gov.pl"),
        "stat": page("https://sw.gov.pl/strona/Statystyka", r"\.(pdf|xlsx?|csv|zip)|statystyka-", 6, r"licenc|ponown\w+ wykorzyst"),
        "annual": page("https://sw.gov.pl/strona/statystyka-roczna", r"\.(pdf|xlsx?|csv|zip)", 4),
        "monthly": page("https://sw.gov.pl/strona/statystyka-miesieczna", r"\.(pdf|xlsx?|csv|zip)", 4),
    }


def plg_033():
    b = "https://statystyka.policja.pl"
    sm = get(b + "/st/sitemap")
    t = text(sm)
    home = get(b + "/")
    rules = links(text(home), r"zasad", 2, b)
    rule_url = rules[0].split("|")[-1] if rules else None
    rr = get(rule_url) if rule_url else {"err": "no link"}
    first = links(t, r"przestepstwa-ogolem|wybrane-statystyki|wypadki", 4, b)
    sub = first[0].split("|")[-1] if first else b + "/st/przestepstwa-ogolem"
    return {
        "robots": robots(b),
        "sitemap": sm.get("st") or sm["err"],
        "topics": [x[-90:] for x in first],
        "topic_page": page(sub, r"\.(pdf|xlsx?|csv|zip)|/download/", 4),
        "rules_url": rule_url,
        "rules": around(text(rr), r"wykorzyst", 80, 330, 1) if "body" in rr else rr["err"],
    }


def ex_003():
    u = "https://www.gov.pl/web/kgpsp/interwencje-psp"
    r = get(u)
    t = text(r)
    att = links(t, r"/attachment/", 60, u)
    v = get("https://www.gov.pl/web/kgpsp/ofiary-polska")
    csv = links(text(v), r"/attachment/|\.csv", 2, u)
    return {
        "robots": robots("https://www.gov.pl"),
        "page": r.get("st") or r["err"],
        "attachments": len(att),
        "first": [x[-120:] for x in att[:2]],
        "last": att[-1][-120:] if att else None,
        "head": [head(x.split("|")[-1]) for x in att[:2]],
        "licence": around(t, r"Rodzaj licencji|CC BY", 40, 160, 2),
        "casualties_csv": [x[-110:] for x in csv],
        "casualties_head": head(csv[0].split("|")[-1]) if csv else None,
    }


def env_022():
    u = "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-20202021"
    r = get(u)
    t = text(r)
    return {
        "page": r.get("st") or r["err"],
        "attachments": len(links(t, r"/attachment/", 200, u)),
        "sdrr_xls": head("https://www.gov.pl/attachment/edfc2cc9-e35e-4702-ba86-43f920229bd2"),
        "shp": head("https://www.gov.pl/attachment/540a9afe-df60-4610-bc93-808a925e0ed0"),
        "licence": around(t, r"licencj|Creative Commons", 60, 260, 1),
        "gpr2025": page("https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025", r"/attachment/", 3),
    }


def env_023():
    return {
        "mop_page": page("https://www.gov.pl/web/gddkia/wykaz-parkingow-i-mop", r"/attachment/", 2),
        "disruptions": page("https://www.gov.pl/web/gddkia/dane-o-utrudnieniach-na-drogach-krajowych", r"kpd\.gddkia|\.xml|\.xsd|/attachment/", 4, r"Krajow\w+ Punkt\w* Dostęp|rejestrac|wniosek", 0),
        "rpbdk": page("https://www.gov.pl/web/gddkia/biezaca-realizacja-pbdk-na-lata-2014-2023", r"/attachment/", 2),
    }


def env_024():
    b = "https://api.cepik.gov.pl"
    q = jget(b + "/pojazdy?wojewodztwo=14&data-od=20240101&data-do=20240131&limit=1&page=1")
    if isinstance(q, dict) and isinstance(q.get("json"), dict):
        d = q.pop("json")
        q["meta"] = d.get("meta")
        q["links"] = d.get("links")
        row = (d.get("data") or [{}])[0]
        q["attrs"] = list((row.get("attributes") or {}))[:25]
    s = jget(b + "/slowniki")
    if isinstance(s, dict) and "json" in s:
        d = s.pop("json")
        s["n"] = len(d.get("data", [])) if isinstance(d, dict) else None
    st = jget(b + "/statystyki")
    if isinstance(st, dict) and "json" in st:
        st["sample"] = json.dumps(st.pop("json"), ensure_ascii=False)[:300]
    g = get("https://www.gov.pl/web/cepik/api-dla-centralnej-ewidencji-pojazdow-i-kierowcow-api-do-cepik")
    return {
        "robots": robots(b),
        "doc": meta(get(b + "/doc", 2000)),
        "pojazdy": q,
        "slowniki": s,
        "statystyki": st,
        "terms": around(text(g), r"do celów komercyjnych", 40, 260, 1) if "body" in g else g["err"],
    }


def env_025():
    return {
        "robots": robots("https://dane.utk.gov.pl"),
        "portal": page("https://dane.utk.gov.pl/", FILES + r"|sts/|przewozy|punktualnosc", 6, r"licenc|ponown\w+ wykorzyst|regulamin"),
        "passengers": page("https://dane.utk.gov.pl/sts/przewozy-pasazerskie/dane-eksploatacyjne", FILES + r"|download", 4, r"licenc|ponown\w+ wykorzyst|źródł"),
        "utk_robots": robots("https://utk.gov.pl"),
    }


def env_026():
    u = "https://ulc.gov.pl/statystyki-analizy/statystyki-wg-portow-lotniczych"
    r = get(u)
    t = text(r)
    pdf = links(t, r"\.pdf", 300, u)
    xl = links(t, r"\.(xlsx?|csv|ods)", 50, u)
    return {
        "robots": robots("https://ulc.gov.pl"),
        "page": r.get("st") or r["err"],
        "pdf": len(pdf),
        "xlsx": len(xl),
        "first": [x[-110:] for x in (xl or pdf)[:2]],
        "last": pdf[-1][-110:] if pdf else None,
        "head": head((xl or pdf)[0].split("|")[-1]) if (xl or pdf) else None,
        "terms": around(t, r"licenc|ponown\w+ wykorzyst|prawa zastrz", 60, 200, 1),
    }


def env_027():
    b = "https://www.portgdansk.pl"
    return {
        "robots": robots(b),
        "home": page(b + "/", r"statyst|fakty|przeladunk|\.xlsx?|\.csv", 6, r"prawa zastrzeżone", 0),
    }


def env_028():
    return {
        "ztm_robots": robots("https://www.ztm.waw.pl"),
        "ztm": page("https://www.ztm.waw.pl/pliki-do-pobrania/dane-rozkladowe/", r"ftp|rozklady\.ztm|\.7z|\.zip|gtfs", 5, r"zasad|ftp", 0),
        "api_robots": robots("https://api.um.warszawa.pl"),
        "api": page("https://api.um.warszawa.pl/", r"regulamin|rejestr|login|apikey|\.pdf", 5, r"apikey|rejestrac|regulamin|licenc", 0),
    }


def env_029():
    c = "https://ckan.multimediagdansk.pl"
    d = c + "/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/"
    p = jget(c + "/api/3/action/package_show?id=c24aa637-3619-4dc2-a171-a23eec8f2172", 400000)
    if isinstance(p, dict) and isinstance(p.get("json"), dict):
        res = p.pop("json").get("result", {})
        p = {k: res.get(k) for k in ("license_id", "license_title", "license_url", "metadata_modified", "num_resources")}
    lj = jget(d + "976e1fd1-73d9-4237-b6ba-3c06004d1105/download/linki.json", 20000)
    if isinstance(lj, dict) and "json" in lj:
        lj["sample"] = json.dumps(lj.pop("json"), ensure_ascii=False)[:260]
    t = get("https://gdansk.pl/otwarte-dane-w-gdansku/warunki-korzystania-z-danych,a,123800")
    return {
        "robots": robots(c),
        "zip": head(d + "30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip"),
        "package": p,
        "linki": lj,
        "terms": around(text(t), r"swobodnego wykorzystania", 120, 330, 1) if "body" in t else t["err"],
    }


def env_030():
    b = "https://www.ztm.poznan.pl"
    r = get(b + "/otwarte-dane/gtfsfiles/", 2000000)
    t = text(r)
    names = sorted(set(re.findall(r"\b(20\d{6}_20\d{6})\.zip", t)))
    return {
        "robots": robots(b),
        "index": r.get("st") or r["err"],
        "zips": len(names),
        "first": names[:1],
        "last": names[-2:],
        "latest": head(b + "/pl/dla-deweloperow/getGTFSFile"),
        "named": head(b + f"/pl/dla-deweloperow/getGTFSFile/?file={names[0]}.zip") if names else None,
        "rules_pdf": head(b + "/wp-content/uploads/2024/07/Zasady-korzystania-ZTM-Poznan.pdf"),
        "footer": around(t, r"prawa zastrzeżone|Zasady korzystania", 60, 120, 2),
    }


def env_032():
    b = "https://open-data.cui.wroclaw.pl"
    return {
        "robots": robots(b),
        "meta51": page(b + "/hdb/metadane/51/", FILES + r"|gtfs|download", 4, r"licencj", 0),
        "db14": page(b + "/hdb/db/14", FILES + r"|gtfs|download", 4, r"licencj", 1),
        "old_ckan": page("https://www.wroclaw.pl/open-data/dataset/rozkladjazdytransportupublicznegoplik_data", FILES + r"|gtfs|licen", 4, r"licencj|license", 1),
    }


def env_033():
    d = "https://dev.wyszukiwarka.gunb.gov.pl"
    w = "https://wyszukiwarka.gunb.gov.pl"
    return {
        "robots": {"dev": robots(d), "www": robots(w)},
        "home": page(w + "/", FILES + r"|pobierz|pobranie|api|docs", 5, r"przerwa serwisowa|pobra\w+ dan|licenc|regulamin", 1),
        "download": page(w + "/pobranie-danych/", FILES, 4, r"licenc|regulamin|ponown\w+ wykorzyst", 1),
        "docs": page(d + "/docs", r"bulk|auth|limit|terms|regulamin|openapi|swagger", 6, r"klucz|token|limit|licenc|regulamin", 0),
        "bulk": page(d + "/docs/bulk", FILES, 4, r"zip|csv|limit|licenc", 1),
    }


def dane_gov():
    b = "https://api.dane.gov.pl"
    out = {"robots": robots(b)}
    for key, q in (("policja", "przestępstwa Policja"), ("sw", "Służba Więzienna"), ("utk", "przewozy pasażerskie UTK"), ("wroclaw", "GTFS Wrocław"), ("pkw", "wyniki wyborów"), ("gunb", "RWDZ")):
        r = jget(b + "/1.4/datasets?per_page=2&q=" + urllib.parse.quote(q), 300000)
        if isinstance(r, dict) and isinstance(r.get("json"), dict):
            d = r["json"]
            r = {"count": (d.get("meta") or {}).get("count"), "top": [((x.get("attributes") or {}).get("title") or "")[:60] + "|" + str(((x.get("attributes") or {}).get("license_name")) or "") + "|" + str(x.get("id")) for x in d.get("data", [])[:2]]}
        elif isinstance(r, dict):
            r.pop("json", None)
        out[key] = r
    return out


GROUPS = [
    [("PLG-020", plg_020), ("PLG-021", plg_021)],
    [("PLG-025", plg_025), ("PLG-028", plg_028)],
    [("PLG-029", plg_029), ("PLG-031", plg_031)],
    [("PLG-032", plg_032), ("PLG-033", plg_033)],
    [("EX-003", ex_003), ("PL-ENV-022", env_022)],
    [("PL-ENV-023", env_023), ("PL-ENV-024", env_024)],
    [("PL-ENV-025", env_025), ("PL-ENV-026", env_026)],
    [("PL-ENV-027", env_027), ("PL-ENV-028", env_028), ("dane.gov.pl", dane_gov)],
    [("PL-ENV-029", env_029), ("PL-ENV-030", env_030)],
    [("PL-ENV-032", env_032), ("PL-ENV-033", env_033)],
]
BUDGET = 3600


def run_group(group):
    res = {}
    for sid, fn in group:
        try:
            data = fn()
        except Exception as e:  # noqa: BLE001
            data = {"probe_error": f"{type(e).__name__}: {str(e)[:200]}"}
        cap = BUDGET // len(group)
        if len(json.dumps(data, default=str, separators=(",", ":"))) > cap:
            data = json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))
            while len(json.dumps(data)) > cap:
                data = data[: int(len(data) * 0.93)]
            data += "…"
        res[sid] = data
    return res


def main():
    # One annotation per group of register ids: 10 annotations in this step.
    with cf.ThreadPoolExecutor(len(GROUPS)) as ex:
        for group, res in zip(GROUPS, ex.map(run_group, GROUPS)):
            annotate("notice", "probe " + " ".join(sid for sid, _ in group), res)


if __name__ == "__main__":
    main()
