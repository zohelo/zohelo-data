"""Read-only follow-up probe (PR #2878, second verdict): GUNB RWDZ bulk files and the UOKiK view columns.

GET/HEAD only; robots.txt honoured; a refusing host is reported and never retried another way. No row values
of the RWDZ files are printed (header and row count only), because rows may name persons.
"""

import json
import re

from dev.src_dfh_probe import around, emit, get, head, jget, js_only, links, meta, plain, robots_txt, short, step, text, zip_peek

B = "https://wyszukiwarka.gunb.gov.pl"
D = "https://dev.wyszukiwarka.gunb.gov.pl"


def gunb():
    o = {}
    step(o, "robots", robots_txt, B)
    r = get(B + "/pobranie.html", n=1500000)
    o["pobranie"] = meta(r) if "err" in r else r["st"]
    urls = []
    if "body" in r:
        t = text(r)
        o["title"] = js_only(r)
        ls = links(t, r"\.(zip|csv|7z|gz)\b", 200, B + "/", 40)
        urls = [x.split("|")[-1] for x in ls]
        o["n_files"] = len(ls)
        o["files"] = [short(x) for x in ls[:24]]
        o["text"] = around(t, r"aktualiz|codzienn|osobow|licenc|ponown|wykorzyst|Pliki|plik", 80, 380, 3)
        o["scripts"] = re.findall(r'(?i)<script[^>]+src=["\']([^"\']+)', t)[:5]
    sizes = []
    for u in urls[:24]:
        h = head(u)
        if isinstance(h, dict) and h.get("len") and "html" not in str(h.get("ct")):
            sizes.append((int(h["len"]), u, h.get("lm")))
        elif not isinstance(h, dict) or "html" in str(h.get("ct")):
            o.setdefault("head_other", []).append(f"{u.rsplit('/', 1)[-1]}: {h}"[:120])
    if sizes:
        sizes.sort()
        o["sizes_mb"] = {s[1].rsplit("/", 1)[-1][:30]: round(s[0] / 1e6, 1) for s in sizes}
        o["lm"] = sorted({str(s[2]) for s in sizes})[:4]
        step(o, "smallest", zip_peek, sizes[0][1], 120000000, False, 3, r"\.csv$", 120000000)
    return o


def gunb_terms_api():
    o = {}
    h = get(B + "/", n=1500000)
    o["home"] = meta(h) if "err" in h else {"st": h["st"], "title": js_only(h), "links": [short(x) for x in links(text(h), r"pobra|\.zip|api|regulamin|pomoc|polityk|deklarac|instrukc", 10, B + "/", 34)]}
    if "body" in h:
        o["home_kw"] = around(text(h), r"ponown\w* wykorzyst|licencj|regulamin|jawn|bezpłat|osobow", 80, 380, 3)
        for x in links(text(h), r"regulamin|pomoc|polityk|instrukc", 3, B + "/", 30):
            u = x.split("|")[-1]
            lr = get(u, n=900000)
            o["p:" + short(u)[:50]] = around(text(lr), r"ponown\w* wykorzyst|licencj|źródł|osobow|jawn", 80, 380, 2) if "body" in lr and lr["body"][:4] != b"%PDF" else meta(lr)
    step(o, "robots_dev", robots_txt, D)
    d = get(D + "/docs", n=20000)
    o["docs"] = plain(text(d))[:200] if "body" in d else d["err"]
    if "body" in d:
        o["docs_refs"] = sorted(set(re.findall(r'["\']([^"\']*(?:openapi|swagger|\.json|\.ya?ml)[^"\']*)["\']', text(d))))[:6]
        for ref in o["docs_refs"][:2]:
            j, m = jget(ref if ref.startswith("http") else D + "/" + ref.lstrip("/"), n=3000000)
            o["spec:" + ref[-30:]] = m if j is None else {"info": {k: str(v)[:300] for k, v in (j.get("info") or {}).items()}, "servers": j.get("servers"), "paths": list(j.get("paths", {}))[:20]}
    return o


def uokik_view():
    o = {}
    base = "https://decyzje.uokik.gov.pl/bp/dec_prez.nsf"
    j, m = jget(base + "/WWW-wszystkie?ReadViewEntries&count=4&ExpandView&OutputFormat=JSON", n=400000, timeout=90)
    if j is None:
        return {"view": m}
    o["top"] = {k: v for k, v in j.items() if k.startswith("@")}
    ents = j.get("viewentry", [])
    o["n"] = len(ents)
    rows = []
    for e in ents[:4]:
        cols = []
        for c in e.get("entrydata", []):
            v = c.get("text") or c.get("datetime") or c.get("number") or c.get("textlist") or {}
            cols.append(f"{c.get('@name')}={json.dumps(v, ensure_ascii=False)[:70]}")
        rows.append({"pos": e.get("@position"), "unid": bool(e.get("@unid")), "children": e.get("@children"), "cols": cols})
    o["rows"] = rows
    r = get(base, timeout=90)
    if "body" in r:
        o["views"] = sorted(set(re.findall(r"dec_prez\.nsf/([A-Za-z0-9_\-+%]+)\?OpenView", text(r))))[:14]
        o["dkk_mentions"] = around(text(r), r"koncentrac|DKK", 60, 160, 2)
    j2, m2 = jget(base + "/WWW-wszystkie?ReadViewEntries&count=3&ExpandView&StartKey=DKK&OutputFormat=JSON", n=200000, timeout=90)
    o["startkey_dkk"] = m2 if j2 is None else [[(c.get("text") or c.get("datetime") or {}).get("0", "")[:40] for c in e.get("entrydata", [])][:4] for e in j2.get("viewentry", [])[:3]]
    return o


def main():
    parts = []
    for sid, fn in (("PL-ENV-033 files", gunb), ("PL-ENV-033 terms api", gunb_terms_api), ("PLG-021 view", uokik_view)):
        try:
            parts.append((sid, fn()))
        except Exception as e:  # noqa: BLE001
            parts.append((sid, {"probe_error": f"{type(e).__name__}: {str(e)[:200]}"}))
    for sid, data in parts:
        emit("probe " + sid, [(sid, data)])


if __name__ == "__main__":
    main()
