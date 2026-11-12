"""Read-only Wikidata SPARQL probe: Polish units by TERC (P1653). One query at a time; nothing is written."""

from collections import Counter
import json
import time

import requests

UA = "zohelo-data/1.0 (https://zohelo.com; public statistics platform) python-requests"
URL = "https://query.wikidata.org/sparql"
PREFIX = ""

QUERIES = {
    "units": """SELECT ?item ?terc ?rank ?end ?dissolved WHERE {
  ?item p:P1653 ?s . ?s ps:P1653 ?terc ; wikibase:rank ?rank .
  OPTIONAL { ?s pq:P582 ?end } OPTIONAL { ?item wdt:P576 ?dissolved } }""",
    "types": """SELECT ?type (COUNT(DISTINCT ?item) AS ?n) WHERE { ?item wdt:P1653 ?terc ; wdt:P31 ?type } GROUP BY ?type
  ORDER BY DESC(?n) LIMIT 25""",
    "population": """SELECT ?item ?s ?rank ?pop ?time WHERE { ?item p:P1653 [] ; p:P1082 ?s .
  ?s ps:P1082 ?pop ; wikibase:rank ?rank . OPTIONAL { ?s pq:P585 ?time } }""",
    "area": """SELECT ?item ?s ?rank ?amount ?unit ?time WHERE { ?item p:P1653 [] ; p:P2046 ?s .
  ?s psv:P2046 [ wikibase:quantityAmount ?amount ; wikibase:quantityUnit ?unit ] ; wikibase:rank ?rank .
  OPTIONAL { ?s pq:P585 ?time } }""",
    "coordinates": """SELECT ?item ?s ?rank ?coord WHERE { ?item p:P1653 [] ; p:P625 ?s .
  ?s ps:P625 ?coord ; wikibase:rank ?rank . }""",
    "website": """SELECT ?item ?s ?rank ?url ?lang ?end WHERE { ?item p:P1653 [] ; p:P856 ?s .
  ?s ps:P856 ?url ; wikibase:rank ?rank . OPTIONAL { ?s pq:P407 ?lang } OPTIONAL { ?s pq:P582 ?end } }""",
    "names": """SELECT ?item ?label_pl ?label_en ?article_pl ?article_en WHERE { ?item p:P1653 [] .
  OPTIONAL { ?item rdfs:label ?label_pl FILTER(LANG(?label_pl) = "pl") }
  OPTIONAL { ?item rdfs:label ?label_en FILTER(LANG(?label_en) = "en") }
  OPTIONAL { ?article_pl schema:about ?item ; schema:isPartOf <https://pl.wikipedia.org/> }
  OPTIONAL { ?article_en schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> } }""",
    "prop": """SELECT ?p ?l WHERE { VALUES ?p { wd:P1653 } ?p rdfs:label ?l FILTER(LANG(?l)="en") }""",
}


def run(name, query):
    t0 = time.time()
    r = requests.get(URL, params={"query": query, "format": "json"}, headers={"User-Agent": UA,
                     "Accept": "application/sparql-results+json"}, timeout=120)
    dt = time.time() - t0
    head = {k: r.headers.get(k) for k in ("content-type", "last-modified", "etag", "x-served-by", "age")}
    if r.status_code != 200:
        print(f"=== {name} status={r.status_code} {dt:.1f}s {r.text[:400]}", flush=True)
        return None
    rows = r.json()["results"]["bindings"]
    print(f"=== {name} status=200 {dt:.1f}s bytes={len(r.content)} rows={len(rows)} headers={head}", flush=True)
    return rows


def v(row, key):
    return row.get(key, {}).get("value")


def main():
    res = {}
    for name, q in QUERIES.items():
        res[name] = run(name, q)
        time.sleep(3)
    units = res["units"] or []
    print("prop", res["prop"])
    print("types", [(v(r, "type").rsplit("/", 1)[-1], v(r, "n")) for r in res["types"] or []])
    items = {v(r, "item") for r in units}
    print("items", len(items), "statements", len(units))
    print("ranks", Counter(v(r, "rank").rsplit("#", 1)[-1] for r in units))
    print("terc lengths", Counter(len(v(r, "terc")) for r in units))
    print("with end", sum(1 for r in units if v(r, "end")), "dissolved items", len({v(r, "item") for r in units if v(r, "dissolved")}))
    by_terc = {}
    for r in units:
        by_terc.setdefault(v(r, "terc"), set()).add(v(r, "item"))
    multi = {t: s for t, s in by_terc.items() if len(s) > 1}
    print("terc codes", len(by_terc), "codes on >1 item", len(multi), list(multi.items())[:8])
    cur = [r for r in units if not v(r, "dissolved") and not v(r, "end") and not v(r, "rank").endswith("Deprecated")]
    by_terc2 = {}
    for r in cur:
        by_terc2.setdefault(v(r, "terc"), set()).add(v(r, "item"))
    multi2 = {t: s for t, s in by_terc2.items() if len(s) > 1}
    print("current codes", len(by_terc2), Counter(len(t) for t in by_terc2), "current codes on >1 item", len(multi2), list(multi2.items())[:12])
    by_item = Counter(v(r, "item") for r in cur)
    print("items with >1 current code", sum(1 for c in by_item.values() if c > 1), [ (i, [v(r,'terc') for r in cur if v(r,'item')==i]) for i,c in by_item.items() if c>1][:8])
    print("bad codes", [t for t in by_terc if not t.isdigit()][:10])
    for name in ("population", "area", "coordinates", "website"):
        rows = res[name] or []
        per = Counter(v(r, "item") for r in rows)
        pref = Counter(v(r, "item") for r in rows if v(r, "rank").endswith("PreferredRank"))
        print(name, "statements", len(rows), "items", len(per), "items with >1", sum(1 for c in per.values() if c > 1),
              "max", max(per.values() or [0]), "ranks", Counter(v(r, "rank").rsplit("#", 1)[-1] for r in rows),
              "items with >1 preferred", sum(1 for c in pref.values() if c > 1))
        print("  sample", json.dumps(rows[:2])[:700])
    pop = res["population"] or []
    print("population times", Counter((v(r, "time") or "none")[:4] for r in pop).most_common(30))
    print("population no time", sum(1 for r in pop if not v(r, "time")))
    print("area units", Counter(v(r, "unit") for r in res["area"] or []))
    print("coord sample", Counter((v(r, "coord") or "")[:6] for r in res["coordinates"] or []))
    print("website langs", Counter(v(r, "lang") for r in res["website"] or []).most_common(8))
    names = res["names"] or []
    print("names rows", len(names), "items", len({v(r, 'item') for r in names}), "no pl label", sum(1 for r in names if not v(r, "label_pl")),
          "no en label", sum(1 for r in names if not v(r, "label_en")), "no plwiki", sum(1 for r in names if not v(r, "article_pl")),
          "no enwiki", sum(1 for r in names if not v(r, "article_en")))
    print("names sample", json.dumps(names[:2])[:600])


if __name__ == "__main__":
    main()
