"""Read-only Dev probe for PLG-016 (#2994): the Polish slice of the ESMA registers Solr core and the Legal Notice
link of registers.esma.europa.eu. Writes nothing to R2. Prints field fill counts, closed lists and formats; no
address contents. Deleted before merge."""

from __future__ import annotations

from collections import Counter
from hashlib import sha256
from html.parser import HTMLParser
import json
import re
import time
from urllib.parse import urljoin

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
SOLR = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select"
BASE = {
    "q": "*:*",
    "fq": ['ae_homeMemberState:"POLAND"', "type_s:parent"],
    "wt": "json",
    "omitHeader": "true",
    "sort": "id asc",
}


def get(url, **kw):
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 120), **kw)
            return r
        except requests.RequestException as exc:
            print("retry", url, exc)
            time.sleep(10 * (attempt + 1))
    raise RuntimeError("unreachable " + url)


class Links(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links, self._href, self._text = [], None, []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._href = dict(attrs).get("href")
            self._text = []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            self.links.append((self._href, " ".join("".join(self._text).split())))
            self._href = None


def solr():
    r = get(SOLR, params={**BASE, "rows": 1000, "start": 0})
    print("STATUS", r.status_code, r.headers.get("Content-Type"), "bytes", len(r.content), "url", r.url)
    print("HEADERS", {k: v for k, v in r.headers.items() if k.lower() in ("last-modified", "etag", "content-length", "cache-control")})
    body = r.json()
    print("TOP KEYS", list(body))
    resp = body["response"]
    docs = resp["docs"]
    print("numFound", resp["numFound"], "docs", len(docs), "start", resp.get("start"))
    time.sleep(3)
    r2 = get(SOLR, params={**BASE, "rows": 1000, "start": 0})
    print("SAME BYTES ON SECOND REQUEST", sha256(r.content).hexdigest() == sha256(r2.content).hexdigest(), len(r2.content))
    time.sleep(3)
    r3 = get(SOLR, params={**BASE, "rows": 1000, "start": 1000})
    print("PAGE 2", r3.status_code, len(r3.content), r3.json()["response"]["numFound"], len(r3.json()["response"]["docs"]))
    fields = Counter()
    types = {}
    for d in docs:
        for k, v in d.items():
            fields[k] += 1
            types.setdefault(k, Counter())[type(v).__name__] += 1
    print("FIELDS (filled count, types):")
    for k, c in sorted(fields.items()):
        print(f"  {k}: {c} {dict(types[k])}")
    ids = [d.get("id") for d in docs]
    print("ids unique", len(set(ids)) == len(ids), "sample", ids[:3])
    print("dbId unique", len({d.get('ae_dbId') for d in docs}), "of", len(docs))
    closed = ["ae_entityTypeCode", "ae_entityTypeLabel", "ae_status", "ae_officeType", "ae_competentAuthority",
              "ae_homeMemberState", "type_s", "_root_", "ae_registerType", "ae_entityType"]
    for k in sorted(set(fields) - {"id", "ae_dbId", "ae_entityName", "ae_lei", "ae_headOfficeLei", "_version_"}):
        vals = Counter(json.dumps(d.get(k), ensure_ascii=False)[:80] for d in docs)
        if "addr" in k.lower() or "street" in k.lower() or "city" in k.lower() or "postal" in k.lower() or "phone" in k.lower() or "mail" in k.lower() or "web" in k.lower():
            print(f"DISTINCT {k}: {len(vals)} values (content not printed); value types {dict(types[k])}")
            sample = next((d[k] for d in docs if d.get(k)), None)
            if isinstance(sample, list) and sample and isinstance(sample[0], str):
                s = sample[0]
                print(f"   shape of first element: len {len(s)}, separators {Counter(ch for ch in s if not ch.isalnum() and ch != ' ').most_common(6)}")
            continue
        if len(vals) <= 40 or k in closed:
            print(f"DISTINCT {k} ({len(vals)}): {vals.most_common(40)}")
        else:
            print(f"DISTINCT {k}: {len(vals)} values, samples {[v for v, _ in vals.most_common(5)]}")
    for k in ("ae_authorisationNotificationDate", "ae_lastUpdate", "ae_withdrawalDate", "ae_endDate"):
        vs = [d.get(k) for d in docs if d.get(k) is not None]
        if vs:
            shapes = Counter(re.sub(r"\d", "9", str(v)) for v in vs)
            print(f"DATE {k}: n={len(vs)} shapes={shapes.most_common(5)} min={min(map(str, vs))} max={max(map(str, vs))}")
    leis = [d.get("ae_lei") for d in docs]
    print("LEI filled", sum(1 for x in leis if x), "shape", Counter(re.sub(r"[0-9A-Z]", "x", str(x)) for x in leis if x).most_common(3))
    lei_dup = Counter(x for x in leis if x)
    print("LEI repeated", [(k, c) for k, c in lei_dup.items() if c > 1][:10], "count", sum(1 for c in lei_dup.values() if c > 1))
    print("by type x status", Counter((d.get("ae_entityTypeCode"), d.get("ae_status")) for d in docs).most_common())
    print("by type x office", Counter((d.get("ae_entityTypeCode"), d.get("ae_officeType")) for d in docs).most_common())
    print("LEI filled by type", Counter(d.get("ae_entityTypeCode") for d in docs if d.get("ae_lei")).most_common())
    suffix = Counter()
    for d in docs:
        name = (d.get("ae_entityName") or "").upper()
        m = re.search(r"(S\.A\.|SA|SP\. Z O\.O\.|SP\.Z O\.O\.|SPÓŁKA AKCYJNA|SPÓŁKA Z OGRANICZONĄ|S\.K\.A|SP\. K|SPÓŁKA KOMANDYTOWA|FIZ|SFIO|FIO|ASI|LIMITED|LTD|GMBH|AG|PLC|BRANCH|ODDZIAŁ|TFI|DOM MAKLERSKI|BANK)", name)
        suffix[m.group(1) if m else "NONE"] += 1
    print("NAME LEGAL-FORM MARKERS", suffix.most_common())
    for want in ("PKO", "PEKAO", "XTB", "QUERCUS", "IPOPEMA"):
        hits = [{k: v for k, v in d.items() if "addr" not in k.lower()} for d in docs if want in (d.get("ae_entityName") or "").upper()][:2]
        print("SPOT", want, json.dumps(hits, ensure_ascii=False)[:1500])
    print("ONE DOC KEYS+VALUES (no address fields)", json.dumps({k: v for k, v in docs[0].items() if "addr" not in k.lower()}, ensure_ascii=False)[:2000])


def legal():
    for page in ("https://registers.esma.europa.eu/publication/", "https://registers.esma.europa.eu/publication/searchRegister?core=esma_registers_upreg", "https://registers.esma.europa.eu/publication/helpApp"):
        r = get(page)
        print("PAGE", page, r.status_code, len(r.content), r.url)
        p = Links()
        p.feed(r.text)
        for href, text in p.links:
            if re.search(r"legal|notice|privacy|copyright|disclaimer|terms", (href or "") + " " + text, re.I):
                print("   LINK", repr(text), "->", urljoin(r.url, href or ""))
        if "helpApp" in page:
            text = re.sub(r"<[^>]+>", " ", r.text)
            for m in re.finditer(r"[^.]{0,200}(limit|rows|throttl|fair use|terms|reuse|re-use)[^.]{0,200}\.", text, re.I):
                print("   HELP", " ".join(m.group(0).split())[:400])
        time.sleep(3)
    for target in ("https://registers.esma.europa.eu/publication/legalNotice", "https://www.esma.europa.eu/about-esma/legal-notice-and-data-protection", "https://www.esma.europa.eu/legal-notice"):
        r = get(target, allow_redirects=True)
        text = " ".join(re.sub(r"<[^>]+>", " ", r.text).split())
        i = text.find("Reproduction")
        print("TARGET", target, r.status_code, "final", r.url, "len", len(text), "reproduction at", i)
        if i >= 0:
            print("   QUOTE", text[i:i + 900])
        time.sleep(3)


if __name__ == "__main__":
    solr()
    legal()
