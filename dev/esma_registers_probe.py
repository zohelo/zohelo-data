"""Read-only Dev probe (PLG-016 ESMA registers, #2994): the shape of the Polish slice of esma_registers_upreg.

One Solr request for every parent entry with home member state Poland (all fields, rows=2000, wt=json), profiled in
memory: field names and fill counts, distinct values of the closed lists, LEI fill, id uniqueness, child documents,
then robots.txt and the footer links of registers.esma.europa.eu (the Legal Notice target is read and its reuse
sentences quoted). Nothing is written to R2. Addresses are counted, never printed. Deleted before merge.
"""

from __future__ import annotations

from collections import Counter
from html.parser import HTMLParser
import json
import re
import time
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
SELECT = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select"
CLOSED = ["ae_entityTypeCode", "ae_entityTypeLabel", "ae_status", "ae_officeType", "ae_competentAuthority",
          "ae_homeMemberState", "entity_type", "type_s", "ae_legalform"]  # fmt: skip


def get(url, params=None, timeout=120):
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=(30, timeout))
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(20 * (attempt + 1))
                continue
            return r
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(20 * (attempt + 1))
    raise RuntimeError("unreachable")


class Links(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links, self._href, self._text = [], None, []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._href, self._text = dict(attrs).get("href"), []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            self.links.append((" ".join("".join(self._text).split()), self._href))
            self._href = None


def plain(html):
    html = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", html)
    return " ".join(re.sub(r"(?s)<[^>]+>", " ", html).split())


def slice_profile():
    params = {"q": "*:*", "fq": ['ae_homeMemberState:"POLAND"', "type_s:parent"], "rows": 2000, "wt": "json",
              "sort": "id asc"}  # fmt: skip
    r = get(SELECT, params)
    body = r.content
    d = r.json()
    resp = d["response"]
    docs = resp["docs"]
    fields = Counter()
    filled = Counter()
    types = {}
    for doc in docs:
        for k, v in doc.items():
            fields[k] += 1
            types.setdefault(k, set()).add(type(v).__name__)
            if v not in ("", None, []):
                filled[k] += 1
    closed = {f: Counter(str(doc.get(f)) for doc in docs).most_common(25) for f in CLOSED}
    ids = Counter(doc.get("id") for doc in docs)
    annotate("notice", "esma probe: Polish slice", {
        "http": r.status_code, "bytes": len(body), "numFound": resp.get("numFound"), "docs": len(docs),
        "header_keys": sorted(d.keys()), "content_type": r.headers.get("Content-Type"),
        "last_modified": r.headers.get("Last-Modified"), "etag": r.headers.get("ETag"),
        "dup_ids": sum(1 for v in ids.values() if v > 1), "ids_without_ae_prefix": sum(1 for i in ids if not str(i).startswith("ae")),
        "fields_present_filled_type": {k: [fields[k], filled[k], sorted(types[k])] for k in sorted(fields)},
    })
    annotate("notice", "esma probe: closed lists", closed)
    print("CLOSED", json.dumps(closed, ensure_ascii=False))
    lei = Counter()
    for doc in docs:
        lei[(doc.get("ae_officeType"), doc.get("ae_status"), bool(doc.get("ae_lei")), bool(doc.get("ae_headOfficeLei")))] += 1
    leis = Counter(doc.get("ae_lei") for doc in docs if doc.get("ae_lei"))
    corp = re.compile(r"(?i)\b(s\.?\s?a\.?|sp\.?\s?z\s?o\.?\s?o\.?|sp\.\s*k\.?|s\.k\.a\.?|spółka|tfi|towarzystwo|"
                      r"fundusz|dom maklerski|bank|ltd|limited|gmbh|ag|plc|asi|s\.?a\.?\b|alternatywna|zarządzając|"
                      r"oddział|branch|fióktelep|giełda|exchange|lithuania|sp\.j\.?|spółka jawna|komandytow|inc|llc|se|n\.v\.|b\.v\.)")  # fmt: skip
    no_marker = [doc.get("ae_entityName") for doc in docs if not corp.search(doc.get("ae_entityName") or "")]
    dates = {f: [min((doc.get(f) or "~") for doc in docs), max((doc.get(f) or "") for doc in docs),
                 sum(1 for doc in docs if not doc.get(f))] for f in ("ae_authorisationNotificationDate", "ae_lastUpdate",
                 "ae_authorisationWithdrawalEndDate", "timestamp")}  # fmt: skip
    hosts = Counter(len(doc.get("ae_hostMemberStates") or []) for doc in docs)
    annotate("notice", "esma probe: LEI, dates, names", {
        "office_status_lei_headlei": [[*k, v] for k, v in sorted(lei.items(), key=str)],
        "lei_repeated": [[k, v] for k, v in leis.items() if v > 1][:20],
        "lei_bad_shape": [k for k in leis if not re.fullmatch(r"[A-Z0-9]{18}[0-9]{2}", k)][:10],
        "dates_min_max_empty": dates, "host_states_per_entry": sorted(hosts.items()),
        "names_without_legal_form_marker": len(no_marker),
        "per_type_label": Counter(f"{doc.get('ae_entityTypeCode')}|{doc.get('ae_entityTypeLabel')}" for doc in docs).most_common(),
        "comment_filled": sum(1 for doc in docs if doc.get("ae_comment")),
    })
    print("NO_MARKER_NAMES", json.dumps(no_marker[:80], ensure_ascii=False))
    print("COMMENTS", json.dumps([doc.get("ae_comment") for doc in docs if doc.get("ae_comment")][:30], ensure_ascii=False))
    print("SAMPLE", json.dumps([{k: v for k, v in doc.items() if "ddress" not in k} for doc in docs[:3]], ensure_ascii=False))
    time.sleep(3)
    # child documents of the Polish parents (services, passports): counted only
    c = get(SELECT, {"q": "*:*", "fq": ["type_s:child", '_root_:ae600'], "rows": 3, "wt": "json"}).json()
    annotate("notice", "esma probe: child docs of one entry", {
        "numFound": c.get("response", {}).get("numFound"),
        "keys": sorted({k for doc in c.get("response", {}).get("docs", []) for k in doc})[:40]})
    # repeat request: are the bytes identical (SHA-256 stability for Landing)?
    time.sleep(3)
    r2 = get(SELECT, params)
    import hashlib

    print("REPEAT", hashlib.sha256(body).hexdigest(), hashlib.sha256(r2.content).hexdigest())
    params_h = {**params, "omitHeader": "true"}
    time.sleep(3)
    a = get(SELECT, params_h).content
    time.sleep(3)
    b = get(SELECT, params_h).content
    return {"same_bytes_with_header": body == r2.content, "same_bytes_omitHeader": a == b,
            "omitHeader_top_keys": sorted(json.loads(a).keys()), "omitHeader_bytes": len(a)}


def legal():
    out = {}
    r = get("https://registers.esma.europa.eu/robots.txt", timeout=60)
    out["robots"] = {"http": r.status_code, "type": r.headers.get("Content-Type"), "head": r.text[:160]}
    for page in ("https://registers.esma.europa.eu/", "https://registers.esma.europa.eu/publication/"):
        time.sleep(3)
        r = get(page, timeout=60)
        parser = Links()
        parser.feed(r.text)
        out[page] = {"http": r.status_code, "final": r.url, "bytes": len(r.content),
                     "links": [[t[:60], urljoin(r.url, h)] for t, h in parser.links
                               if re.search(r"(?i)legal|notice|copyright|disclaimer|privacy|cookie|terms", t + " " + h)][:15],
                     "all_links": len(parser.links)}
        print("PAGE", page, r.status_code, json.dumps(parser.links[:120], ensure_ascii=False))
        print("TEXT", plain(r.text)[:3000])
    targets = []
    for page in ("https://registers.esma.europa.eu/", "https://registers.esma.europa.eu/publication/"):
        for _t, u in out[page]["links"]:
            if re.search(r"(?i)legal", _t + u) and u not in targets:
                targets.append(u)
    out["read"] = {}
    for u in targets[:3]:
        time.sleep(3)
        r = get(u, timeout=60)
        text = plain(r.text)
        hits = [text[max(0, m.start() - 150): m.start() + 650] for m in re.finditer(r"(?i)reproduc|re-?use|copyright", text)][:4]
        out["read"][u] = {"http": r.status_code, "final": r.url, "hits": hits}
        print("LEGAL", u, r.status_code, r.url, text[:6000])
    return out


def main() -> int:
    try:
        rep = slice_profile()
        annotate("notice", "esma probe: byte stability", rep)
    except Exception as exc:  # noqa: BLE001
        annotate("error", "esma probe: slice failed", {"error": f"{type(exc).__name__}: {str(exc)[:600]}"})
    try:
        out = legal()
        annotate("notice", "esma probe: robots and footer links", {k: v for k, v in out.items() if k != "read"})
        annotate("notice", "esma probe: legal notice read", out.get("read"))
    except Exception as exc:  # noqa: BLE001
        annotate("error", "esma probe: legal failed", {"error": f"{type(exc).__name__}: {str(exc)[:600]}"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
