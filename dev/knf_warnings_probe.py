"""Dev probe for PLG-019 (KNF public warning list, Epic #476; test branch only, never on main).

Read-only, one GET of https://www.knf.gov.pl/dla_konsumenta/ostrzezenia_publiczne (robots.txt allows all). Nothing is
stored. The log and annotations get only structure: tables, headers, row counts, masked shapes of the identifier
column, the prosecutor offices (institutions), keyword counts of the judgment column and the tag skeleton of one row
with text replaced by its length. Never a name or a free-text value.
"""

from __future__ import annotations

from collections import Counter
from html.parser import HTMLParser
import html
import json
import re

import requests

from ingestion.common.report import annotate

URL = "https://www.knf.gov.pl/dla_konsumenta/ostrzezenia_publiczne"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


class Tables(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables, self.stack, self.heading, self.headings, self.cell = [], [], "", [], None
        self.in_heading, self.skeleton, self.links = None, [], []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "a" and a.get("href"):
            self.links.append(a["href"])
        if tag in ("h1", "h2", "h3", "h4", "h5", "strong", "caption", "summary", "button"):
            self.in_heading, self.heading_buf = tag, []
        if tag == "table":
            self.stack.append({"heading": self.heading, "rows": [], "attrs": {k: v for k, v in attrs if k in ("class", "id")}})
        elif tag == "tr" and self.stack:
            self.stack[-1]["rows"].append([])
        elif tag in ("td", "th") and self.stack and self.stack[-1]["rows"]:
            self.cell = {"tag": tag, "text": [], "inner": [], "colspan": a.get("colspan"), "rowspan": a.get("rowspan")}
            self.stack[-1]["rows"][-1].append(self.cell)
        elif self.cell is not None:
            self.cell["inner"].append(tag)

    def handle_endtag(self, tag):
        if tag == self.in_heading:
            text = " ".join(" ".join(self.heading_buf).split())
            if text:
                self.heading = text[:160]
                self.headings.append((tag, text[:160]))
            self.in_heading = None
        if tag in ("td", "th"):
            self.cell = None
        if tag == "table" and self.stack:
            self.tables.append(self.stack.pop())

    def handle_data(self, data):
        if self.in_heading:
            self.heading_buf.append(data)
        if self.cell is not None:
            self.cell["text"].append(data)


def text(cell) -> str:
    return " ".join(" ".join(cell["text"]).split())


def shape(value: str) -> str:
    out = re.sub(r"\d", "9", value)
    out = re.sub(r"\b(?!KRS\b|NIP\b|REGON\b|nr\b|Nr\b)[^\W\d_]+", "a", out)
    return re.sub(r"a( a)+", "a+", out)[:60]

def second(body: str, p: Tables) -> None:
    """Section titles before each table, Lp. shapes, short repeated values of the identifier and judgment columns."""
    starts = [m.start() for m in re.finditer(r"<table", body)]
    before = []
    for i, st in enumerate(starts):
        prev = body[max(0, st - 4000):st]
        txt = " ".join(re.sub(r"(?s)<script.*?</script>|<[^>]+>", " ", prev).split())
        before.append({"i": i, "tail": html.unescape(txt[-260:])})
    annotate("notice", "knf_warnings probe2 sections", {"n": len(starts), "before": before})
    lp = []
    for i, t in enumerate(p.tables):
        vals = [text(rw[0]) for rw in t["rows"][1:]]
        lp.append({"i": i, "first": vals[:2], "last": vals[-1:], "shapes": dict(Counter(re.sub(r"\d", "9", v) for v in vals).most_common(4))})
    m = re.search(r"Liczba znalezionych.{0,120}", re.sub(r"<[^>]+>", " ", body), re.S)
    annotate("notice", "knf_warnings probe2 numbering", {"lp": lp, "count_text": " ".join(m.group(0).split()) if m else None})
    rows = [rw for t in p.tables for rw in t["rows"][1:]]
    idv = Counter(text(rw[2]) for rw in rows if re.fullmatch(r"[^\d]*", text(rw[2])))
    jdv = Counter(text(rw[4]) for rw in rows if len(text(rw[4]).split()) <= 3)
    annotate("notice", "knf_warnings probe2 short values", {
        "identifier_no_digits_repeated": [(v[:40], c) for v, c in idv.most_common() if c >= 3],
        "identifier_no_digits_lengths": sorted(Counter(len(v) for v, c in idv.items() if c < 3).items())[:30],
        "judgment_short_repeated": [(v[:40], c) for v, c in jdv.most_common() if c >= 3],
        "judgment_short_once": sum(1 for v, c in jdv.items() if c < 3),
    })
    # do judgment cells name persons? count capitalised word pairs (shape only)
    caps = Counter()
    for rw in rows:
        v = text(rw[4])
        caps["cells_with_capital_pair"] += bool(re.search(r"\b[A-ZŁŚŻŹĆ][a-ząćęłńóśźż]+ [A-ZŁŚŻŹĆ][a-ząćęłńóśźż]+", v))
        caps["cells_with_sygn"] += "sygn" in v.lower()
        caps["cells_with_date"] += bool(re.search(r"\d{1,2}[ .]\w+[ .]\d{4}", v))
    annotate("notice", "knf_warnings probe2 judgment shapes", dict(caps))


def main() -> int:
    r = requests.get(URL, headers={"User-Agent": UA}, timeout=(30, 120))
    body = r.content.decode(r.encoding or "utf-8", "replace")
    count = re.search(r"Liczba znalezionych ostrze[^:]*:\s*(\d+)", body)
    p = Tables()
    p.feed(body)
    head = {"status": r.status_code, "bytes": len(r.content), "content_type": r.headers.get("Content-Type"),
            "last_modified": r.headers.get("Last-Modified"), "etag": r.headers.get("ETag"),
            "count_line": count.group(1) if count else None, "tables": len(p.tables),
            "headings": [h for h in p.headings if not h[0] == "strong"][:40],
            "export_links": sorted({l for l in p.links if re.search(r"(csv|xls|rss|json|xml|pdf)", l, re.I)})[:20]}
    annotate("notice", "knf_warnings probe page", head)
    print(json.dumps(head, ensure_ascii=False), flush=True)
    summary = []
    for i, t in enumerate(p.tables):
        rows = t["rows"]
        widths = Counter(len(rw) for rw in rows)
        header = [text(c)[:40] for c in rows[0]] if rows else []
        summary.append({"i": i, "heading": t["heading"][:100], "attrs": t["attrs"], "rows": len(rows),
                        "widths": dict(widths), "header": header,
                        "th_rows": sum(1 for rw in rows if rw and all(c["tag"] == "th" for c in rw))})
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    print("TABLES"); (lambda *a: None)("notice", "knf_warnings probe tables", {"n": len(summary), "tables": summary[:30]})
    big = sorted(p.tables, key=lambda t: -len(t["rows"]))
    data_rows = [rw for t in p.tables for rw in t["rows"][1:]]
    widths = Counter(len(rw) for rw in data_rows)
    ncol = widths.most_common(1)[0][0] if widths else 0
    cols = []
    headers = Counter(tuple(text(c)[:40] for c in t["rows"][0]) for t in p.tables if t["rows"])
    for j in range(ncol):
        vals = [text(rw[j]) for rw in data_rows if len(rw) == ncol]
        cols.append({"j": j, "nonempty": sum(1 for v in vals if v), "avg_len": round(sum(map(len, vals)) / max(len(vals), 1), 1),
                     "max_len": max(map(len, vals), default=0), "digits_only": sum(1 for v in vals if v.isdigit()),
                     "inner_tags": dict(Counter(tag for rw in data_rows if len(rw) == ncol for tag in rw[j]["inner"]).most_common(6)),
                     "krs": sum("KRS" in v for v in vals), "nip": sum("NIP" in v for v in vals), "regon": sum("REGON" in v for v in vals),
                     "ten_digits": sum(bool(re.search(r"(?<!\d)\d{10}(?!\d)", v)) for v in vals),
                     "prokuratura": sum("rokurat" in v for v in vals), "wyrok": sum("wyrok" in v.lower() for v in vals)})
    annotate("notice", "knf_warnings probe columns", {"ncol": ncol, "widths": dict(widths), "header_sets": [(list(k), c) for k, c in headers.most_common(5)], "columns": cols})
    print(json.dumps(cols, ensure_ascii=False), flush=True)
    # identifier column: the one with most KRS/NIP/REGON
    if cols:
        idc = max(cols, key=lambda c: c["krs"] + c["nip"] + c["regon"])["j"]
        prc = max(cols, key=lambda c: c["prokuratura"])["j"]
        jdc = max(cols, key=lambda c: c["wyrok"])["j"]
        ids = Counter(shape(text(rw[idc])) for rw in data_rows if len(rw) == ncol)
        pro = Counter(text(rw[prc])[:90] for rw in data_rows if len(rw) == ncol)
        kw = Counter()
        for rw in data_rows:
            if len(rw) == ncol:
                v = text(rw[jdc]).lower()
                for k in ("prawomocn", "nieprawomocn", "wyrok", "skazując", "uniewinn", "umorz", "akt oskarżenia", "postanowieni", "warunkowo", "odmow", "zawieszon", "brak"):
                    kw[k] += k in v
                kw["empty"] += not v
        jshapes = Counter(len(text(rw[jdc]).split()) for rw in data_rows if len(rw) == ncol)
        annotate("notice", "knf_warnings probe identifiers", {"column": idc, "shapes": ids.most_common(25), "distinct": len(ids)})
        (lambda *a: None)("notice", "knf_warnings probe prosecutors", {"column": prc, "distinct": len(pro), "top": pro.most_common(45)})
        annotate("notice", "knf_warnings probe judgments", {"column": jdc, "keywords": dict(kw), "word_counts": sorted(jshapes.items())[:30]})
        # skeleton of first 2 data rows of the biggest table
        sk = []
        for rw in big[0]["rows"][:3]:
            sk.append([(c["tag"], c["colspan"], c["rowspan"], len(text(c)), c["inner"][:6]) for c in rw])
        annotate("notice", "knf_warnings probe skeleton", {"rows": sk, "table_headings_by_rows": Counter(t["heading"][:80] for t in p.tables).most_common(20)})
        print("SKELETON", sk, flush=True)
    second(body, p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
