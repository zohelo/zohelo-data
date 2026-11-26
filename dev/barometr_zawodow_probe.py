"""Read-only probe of Barometr Zawodów publications (register PLS-030, Epic #454). Writes nothing to R2.

Reads the national publication pages 2016-2027 (links only), the publication form's fields, one voivodeship page
built from the form, and the national report and poster PDFs the pages link. Prints the PDFs' text layer to the
job log (layout mode), a colour/operator summary of the pages that look like an occupation x voivodeship matrix,
and the base64 of a few small files so the parser can be designed offline. One request at a time, 2.5 s apart.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import sys
import time
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

BASE = "https://barometrzawodow.pl"
PAGE = BASE + "/modul/publikacje?publication={pub}&year={year}"
PAUSE = 2.5
WORDS = re.compile(r"deficyt|równowag|nadwyż|nadwyz", re.IGNORECASE)
VOIV = ["dolnośląskie", "kujawsko", "lubelskie", "lubuskie", "łódzkie", "małopolskie", "mazowieckie", "opolskie",
        "podkarpackie", "podlaskie", "pomorskie", "śląskie", "świętokrzyskie", "warmińsko", "wielkopolskie",
        "zachodniopomorskie"]
session = requests.Session()
session.headers["User-Agent"] = USER_AGENT
requests_made = 0


def get(url: str) -> requests.Response | None:
    global requests_made
    for attempt in range(3):
        time.sleep(PAUSE)
        requests_made += 1
        try:
            r = session.get(url, timeout=(20, 120))
            print(f"GET {url} -> {r.status_code} {r.headers.get('content-type')} {len(r.content)}", flush=True)
            return r
        except requests.RequestException as exc:
            print(f"GET {url} failed {type(exc).__name__}: {exc}", flush=True)
            time.sleep(10 * (attempt + 1))
    return None


class Page(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[list[str]] = []
        self.open = False
        self.selects: list[dict] = []
        self.inputs: list[dict] = []
        self.forms: list[dict] = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "a":
            self.links.append([a.get("href") or "", ""])
            self.open = True
        elif tag == "form":
            self.forms.append({k: a.get(k) for k in ("action", "method", "id")})
        elif tag == "select":
            self.selects.append({"name": a.get("name"), "id": a.get("id"), "options": []})
        elif tag == "option" and self.selects:
            self.selects[-1]["options"].append(a.get("value"))
        elif tag == "input":
            self.inputs.append({k: a.get(k) for k in ("name", "type", "value")})

    def handle_data(self, data):
        if self.open:
            self.links[-1][1] += data

    def handle_endtag(self, tag):
        if tag == "a":
            self.open = False


def parse(html: str, url: str) -> Page:
    p = Page()
    p.feed(html)
    p.links = [[urljoin(url, h.strip()), " ".join(t.split())] for h, t in p.links]
    return p


def files(p: Page) -> list[tuple[str, str]]:
    out = []
    for h, t in p.links:
        if re.search(r"\.(pdf|zip|xlsx?|csv)(\?|$)", h, re.IGNORECASE) and (h, t) not in out:
            out.append((h, t))
    return out


def pdf_dump(url: str, body: bytes, full: bool, b64: bool) -> dict:
    from pypdf import PdfReader

    info: dict = {"url": url[-70:], "bytes": len(body), "sha": hashlib.sha256(body).hexdigest()[:12]}
    if not body.startswith(b"%PDF-"):
        info["not_pdf"] = body[:80].decode("latin-1")
        return info
    reader = PdfReader(io.BytesIO(body))
    info["pages"] = len(reader.pages)
    matrix_pages = []
    print(f"===== PDF {url} pages={len(reader.pages)} bytes={len(body)}", flush=True)
    for n, page in enumerate(reader.pages, 1):
        try:
            text = page.extract_text(extraction_mode="layout")
        except Exception as exc:  # noqa: BLE001 - probe
            text = f"<layout failed {exc}>"
        hits = len(WORDS.findall(text))
        voiv = sum(1 for v in VOIV if v in text.lower())
        if full:
            print(f"----- page {n} words={hits} voiv={voiv}", flush=True)
            print(text, flush=True)
        else:
            first = " | ".join(line.strip() for line in text.splitlines() if line.strip())[:300]
            print(f"----- page {n} words={hits} voiv={voiv} :: {first}", flush=True)
        if voiv >= 8 or hits >= 20:
            matrix_pages.append(n)
    info["matrix_like_pages"] = matrix_pages[:40]
    # operators of the first two matrix-like pages: are categories text, or only coloured cells?
    for n in matrix_pages[:2]:
        from pypdf.generic import ContentStream

        page = reader.pages[n - 1]
        ops = ContentStream(page.get_contents(), reader).operations
        counts: dict[str, int] = {}
        colours: dict[str, int] = {}
        for operands, op in ops:
            k = op.decode("latin-1")
            counts[k] = counts.get(k, 0) + 1
            if k in ("rg", "k", "g", "sc", "scn", "cs"):
                key = k + " " + " ".join(str(round(float(x), 3)) if hasattr(x, "__float__") else str(x)
                                         for x in operands)
                colours[key] = colours.get(key, 0) + 1
        print(f"OPS page {n}: {json.dumps(counts)}", flush=True)
        print(f"FILLS page {n}: {json.dumps(dict(sorted(colours.items(), key=lambda kv: -kv[1])[:30]))}", flush=True)
        seq = []
        for operands, op in ops[:1500]:
            k = op.decode("latin-1")
            if k in ("rg", "k", "g", "sc", "scn", "re", "f", "Tj", "TJ", "Tf", "Td", "Tm", "BT", "ET"):
                seq.append(f"{k}:{str(operands)[:60]}")
        print(f"SEQ page {n}: " + " ; ".join(seq), flush=True)
    if b64:
        enc = base64.b64encode(body).decode()
        print(f"BASE64-BEGIN {url} {len(enc)}", flush=True)
        for i in range(0, len(enc), 4000):
            print("B64 " + enc[i:i + 4000])
        print(f"BASE64-END {url}", flush=True)
    return info


def main() -> int:
    summary: dict = {"years": {}}
    form_seen = None
    national: dict[int, list[tuple[str, str]]] = {}
    for year in range(2016, 2028):
        r = get(PAGE.format(pub="national", year=year))
        if r is None:
            summary["years"][year] = "no answer"
            continue
        p = parse(r.text, r.url)
        national[year] = files(p)
        summary["years"][year] = {"status": r.status_code, "files": [h.rsplit("/", 3)[-3:] for h, _ in national[year]]}
        print(f"YEAR {year} FILES {json.dumps(national[year], ensure_ascii=False)}", flush=True)
        if form_seen is None and p.selects:
            form_seen = {"forms": p.forms, "selects": p.selects, "inputs": p.inputs}
            print("FORM " + json.dumps(form_seen, ensure_ascii=False), flush=True)
            others = [h for h, _ in p.links if "publikacje" in h]
            print("PUBLICATION LINKS " + json.dumps(sorted(set(others)), ensure_ascii=False), flush=True)
    annotate("notice", "barometr probe pages", summary)

    # voivodeship pages: the register names publication=<voivodeship>; try the form's own values
    vo: dict = {}
    for pub in ("malopolskie", "małopolskie", "regional", "voivodeship"):
        r = get(PAGE.format(pub=pub, year=2026))
        if r is not None:
            fl = files(parse(r.text, r.url))
            vo[pub] = [h.rsplit("/", 3)[-3:] for h, _ in fl]
            print(f"VOIV {pub} {json.dumps(fl, ensure_ascii=False)}", flush=True)
    if form_seen:
        for s in form_seen["selects"]:
            print(f"SELECT {s['name']} {s['id']} {s['options'][:40]}", flush=True)
    annotate("notice", "barometr probe voivodeship pages", {"tried": vo, "form": form_seen and
                                                             [(s["name"], s["options"][:20]) for s in form_seen["selects"]]})

    pdfs: list[dict] = []
    for year in sorted(national):
        for href, text in national[year]:
            if not href.lower().endswith(".pdf"):
                continue
            r = get(href)
            if r is None or r.status_code != 200:
                pdfs.append({"url": href[-70:], "status": r and r.status_code})
                continue
            report = "report" in href or "raport" in href.lower()
            full = True  # every national report and poster in full, text layer only
            b64 = year in (2016, 2026) and report and len(r.content) <= 6_000_000
            b64 = b64 or (year == 2026 and not report and len(r.content) <= 3_000_000)
            pdfs.append(pdf_dump(href, r.content, full, b64))
    for i in range(0, len(pdfs), 6):
        annotate("notice", f"barometr probe pdfs {i // 6 + 1}", pdfs[i:i + 6])
    annotate("notice", "barometr probe end", {"requests": requests_made})
    return 0


if __name__ == "__main__":
    sys.exit(main())
