"""Read-only probe, round 3: NID's INSPIRE WFS services linked from nid.pl/zasoby (register PLS-026, #2471-#2473).

Round 2 (run 43797381655): nid.pl publishes the register of immovable monuments per voivodeship only as PDF lists
(nid.pl/wp-content/uploads/2026/08/<WOJ>-rej_13.08.2026.pdf) and links dane.gov.pl (unreachable from Actions) and two
WFS services, usluga.zabytek.gov.pl/INSPIRE_WFS_MHD and INSPIRE_WFS_WHD. This round reads their capabilities
(feature types, constraints, paging, output formats), the schema, hit counts and three sample features per type
(element names; values only of short non-address fields). Also HEAD of one PDF list.

GET/HEAD only, one request at a time, 3 s apart, bounded retries. Writes nothing anywhere.
"""

from __future__ import annotations

import re
import time
from urllib.parse import urlparse

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
NOTES: list[str] = []
BUDGET = [60]
REDACT = re.compile(r"ulic|adres|address|numer|nr_?dom|lokal|wlasc|właśc|owner|osob|kod_?poczt|działk|dzialk|"
                    r"pos|coord|geom|Point|Polygon|Curve|Surface|lowerCorner|upperCorner", re.I)


def note(msg: str) -> None:
    NOTES.append(msg)
    print("NOTE " + msg, flush=True)


def get(url: str, *, head: bool = False, nbytes: int = 5_000_000, tries: int = 2):
    if BUDGET[0] <= 0:
        return None, "budget used"
    for attempt in range(tries):
        BUDGET[0] -= 1
        time.sleep(3)
        try:
            if head:
                r = requests.head(url, headers={"User-Agent": UA}, timeout=(30, 120), allow_redirects=True)
                body = b""
            else:
                with requests.get(url, headers={"User-Agent": UA}, timeout=(30, 300), stream=True) as r:
                    body = b""
                    for block in r.iter_content(1 << 16):
                        body += block
                        if len(body) >= nbytes:
                            break
            hdr = {k: v[:100] for k, v in r.headers.items() if k.lower() in (
                "content-type", "last-modified", "content-length")}
            st = f"{r.status_code} {hdr}"
            if r.status_code >= 500 or r.status_code == 429:
                raise RuntimeError(st + " " + body[:300].decode("utf-8", "replace"))
            return body, st
        except (requests.RequestException, RuntimeError) as exc:
            print(f"   try {attempt + 1}: {type(exc).__name__} {str(exc)[:300]}", flush=True)
            if attempt < tries - 1:
                time.sleep(20)
    return None, f"failed after {tries} tries"


def sample(xml: str) -> str:
    out = []
    for m in re.finditer(r"<([\w.-]+:)?([\w.-]+)(\s[^>]*)?>([^<]{0,120})</", xml):
        name, val = m.group(2), m.group(4).strip()
        if REDACT.search(name):
            val = f"<{len(val)} chars>"
        out.append(f"{name}={val[:60]}")
    return "; ".join(out)


def wfs(base: str) -> None:
    caps, st = get(base + "?service=WFS&request=GetCapabilities")
    note(f"{base} GetCapabilities -> {st} {len(caps or b'')} B")
    if not caps:
        return
    text = caps.decode("utf-8", "replace")
    print(text[:6000], flush=True)
    for tag in ("Title", "Abstract", "Fees", "AccessConstraints"):
        m = re.search(rf"<(?:ows:)?{tag}>(.*?)</(?:ows:)?{tag}>", text, re.S)
        if m:
            note(f"{tag}: {re.sub(r'\s+', ' ', m.group(1))[:600]}")
    versions = re.findall(r"version=\"([^\"]+)\"", text[:3000])
    paging = re.search(r'ImplementsResultPaging.*?<(?:ows:)?DefaultValue>(\w+)', text, re.S)
    counts = re.search(r'CountDefault.*?<(?:ows:)?DefaultValue>(\w+)', text, re.S)
    fmts = sorted(set(re.findall(r"<(?:ows:)?Value>([^<]*(?:json|gml|csv|xml|zip|shape)[^<]*)</", text, re.I)))
    note(f"versions={versions[:3]} paging={paging.group(1) if paging else '?'} "
         f"countdefault={counts.group(1) if counts else '?'} formats={fmts[:12]}")
    types = re.findall(r"<(?:wfs:)?FeatureType>.*?<(?:wfs:)?Name>([^<]+)</(?:wfs:)?Name>.*?<(?:wfs:)?Title>([^<]*)<",
                       text, re.S)
    note(f"feature types: {types[:20]}")
    for name, _ in types[:6]:
        d, st = get(f"{base}?service=WFS&version=2.0.0&request=DescribeFeatureType&typeNames={name}")
        if d:
            elems = re.findall(r'<(?:xsd?:)?element[^>]*name="([^"]+)"[^>]*type="([^"]+)"', d.decode("utf-8", "replace"))
            note(f"schema {name}: {elems[:60]}"[:1800])
        h, st = get(f"{base}?service=WFS&version=2.0.0&request=GetFeature&typeNames={name}&resultType=hits")
        if h:
            ht = h.decode("utf-8", "replace")
            m = re.search(r'numberMatched="([^"]+)"', ht) or re.search(r'numberOfFeatures="([^"]+)"', ht)
            note(f"hits {name}: {m.group(1) if m else ht[:300]}")
        s, st = get(f"{base}?service=WFS&version=2.0.0&request=GetFeature&typeNames={name}&count=3", nbytes=400_000)
        if s:
            st_ = s.decode("utf-8", "replace")
            note(f"sample {name} ({len(s)} B): {sample(st_)}"[:2500])
        s, st = get(f"{base}?service=WFS&version=2.0.0&request=GetFeature&typeNames={name}&count=2&startIndex=1000",
                    nbytes=200_000)
        note(f"paging {name} startIndex=1000 -> {st} {len(s or b'')} B")


def imd(base: str) -> None:
    """Round 4: the register service INSPIRE_IMD (INSPIRE metadata record f6947152-...: 'Zabytki nieruchome wpisane do
    rejestru zabytków', NID, 'No conditions apply to access and use', biannual): output formats and paging."""
    caps, _ = get(base + "?service=WFS&request=GetCapabilities")
    if not caps:
        return
    types = re.findall(r"<(?:wfs:)?FeatureType>.*?<(?:wfs:)?Name>([^<]+)</", caps.decode("utf-8", "replace"), re.S)
    for name in types[:3]:
        for extra in ("&outputFormat=text/csv&count=5", "&outputFormat=text/csv&count=5&startIndex=5",
                      "&outputFormat=application/vnd.geo%2Bjson&count=2"):
            b, st = get(f"{base}?service=WFS&version=2.0.0&request=GetFeature&typeNames={name}{extra}", nbytes=300_000)
            txt = (b or b"").decode("utf-8", "replace")
            if "csv" in extra:
                lines = txt.splitlines()
                note(f"{extra}: {st} header={lines[:1]!r} ids={[x.split(',')[0][:60] for x in lines[1:6]]}"[:1500])
            else:
                keys = sorted(set(re.findall(r'"([A-Za-z_]+)"\s*:', txt)))
                note(f"{extra}: {st} {len(b or b'')} B keys={keys[:40]}")
        b, st = get(f"{base}?service=WFS&version=2.0.0&request=GetFeature&typeNames={name}&outputFormat=text/csv"
                    f"&count=2000", nbytes=50_000_000)
        txt = (b or b"").decode("utf-8", "replace")
        note(f"csv count=2000: {st} {len(b or b'')} B {len(txt.splitlines())} lines")
        for woj in ("02", "14"):
            flt = ("<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0'><fes:PropertyIsLike wildCard='*' "
                   "singleChar='.' escapeChar='!'><fes:ValueReference>INSPIREID</fes:ValueReference>"
                   f"<fes:Literal>*NID_N_{woj}_*</fes:Literal></fes:PropertyIsLike></fes:Filter>")
            b, st = get(f"{base}?service=WFS&version=2.0.0&request=GetFeature&typeNames={name}&resultType=hits"
                        f"&FILTER={requests.utils.quote(flt)}")
            m = re.search(r'numberMatched="([^"]+)"', (b or b"").decode("utf-8", "replace"))
            note(f"hits woj {woj}: {m.group(1) if m else (b or b'')[:300]!r} {st}")


def main() -> None:
    try:
        base = "https://usluga.zabytek.gov.pl/INSPIRE_IMD/service.svc/get"
        wfs(base)
        imd(base)
    finally:
        print("requests left", BUDGET[0], flush=True)
        chunk, out = "", []
        for n in NOTES:
            if len(chunk) + len(n) > 3800:
                out.append(chunk)
                chunk = ""
            chunk += n.replace("\n", " ").replace("%", "%25") + " | "
        if chunk:
            out.append(chunk)
        for i, c in enumerate(out[:10]):
            print(f"::notice title=nid wfs {i + 1}/{len(out)}::{c}", flush=True)


if __name__ == "__main__":
    main()
