"""Read-only probe for PLS-025 gus_exam_results (#449): the GUS eighth-grade CSV and the matura dataset.

GET only, one request at a time, 3 s apart, the platform User-Agent; nothing is written anywhere. Profiles go to the
job log; a few notices carry the summary. cke.gov.pl is never read (robots.txt).
"""

import csv
import io
import json
import re
import sys
import time
from collections import Counter

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

EXAM8 = "https://stat.gov.pl/download/gfx/portalinformacyjny/pl/defaultaktualnosci/5488/18/1/1/srednie_wyniki_uczniow_na_egzaminie_osmoklasisty.csv"
PAGES = [
    "https://stat.gov.pl/obszary-tematyczne/edukacja/edukacja/srednie-wyniki-uczniow-na-egzaminie-osmoklasisty,18,1.html",
    "https://api.dane.gov.pl/1.4/datasets/2621/resources",
    "https://api.dane.gov.pl/1.4/datasets/2585/resources",
]


def get(url):
    time.sleep(3)
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    print(f"=== GET {url} -> {r.status_code} {r.headers.get('content-type')} len={len(r.content)} "
          f"lm={r.headers.get('last-modified')} etag={r.headers.get('etag')}", flush=True)
    return r


def profile(name, body):
    out = {"bytes": len(body), "bom": body[:3] == b"\xef\xbb\xbf"}
    try:
        text = body.decode("utf-8-sig")
        out["encoding"] = "utf-8"
    except UnicodeDecodeError:
        text = body.decode("cp1250")
        out["encoding"] = "cp1250"
    out["crlf"] = "\r\n" in text[:5000]
    lines = text.splitlines()
    out["first_lines"] = lines[:4]
    out["last_line"] = lines[-1:]
    rows = list(csv.reader(io.StringIO(text), delimiter=";", quotechar='"'))
    header = rows[0]
    out["header"] = header
    out["rows"] = len(rows) - 1
    out["widths"] = dict(Counter(len(r) for r in rows[1:]))
    data = [dict(zip(header, r)) for r in rows[1:] if len(r) == len(header)]
    for col in header:
        vals = Counter(d[col] for d in data)
        print(f"--- {name} column {col}: {len(vals)} distinct", flush=True)
        if col.lower() not in ("wartosc",):
            for v, c in sorted(vals.items()):
                print(f"    {v!r}: {c}", flush=True)
    if "wartosc" in header:
        nonnum = Counter(d["wartosc"] for d in data if not re.fullmatch(r"-?[0-9]+([.,][0-9]+)?", d["wartosc"]))
        out["wartosc_non_numeric"] = dict(nonnum.most_common(20))
        out["wartosc_decimal_comma"] = sum("," in d["wartosc"] for d in data)
        out["wartosc_zero"] = sum(d["wartosc"] in ("0", "0,0", "0.0", "0,00") for d in data)
        flag = [c for c in header if c.lower().startswith("flag")]
        if flag:
            out["flag_by_value_kind"] = {"|".join(k): v for k, v in Counter(
                (d[flag[0]], "empty" if d["wartosc"] == "" else ("num" if re.fullmatch(r"-?[0-9]+([.,][0-9]+)?", d["wartosc"]) else d["wartosc"]))
                for d in data).most_common(20)}
    keys = [c for c in header if c.lower() not in ("wartosc", "flaga")]
    out["dup_keys"] = sum(1 for _, c in Counter(tuple(d[k] for k in keys) for d in data).items() if c > 1)
    out["dup_sample"] = ["|".join(k) for k, c in Counter(tuple(d[k] for k in keys) for d in data).items() if c > 1][:5]
    print(f"=== {name} flagged rows", flush=True)
    for d in [d for d in data if d.get("flaga")][:60]:
        print("   ", {k: d[k] for k in ("wojewodztwo", "przedmiot", "plec", "rok", "wartosc", "flaga") if k in d}, flush=True)
    print(f"=== {name} sample rows", flush=True)
    for d in data[:40]:
        print("   ", d, flush=True)
    pl = [d for d in data if "2025" in d.get("rok", "") and (d.get("wojewodztwo", "") in ("", "-", "Polska") or "POLSKA" in d.get("wojewodztwo", "").upper())]
    print(f"=== {name} rows 2025 Poland-like: {len(pl)}", flush=True)
    for d in pl[:80]:
        print("   ", d, flush=True)
    print(f"=== {name} profile {json.dumps(out, ensure_ascii=False)}", flush=True)
    annotate("notice", f"probe {name}", {k: v for k, v in out.items() if k not in ("first_lines",)})
    return out


def main(argv):
    del argv
    found = {}
    r = get(EXAM8)
    annotate("notice", "probe exam8 http", {"status": r.status_code, "ct": r.headers.get("content-type"),
                                           "last_modified": r.headers.get("last-modified"), "etag": r.headers.get("etag"),
                                           "bytes": len(r.content)})
    if r.status_code == 200:
        profile("exam8", r.content)
    for url in PAGES:
        p = get(url)
        text = p.text
        print(text[:6000] if "api.dane" in url else "", flush=True)
        links = sorted(set(re.findall(r'https?://[^"\'\s<>]+?\.(?:csv|xlsx|zip)', text)) | set(
            "https://stat.gov.pl" + h for h in re.findall(r'href="(/[^"]+?\.(?:csv|xlsx|zip))"', text)))
        print(f"=== links in {url}:", *links, sep="\n   ", flush=True)
        found[url] = links
        if "stat.gov.pl/obszary" in url:
            for m in re.finditer(r'href="([^"]+)"[^>]*>([^<]*(?:matur|egzamin)[^<]*)<', text, re.I):
                print("   page link:", m.group(1), "|", m.group(2).strip(), flush=True)
    matura = [l for ls in found.values() for l in ls if "matur" in l.lower() and l.lower().endswith(".csv")]
    annotate("notice", "probe links", {u.split("/")[-2] if "api" in u else "page18": v[:12] for u, v in found.items()})
    for url in list(dict.fromkeys(matura))[:2]:
        m = get(url)
        if m.status_code == 200:
            profile("matura " + url.rsplit("/", 1)[-1][:40], m.content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
