#!/usr/bin/env python3
"""Read-only probe (PLG-034, #491): the KPRM work-list page, its CSV file, the Prime Minister's list and the two
licence statements. No writes to R2. Polite: one request at a time, 3 s apart, a clear user agent, timeouts."""

from __future__ import annotations

from collections import Counter
import csv
import io
import json
import re
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns.bulk_file import USER_AGENT, links_matching

PAGES = {
    "wplip": "https://www.gov.pl/web/premier/wplip-rm",
    "wpl_prm": "https://www.gov.pl/web/premier/wpl-prm",
}
REUSE = "https://www.gov.pl/web/premier/ponowne-wykorzystywanie"


def get(url: str) -> requests.Response:
    time.sleep(3)
    r = requests.get(url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}, timeout=(30, 300))
    print(
        json.dumps(
            {
                "url": url,
                "status": r.status_code,
                "bytes": len(r.content),
                "headers": {
                    k: v
                    for k, v in r.headers.items()
                    if k.lower()
                    in (
                        "content-type",
                        "content-length",
                        "last-modified",
                        "etag",
                        "content-disposition",
                        "cache-control",
                    )
                },
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return r


def text_near(html: str, needle: str, width: int = 400) -> str:
    plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))
    i = plain.find(needle)
    return plain[max(0, i - 50) : i + width] if i >= 0 else ""


def profile(name: str, body: bytes) -> dict:
    text = body.decode("utf-8-sig")
    out: dict = {"bom": body[:3] == b"\xef\xbb\xbf", "crlf": b"\r\n" in body[:5000], "bytes": len(body)}
    rows = list(csv.reader(io.StringIO(text, newline=""), delimiter=";", quotechar='"', strict=True))
    header, data = rows[0], rows[1:]
    out["header"] = header
    out["rows"] = len(data)
    out["widths"] = dict(Counter(len(r) for r in data))
    print(json.dumps({name: out}, ensure_ascii=False), flush=True)
    cols = {h: [r[i] if i < len(r) else None for r in data] for i, h in enumerate(header)}
    prof = {}
    for i, h in enumerate(header):
        vals = cols[h]
        nonempty = [v for v in vals if v not in (None, "")]
        c = Counter(nonempty)
        p = {
            "i": i,
            "empty": len(vals) - len(nonempty),
            "distinct": len(c),
            "maxlen": max((len(v) for v in nonempty), default=0),
            "newline_rows": sum(1 for v in nonempty if "\n" in v),
            "semicolon_rows": sum(1 for v in nonempty if ";" in v),
        }
        if len(c) <= 60 and p["maxlen"] <= 200:
            p["values"] = c.most_common()
        else:
            p["sample"] = [v[:120] for v in list(dict.fromkeys(nonempty))[:6]]
        prof[h] = p
        print(json.dumps({name: {h: p}}, ensure_ascii=False), flush=True)
    return {"header": header, "data": data, "prof": prof, "summary": out}


def main(argv=None) -> int:
    results = {}
    for key, page in PAGES.items():
        r = get(page)
        links = links_matching(r.text, r.url, r"/register-file/")
        csv_links = links_matching(r.text, r.url, r"\.csv")
        anchor = re.findall(r'<a[^>]+href="([^"]*register-file[^"]*)"[^>]*>(.*?)</a>', r.text, flags=re.S)
        results[key] = {
            "status": r.status_code,
            "register_links": links,
            "csv_links": csv_links,
            "anchors": [(h, re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", t)).strip()) for h, t in anchor][:5],
            "footer": text_near(r.text, "Treści tekstowe", 300),
        }
        annotate("notice", f"probe page {key}", results[key])
    files = {}
    for key in PAGES:
        for url in results[key]["register_links"][:1]:
            r = get(url)
            files[key] = profile(key, r.content)
            annotate("notice", f"probe file {key}", {"url": url, **files[key]["summary"]})
    main_ = files.get("wplip")
    if main_:
        h, d = main_["header"], main_["data"]
        ix = {name: i for i, name in enumerate(h)}
        num = [r[0] for r in d]
        dup = [k for k, n in Counter(num).items() if n > 1]
        prefixes = Counter(re.sub(r"[0-9].*$", "", n) for n in num)
        # publication date vs number (entry date or last change?)
        dcol = [c for c in h if "publikacji" in c.lower()]
        scol = [c for c in h if "status" in c.lower()]
        info = {"duplicates": dup[:10], "prefixes": prefixes.most_common(), "date_col": dcol, "status_col": scol}
        if dcol and scol:
            di, si = ix[dcol[0]], ix[scol[0]]
            by_status: dict = {}
            for r in d:
                by_status.setdefault(r[si], []).append(r[di])
            info["date_range_by_status"] = {k: [min(v), max(v), len(v)] for k, v in by_status.items()}
            months = Counter(r[di][:7] for r in d)
            info["rows_per_month"] = sorted(months.items())
            # within each prefix, is the date non-decreasing in the number?
            mono = {}
            for p in prefixes:
                rows = sorted(
                    (
                        (int(re.sub(r"^[A-Z]+", "", r[0]) or 0) if re.fullmatch(r"[A-Z]+[0-9]+", r[0]) else -1, r[di])
                        for r in d
                        if re.sub(r"[0-9].*$", "", r[0]) == p
                    )
                )
                inv = sum(1 for a, b in zip(rows, rows[1:]) if b[1] < a[1])
                mono[p] = {"n": len(rows), "inversions": inv, "first": rows[:2], "last": rows[-2:]}
            info["date_vs_number"] = mono
            info["date_formats"] = Counter(re.sub(r"[0-9]", "9", r[di]) for r in d).most_common(5)
            bad_num = [r[0] for r in d if not re.fullmatch(r"[A-Z]+[0-9]+[A-Z0-9/.-]*", r[0])]
            info["number_not_letters_digits"] = bad_num[:20]
        print(json.dumps({"analysis": info}, ensure_ascii=False), flush=True)
        annotate("notice", "probe analysis", {k: v for k, v in info.items() if k != "rows_per_month"})
        annotate("notice", "probe rows per month", info.get("rows_per_month"))
        # spot rows: first, last, two realised, two withdrawn
        if scol:
            si = ix[scol[0]]
            keep = (
                [0, len(d) - 1]
                + [i for i, r in enumerate(d) if r[si] == "Zrealizowany"][:2]
                + [i for i, r in enumerate(d) if r[si] == "Wycofany"][:2]
            )
            spots = [
                {
                    h[j]: (d[i][j][:100] if j < len(d[i]) else None)
                    for j in range(len(h))
                    if "osoba" not in h[j].lower() and len(d[i][j]) < 400
                }
                for i in keep
            ]
            print(json.dumps({"spots": spots}, ensure_ascii=False), flush=True)
    if "wpl_prm" in files and main_:
        annotate(
            "notice",
            "probe prm vs main",
            {
                "same_header": files["wpl_prm"]["header"] == main_["header"],
                "prm_rows": files["wpl_prm"]["summary"]["rows"],
            },
        )
    r = get(REUSE)
    annotate(
        "notice",
        "probe reuse page",
        {
            "status": r.status_code,
            "text": text_near(r.text, "Informacje sektora", 450),
            "footer": text_near(r.text, "Treści tekstowe", 300),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
