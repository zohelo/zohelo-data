"""Dev probe (read-only, no writes): MF and KRUS gov.pl pages, workbook structure, dane.gov.pl alternative.

python -m dev.probe_mf [rows]
Prints to the job log only.
"""

from __future__ import annotations

from collections import defaultdict
import re
import sys

import requests

from ingestion.common import xlsx_cells
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 60
PAGES = [
    "https://www.gov.pl/web/finanse/szeregiczasowe",
    "https://www.gov.pl/web/finanse/sprawozdania-miesieczne-2026",
    "https://www.gov.pl/web/finanse/sprawozdania-miesieczne-2025",
    "https://www.gov.pl/web/krus/podstawowe-dane-statystyczne-z-zakresu-swiadczeniobiorcow",
]
LINK = re.compile(r'<a[^>]+href="([^"]*attachment/[0-9a-f-]{36})"[^>]*>(.*?)</a>', re.S | re.I)
H = {"User-Agent": USER_AGENT}


def dump(body: bytes, label: str) -> None:
    grid: dict[tuple[int, str], dict[int, dict[int, str]]] = defaultdict(lambda: defaultdict(dict))
    fmts: dict[str, int] = defaultdict(int)
    for c in xlsx_cells.cells(body):
        grid[(c["sheet_index"], c["sheet_name"])][c["row_number"]][c["column_number"]] = c["value_text"]
        if c["cell_type"] == "n":
            fmts[c["number_format"] or ""] += 1
    print(f"#### {label}: sheets={len(grid)} number formats={dict(fmts)}")
    for (i, name), rows in grid.items():
        maxr = max(rows)
        maxc = max(max(r) for r in rows.values())
        print(f"--- sheet {i} {name!r} rows<= {maxr} cols<= {maxc} cells={sum(len(r) for r in rows.values())}")
        shown = sorted(rows)[:ROWS] + sorted(rows)[-4:]
        for r in sorted(set(shown)):
            line = " | ".join(f"{c}={str(v)[:28]}" for c, v in sorted(rows[r].items())[:26])
            print(f"  r{r}: {line}")


def page(url: str) -> None:
    print(f"\n===== {url}")
    try:
        resp = requests.get(url, headers=H, timeout=40)
    except Exception as exc:  # noqa: BLE001
        print("FAILED", type(exc).__name__, exc)
        return
    print("status", resp.status_code, "bytes", len(resp.content))
    links = LINK.findall(resp.text)
    for href, text in links[:40]:
        print("  link", href, "::", re.sub(r"\s+", " ", re.sub("<[^>]+>", " ", text)).strip()[:160])
    picks = links[:3] if "sprawozdania" in url else links
    for href, _text in picks:
        full = href if href.startswith("http") else "https://www.gov.pl" + href
        r = requests.get(full, headers=H, timeout=90)
        print(f"  GET {full} -> {r.status_code} {len(r.content)} B type={r.headers.get('Content-Type')} "
              f"lm={r.headers.get('Last-Modified')} cd={r.headers.get('Content-Disposition')}")
        if r.content[:2] == b"PK":
            try:
                dump(r.content, full)
            except Exception as exc:  # noqa: BLE001
                print("  decode failed", exc)


def dane() -> None:
    print("\n===== dane.gov.pl")
    for q in ("zadłużenie Skarbu Państwa", "dług publiczny", "budżet państwa wykonanie", "KRUS świadczenia"):
        try:
            r = requests.get("https://api.dane.gov.pl/1.4/datasets", params={"q": q, "per_page": 8}, headers=H, timeout=40)
            print("q", q, r.status_code)
            for d in r.json().get("data", []):
                a = d.get("attributes", {})
                print("  ds", d.get("id"), a.get("title", "")[:110], "| inst:", (a.get("institution") or {}).get("title") if isinstance(a.get("institution"), dict) else "", "| upd", a.get("modified"))
        except Exception as exc:  # noqa: BLE001
            print("  failed", type(exc).__name__, exc)


if __name__ == "__main__":
    for p in PAGES:
        page(p)
    dane()
