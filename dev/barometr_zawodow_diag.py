"""Read-only diagnosis of Barometr Zawodów posters (#454). Writes nothing to R2.

Reads each declared poster by its publication page's link (one request at a time, 3 s apart), parses it with
posters.py and prints per poster: categories, rows per category, and every text line that lies in no named category
block (header, footer or a block that was not read). Argument: years (e.g. "2022" or "all"); the files named in
B64 are printed as base64 so that the parser can be checked offline.
"""

from __future__ import annotations

import base64
from collections import Counter
import io
import json
import logging
import sys
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_bronze.barometr_zawodow import posters
import ingestion.to_landing.barometr_zawodow as landing_mod
from ingestion.to_landing.patterns import bulk_file

B64 = {"plakat_p12_2022"}
PAUSE = 3.0


def outside(page, reader) -> dict:
    shapes, texts = posters.drawing(page, reader)
    texts, _ = posters.visible(shapes, texts)
    lines = posters.lines_of(page, reader, texts)
    width, height = float(page.mediabox.width), float(page.mediabox.height)
    wide = [s for s in shapes if s["x1"] - s["x0"] >= width / 2 and 12 <= s["y1"] - s["y0"] < 0.95 * height]
    blocks = [b for b in wide if not any(o is not b and posters._contains(b, o) for o in wide)]
    glyphs = [t for t in texts if t["rotated"]]
    named = posters._name_blocks(page, reader, blocks, shapes, glyphs, lines)
    top = max(b["y1"] for b, _ in named)
    bottom = min(b["y0"] for b, _ in named)
    out = []
    for line in lines:
        if not any(posters._inside(b, line["x"], line["y"]) for b, _ in named):
            out.append(f"{'above' if line['y'] >= top else 'below' if line['y'] <= bottom else 'band'}"
                       f"@{line['y']:.0f}:{line['text'][:50]}")
    return {
        "blocks": [[b["colour"], round(b["y0"]), round(b["y1"]), round(b["x0"]), round(b["x1"])] for b in blocks],
        "named": [[n, b["colour"], round(b["y0"]), round(b["y1"])] for b, n in named],
        "rotated": posters.decode(page, reader, [[g] for g in glyphs], rotated=True)[:12],
        "outside": out,
    }


def main(arg: str) -> int:
    from pypdf import PdfReader

    logging.getLogger("pypdf").setLevel(logging.ERROR)
    specs = [s for s in landing_mod.specs() if not s.get("optional")]
    if arg != "all":
        specs = [s for s in specs if str(s["year"]) in arg.split(",")]
    summary = []
    for spec in specs:
        time.sleep(PAUSE)
        try:
            links = bulk_file.page_links(spec)
        except Exception as exc:  # noqa: BLE001 - diagnosis
            summary.append([spec["entity"], f"page {exc}"])
            continue
        if len(links) != 1:
            summary.append([spec["entity"], f"links {links}"])
            continue
        time.sleep(PAUSE)
        r = requests.get(links[0], headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 120))
        body = r.content
        if spec["entity"] in B64:
            print(f"B64 {spec['entity']} {base64.b64encode(body).decode()}", flush=True)
        reader = PdfReader(io.BytesIO(body))
        for n, page in enumerate(reader.pages, 1):
            try:
                poster = posters.parse(page, reader)
                if poster is None:
                    continue
                info = outside(page, reader)
                cats = Counter(c for c, _ in poster["rows"])
                rec = {"entity": spec["entity"], "page": n, "area": poster["area"], "cats": dict(cats), **info}
            except Exception as exc:  # noqa: BLE001 - diagnosis
                rec = {"entity": spec["entity"], "page": n, "error": f"{type(exc).__name__}: {exc}"}
            print("DIAG " + json.dumps(rec, ensure_ascii=False), flush=True)
            short = {k: rec.get(k) for k in ("entity", "cats", "error")}
            short["outside_n"] = len(rec.get("outside", []))
            short["outside_band"] = [o for o in rec.get("outside", []) if not o.startswith(("above", "below"))]
            summary.append(short)
    odd = [s for s in summary if not isinstance(s, dict) or s.get("error") or len(s.get("cats") or {}) < 2]
    annotate("notice", "barometr_zawodow diag odd posters", {"n": len(summary), "odd": odd[:40]})
    print("SUMMARY " + json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "2022"))
