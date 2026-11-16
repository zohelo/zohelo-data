"""Read-only probe, round 7: the Bronze decoder of nid_monuments on all 17 register lists (PLS-026, #2471-#2473).

Downloads every list the landing module finds on nid.pl (one at a time, 3 s apart), decodes it with
ingestion/to_bronze/nid_monuments/load.py in memory and prints aggregates for Silver: items per list, page headers
(powiat part) with counts, list dates, heading counts and samples, items with and without a register number, the
register-number pattern coverage, long items. Writes nothing anywhere.
"""

from __future__ import annotations

import re
import time
from collections import Counter

import requests

from ingestion.to_bronze.nid_monuments.load import decode
from ingestion.to_landing.nid_monuments import specs

NOTES: list[str] = []
NUM = re.compile(r"([A-Za-z]{0,4}[-/. ]?\d[\w/\-.]*)\s+z\s+(\d{1,2}\s?[./]\s?\d{1,2}\s?[./]\s?\d[\d ]{2,4})")


def note(msg: str) -> None:
    NOTES.append(msg)
    print("NOTE " + msg, flush=True)


def main() -> None:
    try:
        totals = Counter()
        for spec in specs():
            time.sleep(3)
            r = requests.get(spec["url"], headers={"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data)"},
                             timeout=(30, 300))
            r.raise_for_status()
            receipt = {"entity": spec["entity"], "sha256": "x", "version_token": "v", "ingested_at": "20260101T000000Z"}
            try:
                t = decode(r.content, receipt).to_pylist()
            except ValueError as exc:
                note(f"{spec['entity']}: REJECTED {exc}"[:600])
                continue
            heads = Counter(re.sub(r"^.*? – ", "", x["page_header"]) for x in t)
            states = Counter(x["state_text"] for x in t)
            texts = [x["item_text"] for x in t]
            has_nr = sum(1 for x in texts if "nr rej" in x)
            parsed = sum(1 for x in texts if NUM.search(x))
            jw = sum(1 for x in texts if re.search(r"nr rej\.?:?\s*jw", x))
            colon = sum(1 for x in texts if x.rstrip().endswith(":"))
            nr_unparsed = [x for x in texts if "nr rej" in x and not NUM.search(x) and not re.search(r"nr rej\.?:?\s*jw", x)]
            headings = Counter(x["heading_text"] for x in t)
            long = [x for x in t if x["line_count"] >= 4]
            totals.update({"items": len(t), "nr": has_nr, "parsed": parsed, "jw": jw})
            note(f"{spec['entity']}: {len(r.content)} B items={len(t)} pages={t[-1]['page_number']} nr_rej={has_nr} "
                 f"parsed={parsed} jw={jw} colon={colon} nr_unparsed={len(nr_unparsed)} headings={len(headings)} "
                 f"states={dict(states)} long={len(long)}")
            print(f"HEADERS {spec['entity']}: {dict(heads)}", flush=True)
            note(f"{spec['entity']} powiats({len(heads)}): " + "; ".join(sorted(heads))[:900])
            print(f"HEADINGS {spec['entity']}: {headings.most_common(60)}", flush=True)
            for x in nr_unparsed[:8]:
                print(f"UNPARSED {spec['entity']}: {x[-160:]}", flush=True)
            for x in long[:4]:
                print(f"LONG {spec['entity']}: {x['line_count']} {x['item_text'][:300]}", flush=True)
            odd = [h for h in headings if h and (re.search(r"\d", h) or len(h) > 60)]
            print(f"ODD HEADINGS {spec['entity']}: {odd[:20]}", flush=True)
        note(f"TOTAL {dict(totals)}")
    finally:
        chunk, out = "", []
        for n in NOTES:
            if len(chunk) + len(n) > 3800:
                out.append(chunk)
                chunk = ""
            chunk += n.replace("\n", " ").replace("%", "%25") + " | "
        if chunk:
            out.append(chunk)
        for i, c in enumerate(out[:10]):
            print(f"::notice title=nid decode {i + 1}/{len(out)}::{c}", flush=True)


if __name__ == "__main__":
    main()
