"""Dev trial of the transport family (#2966 CEPiK, #2999 GDDKiA GPR) on the PR's final code: runs dev/cepik_trial.py
and dev/gddkia_gpr_trial.py one after the other (each writes only to its own zz_dev_<source> scratch and removes it).
A short reachability check of www.gov.pl first. The activities' own annotations go to the log only; the trials'
results are merged into at most eight compact annotations (each under 4 KB). Deleted before merge."""

from __future__ import annotations

import json
import sys
import time
import traceback

import requests

from ingestion.common import bulk_bronze, report
from ingestion.to_landing.patterns import bulk_file

ORIGINAL = report.annotate
COLLECTED: list[tuple[str, str, object]] = []


def collect(level, title, payload=None, *args, **kwargs):
    print(f"ANNOTATION {level} | {title} | {json.dumps(payload, default=str, ensure_ascii=False)[:6000]}", flush=True)
    COLLECTED.append((level, title, payload))


def emit() -> None:
    order = {"notice": 0, "warning": 1, "error": 2}
    groups: dict[str, list] = {}
    for level, title, payload in COLLECTED:
        if " trial" not in title and "crashed" not in title:
            continue
        groups.setdefault(title.split(" ")[0], []).append((level, title, payload))
    for source, items in groups.items():
        chunk, level = {}, "notice"
        for lv, title, payload in items:
            chunk[title] = payload
            level = max(level, lv, key=lambda x: order.get(x, 0))
            if len(json.dumps(chunk, default=str, ensure_ascii=False)) > 3000:
                ORIGINAL(level, f"{source} trial", chunk)
                chunk, level = {}, "notice"
        if chunk:
            ORIGINAL(level, f"{source} trial", chunk)


def main(argv) -> int:
    started = time.monotonic()
    try:
        r = requests.get("https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025",
                         headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 40))
        reach = {"status": r.status_code, "bytes": len(r.content)}
    except requests.RequestException as exc:
        reach = {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    reach["seconds"] = round(time.monotonic() - started, 1)
    ORIGINAL("notice" if reach.get("status") == 200 else "error", "transport trial reach www.gov.pl", reach)
    if reach.get("status") != 200:
        return 1
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = collect
    rc = 0
    try:
        for name in ("cepik_trial", "gddkia_gpr_trial"):
            time.sleep(5)
            try:
                module = __import__(f"dev.{name}", fromlist=["main"])
                module.annotate = collect
                code = module.main([])
            except Exception:
                traceback.print_exc()
                collect("error", f"{name.split('_trial')[0]} trial crashed", {"traceback": traceback.format_exc()[-600:]})
                code = 1
            print(f"{name} exit {code}", flush=True)
            rc = rc or code
    finally:
        report.annotate = bulk_bronze.annotate = bulk_file.annotate = ORIGINAL
        emit()
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
