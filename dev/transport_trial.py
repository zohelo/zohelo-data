"""Dev trial of the transport family (#2966 CEPiK, #2999 GDDKiA GPR) on the PR's final code: runs dev/cepik_trial.py
and dev/gddkia_gpr_trial.py one after the other (each writes only to its own zz_dev_<source> scratch and removes it).
A short reachability check of www.gov.pl first. Deleted before merge."""

from __future__ import annotations

import sys
import time
import traceback

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file


def main(argv) -> int:
    started = time.monotonic()
    try:
        r = requests.get("https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025",
                         headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 40))
        reach = {"status": r.status_code, "bytes": len(r.content)}
    except requests.RequestException as exc:
        reach = {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    reach["seconds"] = round(time.monotonic() - started, 1)
    annotate("notice" if reach.get("status") == 200 else "error", "transport trial reach www.gov.pl", reach)
    if reach.get("status") != 200:
        return 1
    rc = 0
    for name in ("cepik_trial", "gddkia_gpr_trial"):
        time.sleep(5)
        try:
            module = __import__(f"dev.{name}", fromlist=["main"])
            code = module.main([])
        except Exception:
            traceback.print_exc()
            annotate("error", f"{name} crashed", {"traceback": traceback.format_exc()[-600:]})
            code = 1
        print(f"{name} exit {code}", flush=True)
        rc = rc or code
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
