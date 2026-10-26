"""Gold PoC trial: dbt build of the poc models against real Silver, written to the scratch
namespace zz_dev_poc (target prod with the poc folder's schema set to the scratch namespace;
only the poc folder is ever selected here), then the build check and row counts as annotations.
Pass --drop to remove the scratch tables afterwards."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import time

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "zz_dev_poc"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--select", default="poc")
    parser.add_argument("--drop", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args(argv)
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "warehouse")}
    if not args.skip_build:
        started = time.monotonic()
        run = subprocess.run(
            [
                "dbt",
                "build",
                "--target",
                "prod",
                "--select",
                args.select,
                "--threads",
                "2",
                "--vars",
                f"{{poc_schema: {NAMESPACE}}}",
            ],
            cwd=ROOT / "warehouse",
            env=env,
            capture_output=True,
            text=True,
        )
        results = json.loads((ROOT / "warehouse" / "target" / "run_results.json").read_text())
        timing = sorted(
            (
                {"n": r["unique_id"].split(".")[-1][:40], "s": round(r["execution_time"], 1), "st": r["status"]}
                for r in results["results"]
                if r["unique_id"].startswith("model.")
            ),
            key=lambda r: -r["s"],
        )
        annotate(
            "notice" if run.returncode == 0 else "error",
            "poc trial build",
            {
                "exit": run.returncode,
                "minutes": round((time.monotonic() - started) / 60, 1),
                "models": sum(1 for r in results["results"] if r["unique_id"].startswith("model.")),
                "failed": [
                    r["unique_id"].split(".")[-1] for r in results["results"] if r["status"] not in ("success", "pass")
                ][:20],
                "slowest": timing[:12],
            },
        )
        errors = [ln for ln in run.stdout.splitlines() if "Error" in ln or "error" in ln.lower()][:25]
        if run.returncode != 0:
            annotate("error", "poc trial errors", {"lines": [e[:300] for e in errors]})
    cat = bf.retry(catalog, "catalog")
    counts = {}
    for ident in cat.list_tables(NAMESPACE):
        table = cat.load_table(ident)
        snap = table.current_snapshot()
        counts[ident[-1]] = int((snap.summary or {}).get("total-records", -1)) if snap else 0
    annotate("notice", "poc trial rows", counts)
    try:
        grain = cat.load_table((NAMESPACE, "grain_check")).scan().to_arrow().to_pylist()
        annotate("notice" if not grain else "warning", "poc grain check (duplicates)", grain[:8])
        check = cat.load_table((NAMESPACE, "build_check")).scan().to_arrow().to_pylist()
        rows = [{k: (v if not isinstance(v, list) else v[:6]) for k, v in r.items()} for r in check]
        for i in range(0, len(rows), 6):
            annotate("notice", f"poc build check {i // 6 + 1}", rows[i : i + 6])
        unmatched = (
            cat.load_table((NAMESPACE, "map_geography"))
            .scan(selected_fields=("source_system", "matched"))
            .to_arrow()
            .to_pylist()
        )
        by = {}
        for r in unmatched:
            k = r["source_system"]
            by.setdefault(k, [0, 0])
            by[k][0] += 1
            by[k][1] += 0 if r["matched"] else 1
        annotate("notice", "poc geography mapping (codes, unmatched)", by)
    except Exception as exc:  # the check table may be missing after a failed build
        annotate("warning", "poc check not read", {"error": f"{type(exc).__name__}: {str(exc)[:300]}"})
    if args.drop:
        dropped, failed = [], {}
        for ident in cat.list_tables(NAMESPACE):
            try:
                cat.drop_table(ident)
                dropped.append(ident[-1])
            except Exception as exc:  # report, keep going: a scratch table left behind is not a failure
                failed[ident[-1]] = f"{type(exc).__name__}: {str(exc)[:120]}"
        annotate(
            "notice" if not failed else "warning", "poc trial dropped", {"dropped": len(dropped), "failed": failed}
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
