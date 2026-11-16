"""Read-only checks of the reliability lane (#2873), one Dev run. Writes nothing to R2.

1. The rows of `metrics_value_range` that failed the model build of run 43809388349 (ms_isws, 16 Nov, job
   133110001699, 1 row): the test is compiled with the prod target and its SELECT is run with `dbt show`
   (a query, no materialization).
2. Queue item 2d: dev/p2137_trial.py (production build_bdl P2137 joined with itself, and Landing receipts).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

from ingestion.common.report import annotate
from warehouse.build import PROJECT, TOPICS


def _dbt(args: list[str]) -> subprocess.CompletedProcess:
    base = [
        "dbt",
        "--quiet",
        *args[:1],
        "--project-dir",
        str(PROJECT),
        "--profiles-dir",
        str(PROJECT),
        "--target",
        "prod",
    ]
    return subprocess.run(
        [*base, *args[1:], "--vars", json.dumps({"topics": TOPICS})],
        cwd=PROJECT,
        env={**os.environ},
        text=True,
        capture_output=True,
    )


def value_range() -> None:
    done = _dbt(["compile", "--select", "metrics_value_range"])
    files = list((PROJECT / "target" / "compiled").rglob("metrics_value_range.sql"))
    if done.returncode or not files:
        annotate(
            "error",
            "1 metrics_value_range: compile failed",
            {"rc": done.returncode, "out": (done.stdout + done.stderr)[-1500:]},
        )
        return
    sql = files[0].read_text()
    shown = _dbt(["show", "--inline", sql, "--limit", "50", "--output", "json"])
    out = shown.stdout.strip()
    try:
        rows = json.loads(out[out.index("{") :])["show"]
    except (ValueError, KeyError):
        annotate(
            "error", "1 metrics_value_range: show failed", {"rc": shown.returncode, "out": (out + shown.stderr)[-1500:]}
        )
        return
    annotate(
        "warning" if rows else "notice",
        "1 metrics_value_range rows on production Gold",
        {"rows": len(rows), "first": rows[:10]},
    )
    keys = sorted({(r.get("concept_key"), r.get("source_system"), r.get("measure_key")) for r in rows})
    if keys:
        Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"), "value_range.json").write_text(
            json.dumps(rows, default=str)
        )
        annotate("notice", "1b metrics_value_range keys", [list(k) for k in keys[:30]])


def main() -> int:
    try:
        value_range()
    except Exception as exc:  # the second check still runs
        annotate("error", "1 metrics_value_range crashed", f"{type(exc).__name__}: {str(exc)[:1500]}")
    from dev import p2137_trial

    return p2137_trial.main()


if __name__ == "__main__":
    raise SystemExit(main())
