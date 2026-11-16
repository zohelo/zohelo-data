"""Read-only checks of the reliability lane (#2873, orchestrator order 16 Nov 09:58), one Dev run. Writes nothing to R2.

1. #3153: the rows of `metrics_value_range` on production Gold (the test compiled with the prod target from main
   after #3160, its SELECT run with `dbt show`), and for each failing concept x source x measure the count, min and
   max of all its metrics rows (to tell a stray row from a wrong bound).
2. ulc_airports: whether dane.gov.pl lists a ULC airport-statistics dataset (one API search, 20 s timeout).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

from ingestion.common.report import annotate
from warehouse.build import PROJECT, TOPICS


def _dbt(args: list[str]) -> subprocess.CompletedProcess:
    base = ["dbt", "--quiet", *args[:1], "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT), "--target", "prod"]
    return subprocess.run(
        [*base, *args[1:], "--vars", json.dumps({"topics": TOPICS})],
        cwd=PROJECT,
        env={**os.environ},
        text=True,
        capture_output=True,
    )


def _show(sql: str, label: str) -> list[dict] | None:
    shown = _dbt(["show", "--inline", sql, "--limit", "200", "--output", "json"])
    out = shown.stdout.strip()
    try:
        return json.loads(out[out.index("{") :])["show"]
    except (ValueError, KeyError):
        annotate("error", f"{label}: show failed", {"rc": shown.returncode, "out": (out + shown.stderr)[-1500:]})
        return None


def value_range() -> None:
    done = _dbt(["compile", "--select", "metrics_value_range"])
    files = list((PROJECT / "target" / "compiled").rglob("metrics_value_range.sql"))
    if done.returncode or not files:
        annotate("error", "1 compile failed", {"rc": done.returncode, "out": (done.stdout + done.stderr)[-1500:]})
        return
    sql = files[0].read_text()
    rows = _show(sql, "1 rows")
    if rows is None:
        return
    annotate("warning" if rows else "notice", "1 metrics_value_range rows on production Gold", {"rows": len(rows), "first": rows[:8]})
    out_dir = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "value_range.json").write_text(json.dumps(rows, default=str))
    keys = sorted({(r["concept_key"], r["source_system"], r["measure_key"]) for r in rows})
    if not keys:
        return
    head, _, _ = sql.rpartition("\nwhere m.value")
    if not head:
        annotate("error", "1b: compiled test has no final where", sql[-400:])
        return
    cond = " or ".join(
        "(concept_key = '{}' and source_system = '{}' and measure_key = '{}')".format(*(k.replace("'", "''") for k in key))
        for key in keys[:20]
    )
    agg = (
        "select concept_key, source_system, measure_key, count(*) as n, min(value) as lo, max(value) as hi,"
        " count(distinct geography_key) as areas, min(date_key) as first_date, max(date_key) as last_date,"
        f" max(value_min) as bound_min, max(value_max) as bound_max from ({head}) as t where {cond} group by 1, 2, 3"
    )
    stats = _show(agg, "1b stats")
    if stats is not None:
        annotate("notice", "1b failing keys: all their rows", stats[:20])


def dane_gov() -> None:
    import requests

    try:
        r = requests.get(
            "https://api.dane.gov.pl/1.4/search",
            params={"q": "lotnisk pasażer", "per_page": 20},
            headers={"Accept": "application/json"},
            timeout=20,
        )
        hits = [
            [d["attributes"].get("title", "")[:90], d.get("links", {}).get("self", "")]
            for d in r.json().get("data", [])
        ]
        annotate("notice", "2 dane.gov.pl search 'lotnisk pasażer'", {"status": r.status_code, "hits": hits[:12]})
    except Exception as exc:
        annotate("warning", "2 dane.gov.pl unreachable", f"{type(exc).__name__}: {str(exc)[:300]}")


def main() -> int:
    try:
        value_range()
    except Exception as exc:
        annotate("error", "1 crashed", f"{type(exc).__name__}: {str(exc)[:1500]}")
    dane_gov()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
