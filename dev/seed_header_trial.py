"""Dev trial for #2148: a seed whose header gains a column reloads without --full-refresh.

Seeds kpi_measure into the trial namespace (zz_dbt_trial) without its unit_key column, then
with it, reads the loaded unit keys back, and drops the trial table.
"""

import csv
import io
from pathlib import Path
import subprocess

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

PROJECT = Path("warehouse")
SEED = PROJECT / "seeds" / "kpi_measure.csv"
NAMESPACE = "zz_dbt_trial"


def dbt(*args: str) -> tuple[int, str]:
    command = ["dbt", *args, "--target", "trial", "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    done = subprocess.run(command, text=True, capture_output=True)
    print(done.stdout[-6000:], done.stderr[-2000:], sep="\n", flush=True)
    return done.returncode, done.stdout[-600:]


def main() -> int:
    full = SEED.read_text(encoding="utf-8")
    rows = list(csv.reader(io.StringIO(full)))
    out = io.StringIO()
    csv.writer(out, lineterminator="\n").writerows(r[:-1] for r in rows)
    result: dict = {}
    try:
        SEED.write_text(out.getvalue(), encoding="utf-8")
        result["old_header"] = dbt("seed", "--select", "kpi_measure")[0]
        SEED.write_text(full, encoding="utf-8")
        code, tail = dbt("seed", "--select", "kpi_measure")
        result["new_header"] = code
        if code:
            result["new_header_tail"] = tail
        code, tail = dbt(
            "show",
            "--inline",
            "select count(unit_key) as with_unit from {{ ref('kpi_measure') }}",
            "--output",
            "json",
        )
        result["show"] = tail[-300:]
    finally:
        SEED.write_text(full, encoding="utf-8")
        cat = catalog()
        try:
            cat.drop_table((NAMESPACE, "kpi_measure"))
            result["dropped"] = True
        except Exception as exc:  # noqa: BLE001 - report, the trial result matters
            result["dropped"] = repr(exc)[:200]
    annotate("notice", "seed header trial", result)
    return 0 if result.get("old_header") == 0 and result.get("new_header") == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
