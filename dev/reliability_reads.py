"""Read-only checks for #3196 on this branch's code (reliability lane, 25 Nov). Writes nothing to R2.

1. The branch's entsog_gas_days and ms_krs_entities_complete compiled and run as queries on production (dbt show).
2. Every held ms_krs extract without a name asked again (1 s apart) and reduced with this branch's reduce():
   counts only (register, name present, legal form, seat country), no names in the output.
"""

from __future__ import annotations

from collections import Counter
import json
import os
import subprocess
import time
import traceback

from ingestion.common.report import annotate
from warehouse.build import PROJECT, TOPICS


def _dbt(args: list[str]) -> subprocess.CompletedProcess:
    base = ["dbt", "--quiet", args[0], "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    return subprocess.run(
        [*base, "--target", "prod", *args[1:], "--vars", json.dumps({"topics": TOPICS})],
        cwd=PROJECT,
        env={**os.environ},
        text=True,
        capture_output=True,
    )


def test_rows(name: str) -> None:
    done = _dbt(["compile", "--select", name])
    files = list((PROJECT / "target" / "compiled").rglob(f"{name}.sql"))
    if done.returncode or not files:
        annotate("error", f"1 {name}: compile failed", (done.stdout + done.stderr)[-1500:])
        return
    shown = _dbt(["show", "--inline", files[0].read_text(), "--limit", "20", "--output", "json"])
    out = shown.stdout.strip()
    try:
        rows = json.loads(out[out.index("{") :])["show"]
    except (ValueError, KeyError):
        annotate("error", f"1 {name}: show failed", (out + shown.stderr)[-1500:])
        return
    annotate("warning" if rows else "notice", f"1 {name} rows on production (branch SQL)", {"rows": len(rows), "first": rows[:5]})


def krs() -> None:
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.ms_krs import extract as x

    parts = x.held_parts(s3(), req("STORAGE_LANDING_BUCKET_NAME"), "01_landing")
    held = {k: r for p in parts.values() for k, r in p["records"].items()}
    unnamed = sorted(k for k, r in held.items() if r.get("http_status") == 200 and r.get("name") is None)
    api = x.Api()
    outcome, fields = Counter(), Counter()
    still = []
    legal, country = Counter(), Counter()
    for krs in unnamed:
        record, how = api.extract(krs, "2026-11-25")
        outcome[how] += 1
        if how == "none":
            continue
        for key in ("register", "name", "legal_form", "nip", "regon", "seat_country", "seat_voivodeship", "seat_town"):
            fields[key] += record.get(key) is not None
        legal[str(record.get("legal_form"))] += 1
        country[str(record.get("seat_country"))] += 1
        if record.get("name") is None or record.get("register") is None:
            still.append(krs)
    annotate(
        "warning" if still else "notice",
        "2 ms_krs extracts without a name, asked again with the branch's reduce()",
        {
            "held": len(held),
            "unnamed": len(unnamed),
            "answers": dict(outcome),
            "fields_present": dict(fields),
            "legal_form": dict(legal.most_common(5)),
            "seat_country": dict(country.most_common(5)),
            "still_without_name_or_register": still[:20],
            "requests": api.requests,
        },
    )


def main() -> int:
    failed = 0
    for step in (lambda: test_rows("entsog_gas_days"), lambda: test_rows("ms_krs_entities_complete"), krs):
        try:
            step()
        except Exception:
            failed += 1
            annotate("error", "step crashed", traceback.format_exc()[-1500:])
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
