"""Dev trial for #2035: run the metrics_value_range test against prod Gold (reads only)."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from ingestion.common.report import annotate

PROJECT = Path("warehouse")


def main(argv: list[str]) -> int:
    command = [
        "dbt",
        "test",
        "--select",
        "metrics_value_range",
        "--target",
        "prod",
        "--project-dir",
        str(PROJECT),
        "--profiles-dir",
        str(PROJECT),
    ]
    completed = subprocess.run(command, text=True, capture_output=True)
    results_file = PROJECT / "target" / "run_results.json"
    results = json.loads(results_file.read_text())["results"] if results_file.exists() else []
    data = {
        "exit": completed.returncode,
        "results": [
            {"node": r["unique_id"], "status": r["status"], "failures": r.get("failures"), "message": r.get("message")}
            for r in results
        ],
    }
    if not results:
        data["tail"] = (completed.stdout + completed.stderr)[-2000:]
    status = results[0]["status"] if len(results) == 1 else None
    data["status"] = status
    data["failures"] = results[0].get("failures") if len(results) == 1 else None
    annotate("notice" if status == "pass" else "error", "metrics_value_range", data)
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
