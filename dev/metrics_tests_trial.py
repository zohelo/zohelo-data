"""Dev trial for #2219: run the metrics_grain and metrics_not_empty tests against prod Gold (read only)."""

import json
from pathlib import Path
import subprocess

from ingestion.common.report import annotate


def main() -> int:
    """Run the two dbt tests and annotate their status and failure counts."""
    command = ["dbt", "test", "--select", "metrics_grain", "metrics_not_empty", "--target", "prod",
               "--project-dir", "warehouse", "--profiles-dir", "warehouse"]
    done = subprocess.run(command, capture_output=True, text=True)
    results_path = Path("warehouse/target/run_results.json")
    if not results_path.exists():
        annotate("error", "metrics tests", {"returncode": done.returncode, "tail": (done.stdout + done.stderr)[-1500:]})
        return 1
    results = json.loads(results_path.read_text())["results"]
    annotate("notice", "metrics tests", {r["unique_id"]: [r["status"], r.get("failures")] for r in results})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
