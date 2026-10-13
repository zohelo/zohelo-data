"""silver_gold.build: dbt build of the warehouse SQL project against the R2 Data Catalog.

The run filter becomes dbt's --select (e.g. "silver.nbp" or "nbp_fx_rates+"); empty
builds everything. Target prod writes to the silver/gold namespaces; results (models,
tests, failures) are published as an annotation.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

from ingestion.common.report import annotate, step_summary

PROJECT = Path(__file__).resolve().parent


def main(select: str = "", target: str = "prod") -> int:
    command = ["dbt", "build", "--target", target, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    selectors = [s.strip() for s in select.replace(",", " ").split() if s.strip()]
    if selectors:
        command += ["--select", *selectors]
    completed = subprocess.run(command, cwd=PROJECT, env={**os.environ}, text=True, capture_output=True)
    print(completed.stdout[-20000:], completed.stderr[-5000:], sep="\n", flush=True)
    results_path = PROJECT / "target" / "run_results.json"
    results = []
    if results_path.exists():
        results = json.loads(results_path.read_text()).get("results", [])
    summary = {"target": target, "select": selectors, "exit_code": completed.returncode,
               "by_status": {}, "failures": []}
    for r in results:
        summary["by_status"][r["status"]] = summary["by_status"].get(r["status"], 0) + 1
        if r["status"] in ("error", "fail", "runtime error"):
            summary["failures"].append({"node": r["unique_id"], "message": str(r.get("message"))[:300]})
    if not results:
        summary["output_tail"] = completed.stdout[-1500:]
    annotate("notice" if completed.returncode == 0 else "error", "warehouse dbt build", summary)
    step_summary(f"## warehouse · dbt build\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return completed.returncode
