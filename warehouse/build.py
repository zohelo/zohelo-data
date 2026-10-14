"""silver_gold.build: dbt build of the warehouse SQL project against the R2 Data Catalog.

The run filter becomes dbt's --select (e.g. "silver.nbp" or "nbp_fx_rates+"); empty
builds everything. When the Eurostat Silver model is selected, warehouse/eurostat_batch.py
plans its batch first (passed as dbt vars) and records it after the model ran. Target prod writes to the silver/gold namespaces; results (models,
tests, failures) are published as an annotation.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

from ingestion.common.report import annotate, step_summary
from warehouse import eurostat_batch

PROJECT = Path(__file__).resolve().parent
ROW_BUDGET = 1_500_000_000   # Bronze rows per Eurostat Silver batch (395M took 7 min on a runner)


def main(select: str = "", target: str = "prod", variables: dict | None = None) -> int:
    command = ["dbt", "build", "--target", target, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    selectors = [s.strip() for s in select.replace(",", " ").split() if s.strip()]
    if selectors:
        command += ["--select", *selectors]
    variables = dict(variables or {})
    plan = None
    if eurostat_batch.wanted(selectors):
        namespace = "zz_dbt_trial" if target == "trial" else "silver"
        plan = eurostat_batch.prepare(namespace, int(variables.pop("eurostat_row_budget", ROW_BUDGET)))
        variables.update(plan["vars"])
    if variables:
        command += ["--vars", json.dumps(variables)]
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
    if plan is not None:
        summary["eurostat_batch"] = plan["summary"]
        if any(r["unique_id"].endswith("." + eurostat_batch.MODEL) and r["status"] == "success" for r in results):
            eurostat_batch.finish(namespace, plan)
            summary["eurostat_batch"]["recorded"] = True
    if not results:
        summary["output_tail"] = completed.stdout[-1500:]
    if completed.returncode == 0 and target == "prod":
        from warehouse import catalog
        summary["catalog"] = catalog.publish()
    annotate("notice" if completed.returncode == 0 else "error", "warehouse dbt build", summary)
    step_summary(f"## warehouse · dbt build\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return completed.returncode
