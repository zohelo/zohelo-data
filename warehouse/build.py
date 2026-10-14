"""silver_gold.build: dbt build of the warehouse SQL project against the R2 Data Catalog.

The run filter becomes dbt's --select (e.g. "silver.nbp" or "nbp_fx_rates+"); empty
builds everything. When the Eurostat Silver model is selected, warehouse/eurostat_batch.py
plans its batch first (passed as dbt vars) and records it after the model ran. A run that
selects only the Eurostat Silver models repeats batches while Silver is incomplete and the
time budget allows (LOOP_MINUTES). Target prod writes to the silver/gold namespaces;
results (models, tests, failures) are published per build as an annotation (GitHub keeps
the first 10 per step) and in the step summary (all builds).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time

from ingestion.common.report import annotate, step_summary
from warehouse import eurostat_batch

PROJECT = Path(__file__).resolve().parent
SETTINGS = eurostat_batch.SETTINGS  # warehouse/warehouse.yaml "eurostat_silver"
ROW_BUDGET = int(SETTINGS["row_budget"])
LOOP_MINUTES = float(SETTINGS["loop_minutes"])
LOOP_BUILDS = int(SETTINGS["loop_builds"])
EUROSTAT_ONLY = {"eurostat_dataset_versions", "eurostat_observations"}


def main(select: str = "", target: str = "prod", variables: dict | None = None) -> int:
    try:
        return _main(select, target, variables)
    except Exception:
        import traceback

        annotate("error", "warehouse build crashed", {"traceback": traceback.format_exc()[-3000:]})
        raise


def _main(select: str, target: str, variables: dict | None) -> int:
    selectors = [s.strip() for s in select.replace(",", " ").split() if s.strip()]
    if not selectors or not set(selectors) <= EUROSTAT_ONLY:
        return build_once(selectors, target, variables)[0]
    started, longest = time.monotonic(), 0.0
    for _ in range(LOOP_BUILDS):
        began = time.monotonic()
        code, remaining = build_once(selectors, target, variables)
        longest = max(longest, time.monotonic() - began)
        elapsed = time.monotonic() - started
        if code != 0 or not remaining or elapsed + 1.3 * longest > LOOP_MINUTES * 60:
            return code
    return code


def build_once(selectors: list[str], target: str, variables: dict | None) -> tuple[int, int]:
    """One dbt build; returns (exit code, Eurostat versions still to load after it)."""
    command = ["dbt", "build", "--target", target, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
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
    summary = {
        "target": target,
        "select": selectors,
        "exit_code": completed.returncode,
        "by_status": {},
        "failures": [],
    }
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
    return completed.returncode, (plan["summary"]["remaining_after_batch"] if plan else 0)
