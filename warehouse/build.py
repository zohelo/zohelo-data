"""silver_gold.build: dbt build of the warehouse SQL project against the R2 Data Catalog.

The run filter becomes dbt's --select (e.g. "silver.nbp" or "nbp_fx_rates+"). An empty
filter (the scheduled run) builds only the models whose inputs or code changed since their
last build (warehouse/changed.py, owner policy #822); "fqn:*" builds everything. When a
run names the Eurostat Silver models explicitly (one-off backfills only, owner policy #822;
a full build leaves them unchanged), warehouse/eurostat_batch.py
plans its batch first (passed as dbt vars) and records it after the model ran. A run that
selects only the Eurostat Silver models repeats batches while Silver is incomplete and the
time budget allows (LOOP_MINUTES). A build whose only failures are the catalog's rate
limit (HTTP 429) is resumed with `dbt retry` after declared waits, with fewer threads.
Target prod writes to the silver/gold namespaces; results (models, tests, failures) are
published per build as an annotation (GitHub keeps the first 10 per step) and in the step
summary (all builds).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time

import yaml

from ingestion.common.report import ANNOTATION_LIMIT, annotate, step_summary
from warehouse import eurostat_batch

PROJECT = Path(__file__).resolve().parent
SETTINGS = eurostat_batch.SETTINGS  # warehouse/warehouse.yaml "eurostat_silver"
ROW_BUDGET = int(SETTINGS["row_budget"])
LOOP_MINUTES = float(SETTINGS["loop_minutes"])
LOOP_BUILDS = int(SETTINGS["loop_builds"])
_RETRY = yaml.safe_load((PROJECT / "warehouse.yaml").read_text())
RATE_LIMIT_WAITS = [float(s) for s in _RETRY["rate_limit_retry_waits"]]
RATE_LIMIT_THREADS = int(_RETRY["rate_limit_retry_threads"])
EUROSTAT_ONLY = eurostat_batch.EUROSTAT_ONLY


def main(select: str = "", target: str = "prod", variables: dict | None = None) -> int:
    try:
        return _main(select, target, variables)
    except Exception:
        import traceback

        annotate("error", "warehouse build crashed", {"traceback": traceback.format_exc()[-3000:]})
        raise


def _main(select: str, target: str, variables: dict | None) -> int:
    selectors = [s.strip() for s in select.replace(",", " ").split() if s.strip()]
    if not selectors and target == "prod":
        return changed_only(target, variables)
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


def changed_only(target: str, variables: dict | None) -> int:
    """The scheduled build: only models whose inputs or code changed (warehouse/changed.py)."""
    from ingestion.common.r2 import catalog
    from warehouse import changed

    cat = catalog()
    meta = changed.manifest(target)
    plan = changed.plan(cat, meta)
    step_summary(f"## warehouse · changed models\n\n```json\n{json.dumps(plan, indent=2)}\n```\n")
    if not plan["select"]:
        from warehouse import catalog as data_catalog

        # Descriptions, tests and metrics can change without a rebuild; the parse's manifest is current.
        published = data_catalog.publish()
        summary = {"target": target, "changed_only": {"rebuilt": 0, **_why(plan)}, "catalog": published}
        annotate("notice", "warehouse dbt build", summary)
        return 0
    return build_once(plan["select"], target, variables, changed_plan={"cat": cat, "meta": meta, **plan})[0]


def _why(plan: dict) -> dict:
    """Counts of the rebuild reasons by kind, and of unchanged models (fits any annotation)."""
    kinds: dict[str, int] = {}
    for reason in plan["reasons"].values():
        kind = reason.split(":")[0]
        kinds[kind] = kinds.get(kind, 0) + 1
    return {"unchanged": plan["unchanged"], "why": kinds}


MAX_FAILURES_SHOWN = 8
ANNOTATION_FIT = ANNOTATION_LIMIT - 100  # room for the title


def _short(message: str, head: int = 200, tail: int = 600) -> str:
    """A failure message cut to fit an annotation, keeping its end: errors such as Iceberg
    commit failures put a long catalog URL first and the HTTP status and reason last."""
    return message if len(message) <= head + tail else f"{message[:head]} ... {message[-tail:]}"


def _for_annotation(summary: dict, shown: int = MAX_FAILURES_SHOWN) -> dict:
    """The summary fitted to one annotation: failures go last, at most `shown` of them (fewer
    if escaping makes them too long) with a count of the rest, their messages sharing 2,400
    characters, so the batch and catalog results are never cut off. The step summary keeps
    every failure in full."""
    failures = summary["failures"]
    out = {k: v for k, v in summary.items() if k != "failures"}
    budget = 2400 // max(1, min(len(failures), shown))
    listed = [
        {**f, "message": _short(f["message"], head=min(200, budget // 4), tail=max(1, min(600, budget * 3 // 4)))}
        for f in failures[:shown]
    ]
    # JSON escaping (quotes, newlines, non-ASCII) can still push it over: drop failures from
    # the end until the escaped text fits, counting them in failures_more.
    while True:
        if len(failures) > len(listed):
            out["failures_more"] = len(failures) - len(listed)
        out["failures"] = listed
        if not listed or len(json.dumps(out, default=str, separators=(",", ":"))) <= ANNOTATION_FIT:
            return out
        listed = listed[:-1]


RESULTS = PROJECT / "target" / "run_results.json"


def _dbt(command: list[str]) -> tuple[int, list[dict], str]:
    """Run one dbt command; returns its exit code, the results it wrote and its output tail.
    `dbt retry` reads the previous run_results.json, so only results newer than the start count."""
    started = time.time()
    completed = subprocess.run(command, cwd=PROJECT, env={**os.environ}, text=True, capture_output=True)
    print(completed.stdout[-20000:], completed.stderr[-5000:], sep="\n", flush=True)
    results = []
    if RESULTS.exists() and RESULTS.stat().st_mtime >= started:
        results = json.loads(RESULTS.read_text()).get("results", [])
    return completed.returncode, results, completed.stdout[-1500:]


FAILED = ("error", "fail", "runtime error")


def _rate_limited_only(results: list[dict]) -> bool:
    """True when the build failed and every failure is the catalog's HTTP 429 (rate limit)."""
    failures = [r for r in results if r["status"] in FAILED]
    return bool(failures) and all("HTTP 429" in str(r.get("message")) for r in failures)


def build_once(
    selectors: list[str], target: str, variables: dict | None, changed_plan: dict | None = None
) -> tuple[int, int]:
    """One dbt build; returns (exit code, Eurostat versions still to load after it). With a
    changed-only plan, a successful build records its build properties on the models it wrote."""
    started_ms = int(time.time() * 1000)
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
    returncode, results, tail = _dbt(command)
    # `dbt retry` re-runs the failed and skipped nodes of the previous invocation with its
    # arguments and writes results for those nodes only; they replace the earlier ones.
    latest = {r["unique_id"]: r for r in results}
    retries = []
    for wait in RATE_LIMIT_WAITS:
        if returncode == 0 or not _rate_limited_only(list(latest.values())):
            break
        time.sleep(wait)
        retry = ["dbt", "retry", "--target", target, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
        returncode, retried, tail = _dbt([*retry, "--threads", str(RATE_LIMIT_THREADS)])
        retries.append({"after_seconds": wait, "nodes": len(retried), "exit_code": returncode})
        if not retried:
            retries[-1]["output_tail"] = tail[-600:]  # why it wrote no results (job logs are unreadable)
            break
        latest.update({r["unique_id"]: r for r in retried})
    results = list(latest.values())
    summary = {
        "target": target,
        "select": selectors,
        "exit_code": returncode,
        "by_status": {},
        "failures": [],
    }
    if retries:
        summary["rate_limit_retries"] = retries
    for r in results:
        summary["by_status"][r["status"]] = summary["by_status"].get(r["status"], 0) + 1
        if r["status"] in FAILED:
            summary["failures"].append({"node": r["unique_id"], "message": str(r.get("message"))})
    if plan is not None:
        summary["eurostat_batch"] = plan["summary"]
        if any(r["unique_id"].endswith("." + eurostat_batch.MODEL) and r["status"] == "success" for r in results):
            eurostat_batch.finish(namespace, plan)
            summary["eurostat_batch"]["recorded"] = True
    if not results:
        summary["output_tail"] = tail
    if changed_plan is not None:
        from warehouse import changed

        summary["changed_only"] = {"rebuilt": len(selectors), **_why(changed_plan)}
        summary["changed_only"]["properties"] = changed.record(
            changed_plan["cat"], changed_plan["meta"], results, started_ms
        )
    if returncode == 0 and target == "prod":
        from warehouse import catalog

        summary["catalog"] = catalog.publish()
    annotate("notice" if returncode == 0 else "error", "warehouse dbt build", _for_annotation(summary))
    step_summary(f"## warehouse · dbt build\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return returncode, (plan["summary"]["remaining_after_batch"] if plan else 0)
