"""silver_gold.build: dbt build of the warehouse SQL project against the R2 Data Catalog.

The run filter becomes dbt's --select (e.g. "silver.nbp" or "nbp_fx_rates+"). An empty
filter (the scheduled run) builds only the models whose inputs or code changed since their
last build (warehouse/changed.py, owner policy #822), incremental models incrementally unless
their code changed; "fqn:*" builds everything (incremental models incrementally). Eurostat and
GUS BDL observations are not dbt models: their loaders write Silver per entity (design #875,
warehouse/entity/). A build whose failed models all failed on transient catalog errors (the rate limit, HTTP
429, or an Iceberg commit that does not yet see a table the build just created, #2574) is
resumed with `dbt retry` after declared waits, with fewer threads; failing data tests do not
prevent that (#2789).
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
from warehouse import declared

PROJECT = Path(__file__).resolve().parent
_RETRY = yaml.safe_load((PROJECT / "warehouse.yaml").read_text())
RATE_LIMIT_WAITS = [float(s) for s in _RETRY["rate_limit_retry_waits"]]
RATE_LIMIT_THREADS = int(_RETRY["rate_limit_retry_threads"])
COMMIT_RETRY_THREADS = int(_RETRY["commit_retry_threads"])
# Owner policy #822: only small tables are rebuilt in full; each full-rebuild model has a row ceiling.
FULL_REBUILD = declared.full_rebuild()  # warehouse.yaml and each source's own YAML
# Topics settings (docs/topics.md): weights, windows and thresholds, passed to every build as the dbt var `topics`.
TOPICS = _RETRY["topics"]


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
    return build_once(selectors, target, variables)[0]


def changed_only(target: str, variables: dict | None) -> int:
    """The scheduled build: only models whose inputs or code changed (warehouse/changed.py)."""
    from ingestion.common.r2 import catalog
    from warehouse import changed

    cat = catalog()
    meta = changed.manifest(target)
    plan = changed.plan(cat, meta)
    step_summary(f"## warehouse · changed models\n\n```json\n{json.dumps(plan, indent=2)}\n```\n")
    if plan["stalled"]:
        # A waiting model that has a table is not a new source: it would stay frozen, and a stalled Gold block
        # passed on as empty would take its rows out of the facts. Nothing is built or published.
        annotate("error", "warehouse dbt build: stalled models, nothing built", _stalled(plan["stalled"]))
        return 1
    if not plan["select"]:
        from warehouse import catalog as data_catalog

        # Descriptions, tests and metrics can change without a rebuild; the parse's manifest is current.
        published = data_catalog.publish()
        summary = {"target": target, "changed_only": {"rebuilt": 0, **_why(plan)}, "catalog": published}
        annotate("notice", "warehouse dbt build", summary)
        return 0
    variables = dict(variables or {})
    if plan["full_refresh"]:
        variables["full_refresh_models"] = plan["full_refresh"]
    if plan["empty_blocks"]:  # the only Gold blocks poc_observations() may find without a table
        variables["waiting_blocks"] = plan["empty_blocks"]
    if plan["waiting"]:  # the only optional inputs poc_optional_ref() may find without a table
        variables["waiting_models"] = plan["waiting"]
    return build_once(plan["select"], target, variables, changed_plan={"cat": cat, "meta": meta, **plan})[0]


STALLED_SHOWN = 20  # models named in the annotation; the step summary has the whole plan


def _stalled(stalled: dict[str, list[str]]) -> dict:
    """The stalled models for the error annotation: what happened, what to do, and each model with the source
    tables it waits for (the first STALLED_SHOWN, with a count of the rest)."""
    shown = dict(list(stalled.items())[:STALLED_SHOWN])
    out = {
        "error": (
            "models that have a table in production are waiting for source tables that do not exist; they would "
            "not be rebuilt, and a Gold block among them must never be read as empty. Nothing was built."
        ),
        "fix": (
            "load the missing source tables (the source's first Bronze run), or make the new input optional "
            "(poc_optional_ref and config meta optional_parents), or take it out of the model"
        ),
        "stalled": shown,
    }
    if len(stalled) > len(shown):
        out["stalled_more"] = len(stalled) - len(shown)
    return out


def _why(plan: dict) -> dict:
    """Counts of the rebuild reasons by kind, and of unchanged models (fits any annotation)."""
    kinds: dict[str, int] = {}
    for reason in plan["reasons"].values():
        kind = reason.split(":")[0]
        kinds[kind] = kinds.get(kind, 0) + 1
    return {
        "unchanged": plan["unchanged"],
        "why": kinds,
        "waiting": plan.get("waiting", []),
        "empty_blocks": plan.get("empty_blocks", []),  # waiting Gold blocks the facts read as zero rows
    }


MAX_FAILURES_SHOWN = 8
ANNOTATION_FIT = ANNOTATION_LIMIT - 100  # room for the title


def _short(message: str, head: int = 200, tail: int = 600) -> str:
    """A failure message cut to fit an annotation, keeping its end: errors such as Iceberg
    commit failures put a long catalog URL first and the HTTP status and reason last."""
    return message if len(message) <= head + tail else f"{message[:head]} ... {message[-tail:]}"


SELECT_SHOWN_CHARS = 400  # of the select list in the annotation; the rest is a count


def _select_for_annotation(selectors: list[str], limit: int = SELECT_SHOWN_CHARS) -> dict:
    """The select list for the annotation: whole when short, else its first names and `select_count`.

    A changed-only build after a shared macro or seed change selects 150 to 200 models, about 4,000 characters:
    left whole, the annotation was cut inside the list and lost exit_code, row_ceilings and every failure (#2875)."""
    if len(json.dumps(selectors, separators=(",", ":"))) <= limit:
        return {"select": selectors}
    first: list[str] = []
    for name in selectors:
        if len(json.dumps([*first, name], separators=(",", ":"))) > limit:
            break
        first.append(name)
    return {"select": first, "select_count": len(selectors), "select_more": len(selectors) - len(first)}


def _for_annotation(summary: dict, shown: int = MAX_FAILURES_SHOWN) -> dict:
    """The summary fitted to one annotation: a long select list is shortened to its first names and a
    count; failures go last, at most `shown` of them (fewer if escaping makes them too long) with a
    count of the rest, their messages sharing 2,400 characters, so the batch and catalog results are
    never cut off. The step summary keeps the select list and every failure in full."""
    failures = summary["failures"]
    out = {k: v for k, v in summary.items() if k != "failures"}
    out.update(_select_for_annotation(summary.get("select") or []))
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


RATE_LIMITED = "HTTP 429"
# An Iceberg commit right after a table's creation can miss it in the R2 Data Catalog ("Failed to
# commit Iceberg transaction: Table nbp_fx_rates__dbt_tmp does not exist", #2574); a rerun of the
# model drops the leftover __dbt_tmp/__dbt_backup and succeeds.
COMMIT_FAILED = "Failed to commit Iceberg transaction"


def _transient(message: str) -> bool:
    return RATE_LIMITED in message or COMMIT_FAILED in message


def _transient_only(results: list[dict]) -> bool:
    """True when some failure is transient (the catalog's HTTP 429 rate limit or an Iceberg
    commit failure) and every failed model, seed or snapshot failed transiently. A data test
    that finds rows (#2789) does not block the retry: `dbt retry` re-runs it, it fails again,
    and the build still fails on it, but the rate-limited models get built."""
    failures = [r for r in results if r["status"] in FAILED]
    blocking = [r for r in failures if not r["unique_id"].startswith("test.") and not _transient(str(r.get("message")))]
    return any(_transient(str(r.get("message"))) for r in failures) and not blocking


def _retry_threads(results: list[dict]) -> int:
    """Threads of a retry: one after a commit failure (no concurrent commits), else fewer than the build's."""
    commit = any(r["status"] in FAILED and COMMIT_FAILED in str(r.get("message")) for r in results)
    return COMMIT_RETRY_THREADS if commit else RATE_LIMIT_THREADS


def build_once(
    selectors: list[str], target: str, variables: dict | None, changed_plan: dict | None = None
) -> tuple[int, int]:
    """One dbt build; returns (exit code, 0). With a changed-only plan, a successful build
    records its build properties on the models it wrote."""
    started_ms = int(time.time() * 1000)
    command = ["dbt", "build", "--target", target, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    if selectors:
        command += ["--select", *selectors]
    variables = {"topics": TOPICS, **(variables or {})}
    command += ["--vars", json.dumps(variables)]
    returncode, results, tail = _dbt(command)
    # `dbt retry` re-runs the failed and skipped nodes of the previous invocation with its
    # arguments and writes results for those nodes only; they replace the earlier ones.
    latest = {r["unique_id"]: r for r in results}
    retries = []
    for wait in RATE_LIMIT_WAITS:
        if returncode == 0 or not _transient_only(list(latest.values())):
            break
        threads = _retry_threads(list(latest.values()))
        time.sleep(wait)
        retry = ["dbt", "retry", "--target", target, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
        returncode, retried, tail = _dbt([*retry, "--threads", str(threads)])
        retries.append({"after_seconds": wait, "threads": threads, "nodes": len(retried), "exit_code": returncode})
        if not retried:
            retries[-1]["output_tail"] = tail[-600:]  # why it wrote no results (job logs are unreadable)
            break
        latest.update({r["unique_id"]: r for r in retried})
    results = list(latest.values())
    # The exit code so far is the last dbt command's: a retry re-runs only the failed and skipped nodes, so it can
    # exit 0 while an earlier failure stays in `latest` (or write no results at all); any failure left fails the build.
    if returncode == 0 and any(r["status"] in FAILED for r in results):
        returncode = 1
    summary = {
        "target": target,
        "select": selectors,
        "exit_code": returncode,
        "by_status": {},
        "failures": [],
    }
    if retries:
        summary["transient_retries"] = retries
    for r in results:
        summary["by_status"][r["status"]] = summary["by_status"].get(r["status"], 0) + 1
        if r["status"] in FAILED:
            summary["failures"].append({"node": r["unique_id"], "message": str(r.get("message"))})
        elif r["status"] == "warn":  # warn-severity tests: the count of the rows they found
            summary.setdefault("warnings", []).append({"node": r["unique_id"], "rows": r.get("failures")})
    if not results:
        summary["output_tail"] = tail
    if changed_plan is not None:
        from warehouse import changed

        summary["changed_only"] = {
            "rebuilt": len(selectors),
            "full_refresh": changed_plan["full_refresh"],
            **_why(changed_plan),
        }
        summary["changed_only"]["properties"] = changed.record(
            changed_plan["cat"], changed_plan["meta"], results, started_ms
        )
    if target == "prod" and any(r["unique_id"].startswith("model.") and r["status"] == "success" for r in results):
        try:
            summary["row_ceilings"] = row_ceilings(results)
        except Exception as exc:  # the check never hides the build's own result
            summary["row_ceilings"] = {"above_twice": [], "errors": [f"{type(exc).__name__}: {str(exc)[:200]}"]}
        if summary["row_ceilings"]["above_twice"] and returncode == 0:
            returncode = 1
    if returncode == 0 and target == "prod":
        from warehouse import catalog

        summary["catalog"] = catalog.publish()
    level = "notice" if returncode == 0 else "error"
    ceilings = summary.get("row_ceilings") or {}
    if level == "notice" and (ceilings.get("over") or ceilings.get("unlisted") or ceilings.get("errors")):
        level = "warning"
    annotate(level, "warehouse dbt build", _for_annotation(summary))
    step_summary(f"## warehouse · dbt build\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return returncode, 0


def table_rows(table) -> int:
    """Rows of a table's current snapshot: its summary's total-records, or the data files'
    record counts when a writer left the totals out (no data scan; deletes not subtracted)."""
    from pyiceberg.manifest import DataFileContent

    snapshot = table.current_snapshot()
    if snapshot is None:
        return 0
    total = snapshot.summary.get("total-records") if snapshot.summary is not None else None
    if total is not None:
        return int(total)
    return sum(
        entry.data_file.record_count
        for manifest in snapshot.manifests(table.io)
        for entry in manifest.fetch_manifest_entry(table.io)
        if entry.data_file.content == DataFileContent.DATA
    )


def row_ceilings(results: list[dict]) -> dict:
    """Owner policy #822 (#868): the rows of each full-rebuild model this build wrote, against
    its declared ceiling (`full_rebuild`, warehouse/declared.py). Above the ceiling: a warning; above twice the
    ceiling: the build fails, so a full rebuild that grew is noticed and made incremental (or
    given an exception). A full-rebuild model missing from the list is reported as unlisted
    (repo_check fails on it too). dbt's manifest of this build gives materializations and tables."""
    from ingestion.common.r2 import catalog
    from warehouse import changed

    meta = json.loads((PROJECT / "target" / "manifest.json").read_text())
    cat = catalog()
    out: dict = {"checked": 0, "over": [], "above_twice": [], "unlisted": [], "errors": []}
    for r in results:
        uid = r["unique_id"]
        node = meta["nodes"].get(uid)
        if not uid.startswith("model.") or r["status"] != "success" or node is None:
            continue
        if node.get("config", {}).get("materialized") == "incremental":
            continue
        name = node["name"]
        if name not in FULL_REBUILD:
            out["unlisted"].append(name)
            continue
        try:
            rows = table_rows(cat.load_table(changed.relation(node)))
        except Exception as exc:  # reported, never hides the build's own result
            out["errors"].append(f"{name}: {type(exc).__name__}: {str(exc)[:150]}")
            continue
        out["checked"] += 1
        ceiling = int(FULL_REBUILD[name]["rows"])
        if rows > ceiling:
            out["over"].append({"model": name, "rows": rows, "ceiling": ceiling})
        if rows > 2 * ceiling:
            out["above_twice"].append(name)
    out["errors"] = out["errors"][:5]
    return out
