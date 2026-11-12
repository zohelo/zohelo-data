"""silver_gold.queue: decides, before a run's model build is queued, whether another run's build does its work.

Only one model build runs at a time (pipeline.yml, concurrency group ``zohelo-pipeline-build``). GitHub keeps one
running and one waiting job per group and cancels the waiting one when another arrives, so sources dispatched
together cancelled each other's builds and their runs ended ``cancelled`` (#2932). This job runs before the
build job and keeps every run out of the group unless the waiting place is free:

- ``enter``: no build is waiting. The run's build job joins the group (it runs at once, or waits for the running one).
- ``covered``: a changed-only build of another run is waiting and has not started. That build plans after this
  run's Bronze load ended (``warehouse/changed.py`` is stateless and reads the tables when the build starts), so
  it rebuilds what this run changed. This run's build and publish are skipped and the run ends green, naming
  the covering run.
- a build with a model filter (a ``platform`` run with a ``filter``) is not changed-only: it covers no other run
  and no run covers it. When such a build is waiting, or this run is one and any build is waiting, this job
  waits until the waiting build has started and decides again.

Queue jobs decide one after another, in the order GitHub created them (job id): a job waits while an earlier queue
job of another run is still deciding, so two runs that arrive together never both see a free place. The state is
read from the GitHub API (the jobs of the Pipeline runs on main that have not ended); nothing is stored. An API
error or a wait longer than WAIT_LIMIT ends as ``enter`` with a warning: the worst case is then GitHub's own
replacement, as before, never a build that nobody does.

Standard library only: the job runs on the runner's Python without installing the requirements.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

from ingestion.common.report import annotate, step_summary

API = "https://api.github.com"
QUEUE_JOB = "silver_gold.queue"  # this job's name in pipeline.yml; a build with a model filter adds FORCED
BUILD_JOB = "silver_gold.build"
FORCED = "· filter"
COVERED_STEP = "covered by another run"  # pipeline.yml: the step that runs only when the decision is `covered`
RUN_PAGES = 2  # of 100 dispatched runs on main, newest first: about six hours of runs
SHORT_WAIT, SHORT_WAITS, LONG_WAIT = 5, 12, 120  # seconds: a queue job decides in seconds; a build starts in hours
WAIT_LIMIT = 180 * 60  # the build job's own time limit
OWN_JOB_TRIES = 5


def _get(repo: str, token: str, path: str) -> dict:
    request = urllib.request.Request(
        f"{API}/repos/{repo}/actions/{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def place(run: dict, jobs: list[dict]) -> dict | None:
    """Another run's place in the build queue, from its jobs: None when its build has not come near the queue or
    has ended, else {"run", "state", "forced", "queue_job"} with state

    - ``deciding``: its queue job has not ended;
    - ``waiting``: it entered and its build job has not started (GitHub lists a job that waits for the
      concurrency group as ``pending``, one that waits for a runner as ``queued``, and for a moment not at all);
    - ``running``: its build job is in progress.

    A run of the workflow as it was before this job (no queue job) is read from its build job alone."""
    queue = next((j for j in jobs if str(j.get("name", "")).startswith(QUEUE_JOB)), None)
    build = next((j for j in jobs if j.get("name") == BUILD_JOB), None)
    if queue is None and build is None:
        return None
    forced = queue["name"].endswith(FORCED) if queue else "· platform ·" in str(run.get("display_title", ""))
    if build is not None:
        if build.get("status") == "completed":
            return None
        state = "running" if build.get("status") == "in_progress" else "waiting"
    elif queue.get("status") != "completed":
        state = "deciding"
    elif queue.get("conclusion") != "success" or any(
        s.get("name") == COVERED_STEP and s.get("conclusion") == "success" for s in queue.get("steps") or []
    ):
        return None  # failed or cancelled, or covered: it does not build
    else:
        state = "waiting"
    return {"run": int(run["id"]), "state": state, "forced": forced, "queue_job": int(queue["id"]) if queue else 0}


def decide(forced: bool, queue_job: int, others: list[dict]) -> tuple[str, int | None]:
    """(``enter`` | ``covered`` | ``wait``, the run that covers or is waited for)."""
    earlier = [o for o in others if o["state"] == "deciding" and o["queue_job"] < queue_job]
    if earlier:
        return "wait", earlier[0]["run"]
    waiting = [o for o in others if o["state"] == "waiting"]
    if not waiting:
        return "enter", None
    covering = [o for o in waiting if not o["forced"]]
    if not forced and covering:
        return "covered", covering[0]["run"]
    return "wait", waiting[0]["run"]


def snapshot(repo: str, token: str, run_id: int) -> tuple[int, list[dict]]:
    """(this run's queue job id, the places of the other Pipeline runs on main that have not ended)."""
    own = None
    for attempt in range(OWN_JOB_TRIES):
        jobs = _get(repo, token, f"runs/{run_id}/jobs?per_page=100")["jobs"]
        own = next((j for j in jobs if str(j.get("name", "")).startswith(QUEUE_JOB)), None)
        if own is not None:
            break
        time.sleep(3 * (attempt + 1))
    if own is None:
        raise LookupError(f"run {run_id} lists no {QUEUE_JOB} job")
    others = []
    for page in range(1, RUN_PAGES + 1):
        runs = _get(
            repo, token, f"workflows/pipeline.yml/runs?branch=main&event=workflow_dispatch&per_page=100&page={page}"
        )["workflow_runs"]
        for run in runs:
            if int(run["id"]) == run_id or run.get("status") == "completed":
                continue
            found = place(run, _get(repo, token, f"runs/{run['id']}/jobs?per_page=100")["jobs"])
            if found is not None:
                others.append(found)
        if len(runs) < 100:
            break
    return int(own["id"]), others


def main() -> int:
    repo, token = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"]
    run_id = int(os.environ["GITHUB_RUN_ID"])
    forced = os.environ.get("FORCED") == "true"
    started = time.monotonic()
    waits, waited_for, others, problem = 0, [], [], None
    try:
        while True:
            queue_job, others = snapshot(repo, token, run_id)
            decision, other = decide(forced, queue_job, others)
            if decision != "wait":
                break
            if other not in waited_for:
                waited_for.append(other)
            if time.monotonic() - started > WAIT_LIMIT:
                decision, other, problem = "enter", None, f"waited over {WAIT_LIMIT // 60} minutes"
                break
            time.sleep(SHORT_WAIT if waits < SHORT_WAITS else LONG_WAIT)
            waits += 1
    except (urllib.error.URLError, OSError, LookupError, ValueError, TypeError) as error:
        # Never a build that nobody does: without a reading of the queue the run builds, as it did before this job.
        decision, other, problem = "enter", None, f"{type(error).__name__}: {error}"[:300]
    summary = {
        "decision": decision,
        "forced": forced,
        "decided_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "waited_seconds": round(time.monotonic() - started),
        "waited_for_runs": waited_for,
        "queue": others,
    }
    if decision == "covered":
        summary["covered_by_run"] = other
        summary["covered_by"] = (
            f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{repo}/actions/runs/{other}"
        )
        summary["why"] = (
            f"the changed-only build of run {other} had not started at decided_at, after this run's Bronze load; "
            "it plans when it starts and rebuilds what this run changed. This run builds and publishes nothing."
        )
    if problem:
        summary["problem"] = problem
    title = f"silver_gold.build covered by run {other}" if decision == "covered" else "silver_gold.queue"
    annotate("warning" if problem else "notice", title, summary)
    step_summary(f"## {title}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"decision={decision}\ncovered_by={other or ''}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
