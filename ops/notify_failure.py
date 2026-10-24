#!/usr/bin/env python3
"""Open (or update) a GitHub issue when a Pipeline run fails, so failures reach the owner.

Run by the Pipeline workflow's `notify` job when any job of the run failed or was
cancelled. One open issue per source and run type ("Pipeline failed: <source> · <run>"),
labelled `alert`: a repeated failure adds a comment instead of a new issue; closing the
issue after the fix re-arms the alert. GitHub e-mails the owner about new issues and comments.
Needs GITHUB_TOKEN with `issues: write`; reads the run context from the environment.

``--check-jobs`` (runs with an extract step): the extract jobs continue on error so landed files
still reach Bronze, which hides their failure from the workflow's failure(); the run's job
conclusions are read (`actions: read`) and the alert is raised only when a job failed or was
cancelled (a model build replaced in its queue is cancelled by design and does not count); this
job then exits 1, so the run concludes "failure" for the scheduler's catch-up and the tracker.
"""

from __future__ import annotations

import argparse
import os
import sys

import requests

API = "https://api.github.com"
LABEL = "alert"
CANCELLED_BY_DESIGN = ("silver_gold.build",)


def failed_jobs(session: requests.Session, repo: str, run_id: str) -> list[str]:
    """Names of the run's jobs that ended failed or cancelled (this notify job excluded)."""
    names, page = [], 1
    while True:
        response = session.get(
            f"{API}/repos/{repo}/actions/runs/{run_id}/jobs", params={"per_page": 100, "page": page}, timeout=30
        )
        response.raise_for_status()
        jobs = response.json().get("jobs") or []
        for job in jobs:
            name, conclusion = job.get("name") or "", job.get("conclusion")
            if name == "notify":
                continue
            if conclusion is None and job.get("status") != "completed":  # not finished: never fail open
                names.append(f"{name} (status {job.get('status')})")
                continue
            if conclusion == "failure" or (conclusion == "cancelled" and not name.startswith(CANCELLED_BY_DESIGN)):
                names.append(name)
        if len(jobs) < 100:
            return names
        page += 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-jobs", action="store_true", help="alert only if a job of the run failed")
    args = parser.parse_args(argv)
    repo, token, run_id = (os.environ.get(k, "") for k in ("GITHUB_REPOSITORY", "GITHUB_TOKEN", "GITHUB_RUN_ID"))
    if not (repo and token and run_id):
        print("missing GITHUB_REPOSITORY, GITHUB_TOKEN or GITHUB_RUN_ID", file=sys.stderr)
        return 1
    source = os.environ.get("RUN_SOURCE") or "scheduler"
    run = os.environ.get("RUN_NAME") or "schedule"
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
    failed: list[str] = []
    if args.check_jobs:
        failed = failed_jobs(session, repo, run_id)
        if not failed:
            print("no failed or cancelled job: no alert")
            return 0
        print(f"failed or cancelled: {failed}")
    title = f"Pipeline failed: {source} · {run}"
    link = f"https://github.com/{repo}/actions/runs/{run_id}"
    body = (
        f"Run {link} ({event}) failed or was cancelled. The job annotations on the run page say which activity and why."
    )

    found = session.get(
        f"{API}/repos/{repo}/issues", params={"labels": LABEL, "state": "open", "per_page": 100}, timeout=30
    )
    found.raise_for_status()
    existing = [i for i in found.json() if i["title"] == title and "pull_request" not in i]
    if existing:
        number = existing[0]["number"]
        response = session.post(
            f"{API}/repos/{repo}/issues/{number}/comments",
            json={"body": f"Failed or cancelled again: {link}"},
            timeout=30,
        )
    else:
        response = session.post(
            f"{API}/repos/{repo}/issues", json={"title": title, "body": body, "labels": [LABEL, "area:ops"]}, timeout=30
        )
    response.raise_for_status()
    print(response.json().get("html_url"))
    # A failed extract leg continues on error, so the run would end "success" and the scheduler's
    # catch-up would treat the slot as done: this job fails to give the run its true conclusion.
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
