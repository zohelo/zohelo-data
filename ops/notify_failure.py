#!/usr/bin/env python3
"""Open (or update) a GitHub issue when a Pipeline run fails, so failures reach the owner.

Run by the Pipeline workflow's `notify` job when any job of the run failed or was
cancelled. One alert per source and run type ("Pipeline failed: <source> · <run>"), labelled `alert`
(type Bug): a repeated failure comments on the open alert, or re-opens the newest alert of that title
when it was closed less than 14 days ago; only otherwise a new one is created. The alert is made a
sub-issue of its source Epic (register entry with source_system == RUN_SOURCE -> Epic key
`data/src-<id lower>`); when there is no such Epic issue (source `scheduler`, or the backlog sync has
not created it yet) the alert stays without a parent. GitHub e-mails the owner about new issues and comments.
Needs GITHUB_TOKEN with `issues: write`; reads the run context from the environment.

``--check-jobs`` (runs with an extract step): the extract jobs continue on error so landed files
still reach Bronze, which hides their failure from the workflow's failure(); the run's job
conclusions are read (`actions: read`) and the alert is raised only when a job failed or was
cancelled (a model build replaced in its queue is cancelled by design and does not count); this
job then exits 1, so the run concludes "failure" for the scheduler's catch-up and the tracker.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import sys

import requests
import yaml

API = "https://api.github.com"
LABEL = "alert"
CANCELLED_BY_DESIGN = ("silver_gold.build",)
REOPEN_DAYS = 14
REGISTER = Path(__file__).resolve().parent.parent / "ingestion" / "register.yaml"


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


def newest_alert(session: requests.Session, repo: str, title: str) -> dict | None:
    """The newest issue labelled `alert` with exactly this title, open or closed (None when there is none)."""
    page = 1
    while True:
        response = session.get(
            f"{API}/repos/{repo}/issues",
            params={
                "labels": LABEL,
                "state": "all",
                "sort": "created",
                "direction": "desc",
                "per_page": 100,
                "page": page,
            },
            timeout=30,
        )
        response.raise_for_status()
        items = response.json()
        for item in items:
            if item["title"] == title and "pull_request" not in item:
                return item
        if len(items) < 100:
            return None
        page += 1


def recently_closed(issue: dict, now: datetime | None = None) -> bool:
    """True when the issue was closed less than REOPEN_DAYS days ago."""
    closed = issue.get("closed_at")
    if not closed:
        return False
    closed_at = datetime.fromisoformat(closed.replace("Z", "+00:00"))
    return (now or datetime.now(UTC)) - closed_at < timedelta(days=REOPEN_DAYS)


def epic_key(source: str, register: Path = REGISTER) -> str | None:
    """Backlog key of the source's Epic: the register entry whose source_system is `source` -> data/src-<id lower>."""
    entries = (yaml.safe_load(register.read_text()) or {}).get("sources") or []
    for entry in entries:
        if entry.get("source_system") == source and entry.get("id"):
            return f"data/src-{str(entry['id']).lower()}"
    return None


def find_epic(session: requests.Session, repo: str, key: str) -> dict | None:
    """The issue whose body starts with the backlog key marker, or None."""
    marker = f"<!-- zohelo:key={key} -->"
    page = 1
    while True:
        response = session.get(
            f"{API}/repos/{repo}/issues",
            params={"state": "all", "type": "Epic", "per_page": 100, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        items = response.json()
        for item in items:
            if "pull_request" not in item and (item.get("body") or "").split("\n", 1)[0].strip() == marker:
                return item
        if len(items) < 100:
            return None
        page += 1


def link_to_epic(session: requests.Session, repo: str, alert: dict, source: str) -> None:
    """Add the alert as a sub-issue of its source Epic; skip quietly when there is no Epic issue.

    A failure here never hides the alert itself: it is reported as a warning only."""
    try:
        key = epic_key(source)
        epic = find_epic(session, repo, key) if key else None
        if not epic:
            return
        response = session.post(
            f"{API}/repos/{repo}/issues/{epic['number']}/sub_issues", json={"sub_issue_id": alert["id"]}, timeout=30
        )
        if response.status_code != 422:  # 422: already a sub-issue of an Epic
            response.raise_for_status()
    except (requests.RequestException, OSError, ValueError, yaml.YAMLError) as error:
        print(f"::warning::alert not linked to its Epic: {error}", file=sys.stderr)


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

    previous = newest_alert(session, repo, title)
    again = f"Failed or cancelled again: {link}"
    if previous and previous["state"] == "open":
        number = previous["number"]
        response = session.post(f"{API}/repos/{repo}/issues/{number}/comments", json={"body": again}, timeout=30)
    elif previous and recently_closed(previous):
        number = previous["number"]
        reopened = session.patch(
            f"{API}/repos/{repo}/issues/{number}", json={"state": "open", "state_reason": "reopened"}, timeout=30
        )
        reopened.raise_for_status()
        response = session.post(f"{API}/repos/{repo}/issues/{number}/comments", json={"body": again}, timeout=30)
        link_to_epic(session, repo, previous, source)
    else:
        response = session.post(
            f"{API}/repos/{repo}/issues",
            json={"title": title, "body": body, "labels": [LABEL], "type": "Bug"},
            timeout=30,
        )
        response.raise_for_status()
        link_to_epic(session, repo, response.json(), source)
    response.raise_for_status()
    print(response.json().get("html_url"))
    # A failed extract leg continues on error, so the run would end "success" and the scheduler's
    # catch-up would treat the slot as done: this job fails to give the run its true conclusion.
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
