#!/usr/bin/env python3
"""Open (or update) a GitHub issue when a Pipeline run fails, so failures reach the owner.

Run by the Pipeline workflow's `notify` job when any job of the run failed or was
cancelled. One open issue per source and run type ("Pipeline failed: <source> · <run>"),
labelled `alert`: a repeated failure adds a comment instead of a new issue; closing the
issue after the fix re-arms the alert. GitHub e-mails the owner about new issues and comments.
Needs GITHUB_TOKEN with `issues: write`; reads the run context from the environment.
"""

from __future__ import annotations

import os
import sys

import requests

API = "https://api.github.com"
LABEL = "alert"


def main() -> int:
    repo, token, run_id = (os.environ.get(k, "") for k in ("GITHUB_REPOSITORY", "GITHUB_TOKEN", "GITHUB_RUN_ID"))
    if not (repo and token and run_id):
        print("missing GITHUB_REPOSITORY, GITHUB_TOKEN or GITHUB_RUN_ID", file=sys.stderr)
        return 1
    source = os.environ.get("RUN_SOURCE") or "scheduler"
    run = os.environ.get("RUN_NAME") or "schedule"
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
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
            f"{API}/repos/{repo}/issues/{number}/comments", json={"body": f"Failed again: {link}"}, timeout=30
        )
    else:
        response = session.post(
            f"{API}/repos/{repo}/issues", json={"title": title, "body": body, "labels": [LABEL, "area:ops"]}, timeout=30
        )
    response.raise_for_status()
    print(response.json().get("html_url"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
