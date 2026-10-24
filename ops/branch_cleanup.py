#!/usr/bin/env python3
"""Delete leftover work branches (owner approval, 24 Oct 2026).

Candidates: branches named ``test/*`` or ``dev/*`` (Dev trials and probes), plus any branch
named with ``--also`` (a one-off list the owner approved). A candidate is deleted only when
it has no open pull request and its last commit is older than ``--min-age-hours``. ``main``
and branches with open pull requests are never touched. Without ``--apply`` the script only
reports what it would delete. Needs GITHUB_TOKEN with ``contents: write`` (the Ops workflow's
branch_cleanup job) and GITHUB_REPOSITORY.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import os
from urllib.parse import quote

import requests

from ingestion.common.report import annotate, step_summary

API = "https://api.github.com"
PREFIXES = ("test/", "dev/")
PROTECTED = {"main"}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="delete; without it, only report")
    parser.add_argument("--min-age-hours", type=float, default=48)
    parser.add_argument("--also", nargs="*", action="extend", default=[], help="other branch names the owner approved")
    args = parser.parse_args(argv)
    repo, token = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"]
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})

    def pages(path: str):
        page = 1
        while True:
            response = session.get(f"{API}/repos/{repo}/{path}", params={"per_page": 100, "page": page}, timeout=30)
            response.raise_for_status()
            rows = response.json()
            yield from rows
            if len(rows) < 100:
                return
            page += 1

    in_use = set()  # heads of open pull requests from this repository, and every open PR's base
    for pr in pages("pulls?state=open"):
        if (pr["head"].get("repo") or {}).get("full_name") == repo:
            in_use.add(pr["head"]["ref"])
        in_use.add(pr["base"]["ref"])
    now = datetime.now(UTC)
    report: dict = {"apply": args.apply, "deleted": [], "would_delete": [], "kept": [], "failed": []}
    for branch in pages("branches"):
        name = branch["name"]
        if name in PROTECTED or not (name.startswith(PREFIXES) or name in args.also):
            continue
        if name in in_use:
            report["kept"].append([name, "open pull request"])
            continue
        if branch.get("protected"):
            report["kept"].append([name, "protected"])
            continue
        commit = session.get(f"{API}/repos/{repo}/commits/{branch['commit']['sha']}", timeout=30)
        commit.raise_for_status()
        when = datetime.fromisoformat(commit.json()["commit"]["committer"]["date"].replace("Z", "+00:00"))
        age = (now - when).total_seconds() / 3600
        if age < args.min_age_hours:
            report["kept"].append([name, f"last commit {age:.0f} h ago"])
            continue
        if not args.apply:
            report["would_delete"].append(name)
            continue
        deleted = session.delete(f"{API}/repos/{repo}/git/refs/heads/{quote(name, safe='/')}", timeout=30)
        if deleted.status_code in (204, 404):
            report["deleted"].append(name)
        else:  # e.g. a ruleset blocks it: recorded, the others go on
            report["failed"].append([name, deleted.status_code, deleted.text[:120]])
    annotate("warning" if report["failed"] else "notice", "branch cleanup", report)
    step_summary("## ops · branch_cleanup\n\n" + "\n".join(f"- {k}: {v}" for k, v in report.items()) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
