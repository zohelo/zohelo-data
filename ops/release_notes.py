"""Weekly Release of the zohelo-data engine (#1977): `engine-<ISO year>-W<week>` on main.

Engine changes (ingestion, warehouse, ops) are live once merged; one Release a week lists the pull
requests merged since the previous engine-* Release (GitHub's generated notes). No merged pull request
since then: annotation 'no changes', nothing created. Token: GITHUB_TOKEN with contents: write.
"""

from __future__ import annotations

from datetime import UTC, datetime
import os
import sys

import requests

from ingestion.common.report import annotate

GITHUB_API = "https://api.github.com"
PREFIX = "engine-"


def session() -> requests.Session:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN is not set")
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
    return s


def previous_release(s: requests.Session, repo: str) -> dict | None:
    """The newest engine-* Release (by creation time), or None."""
    newest, page = None, 1
    while True:
        r = s.get(f"{GITHUB_API}/repos/{repo}/releases", params={"per_page": 100, "page": page}, timeout=30)
        r.raise_for_status()
        releases = r.json()
        for rel in releases:
            if rel["tag_name"].startswith(PREFIX) and (newest is None or rel["created_at"] > newest["created_at"]):
                newest = rel
        if len(releases) < 100:
            return newest
        page += 1


def merged_since(s: requests.Session, repo: str, since: str | None) -> list[int]:
    """Numbers of pull requests merged into main after `since` (ISO time; None = all)."""
    merged, page = [], 1
    while True:
        r = s.get(
            f"{GITHUB_API}/repos/{repo}/pulls",
            params={
                "state": "closed",
                "base": "main",
                "sort": "updated",
                "direction": "desc",
                "per_page": 100,
                "page": page,
            },
            timeout=30,
        )
        r.raise_for_status()
        pulls = r.json()
        for pr in pulls:
            if pr.get("merged_at") and (since is None or pr["merged_at"] > since):
                merged.append(pr["number"])
        # Sorted by last update: once a page ends before `since`, later pages are older still.
        if len(pulls) < 100 or (since is not None and pulls[-1]["updated_at"] < since):
            return merged
        page += 1


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv:
        print("usage: python -m ops.release_notes (no arguments)", file=sys.stderr)
        return 2
    repo = os.environ.get("GITHUB_REPOSITORY") or "zohelo/zohelo-data"
    year, week, _ = datetime.now(UTC).isocalendar()
    tag = f"{PREFIX}{year}-W{week:02d}"
    try:
        s = session()
        previous = previous_release(s, repo)
        if previous and previous["tag_name"] == tag:
            annotate("notice", "release notes", {"release": tag, "result": "exists", "url": previous["html_url"]})
            return 0
        since = previous["created_at"] if previous else None
        merged = merged_since(s, repo, since)
        if not merged:
            annotate("notice", "release notes", {"result": "no changes", "since": previous and previous["tag_name"]})
            return 0
        # Notes against the previous engine-* tag, not the newest Release of any kind (portal-* Releases).
        notes = {"tag_name": tag, "target_commitish": "main"}
        if previous:
            notes["previous_tag_name"] = previous["tag_name"]
        r = s.post(f"{GITHUB_API}/repos/{repo}/releases/generate-notes", json=notes, timeout=30)
        r.raise_for_status()
        body = r.json()["body"]
        r = s.post(
            f"{GITHUB_API}/repos/{repo}/releases",
            json={
                "tag_name": tag,
                "target_commitish": "main",
                "name": f"zohelo-data engine {year}-W{week:02d}",
                "body": body,
            },
            timeout=30,
        )
        r.raise_for_status()
    except (RuntimeError, requests.RequestException) as exc:
        annotate("error", "release notes", {"release": tag, "error": str(exc)[:500]})
        return 1
    annotate(
        "notice", "release notes", {"release": tag, "merged pull requests": len(merged), "url": r.json()["html_url"]}
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
