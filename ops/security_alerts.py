"""Mirror open Dependabot and code-scanning alerts of the three repositories into Bug issues (#1974).

Modes: `plan` (default) reads and annotates; `apply` creates one Bug per open alert in the alert's repository
(labels `security-alert` and `alert-<dependabot|code-scanning>-<number>`, no key marker) and closes the Bug of
an alert that is no longer open, with a comment naming the alert's state. Existing Bugs are found by the
`alert-<kind>-<number>` label, so a rerun creates nothing new. At most 200 writes per run, 1 s apart.
Token: GH_PROJECT_TOKEN (the org app: Dependabot alerts and security events read, issues write for apply).
"""

from __future__ import annotations

from collections import Counter
import os
import sys
import time

import requests

from ingestion.common.report import annotate

REST = "https://api.github.com"
ORG = "zohelo"
REPOS = ["zohelo-data", "zohelo-bi", "zohelo"]
WRITE_LIMIT = 200
KINDS = {"dependabot": "dependabot/alerts", "code-scanning": "code-scanning/alerts"}


class Stop(Exception):
    """The write limit is reached; a rerun continues."""


class Api:
    def __init__(self, token: str, apply: bool) -> None:
        self.http = requests.Session()
        self.http.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        self.apply = apply
        self.writes = 0

    def get(self, path: str) -> requests.Response:
        return self.http.get(REST + path, timeout=60)

    def pages(self, path: str) -> list[dict]:
        items, page = [], 1
        while True:
            sep = "&" if "?" in path else "?"
            response = self.get(f"{path}{sep}per_page=100&page={page}")
            if response.status_code != 200:
                raise PermissionError(f"GET {path}: HTTP {response.status_code} {response.text[:300]}")
            batch = response.json()
            items += batch
            if len(batch) < 100:
                return items
            page += 1

    def write(self, method: str, path: str, body: dict) -> dict:
        if not self.apply:
            return {}
        if self.writes >= WRITE_LIMIT:
            raise Stop
        self.writes += 1
        response = self.http.request(method, REST + path, json=body, timeout=60)
        if response.status_code not in (200, 201):
            raise RuntimeError(f"{method} {path}: HTTP {response.status_code} {response.text[:300]}")
        time.sleep(1)
        return response.json()


def describe(kind: str, alert: dict) -> dict:
    """Name, severity, summary, path and URL of one alert."""
    if kind == "dependabot":
        dep = alert.get("dependency") or {}
        advisory = alert.get("security_advisory") or {}
        severity = (alert.get("security_vulnerability") or {}).get("severity") or advisory.get("severity")
        return {
            "name": (dep.get("package") or {}).get("name") or "unknown package",
            "severity": severity or "unknown",
            "summary": advisory.get("summary") or "",
            "path": dep.get("manifest_path") or "",
        }
    rule = alert.get("rule") or {}
    location = (alert.get("most_recent_instance") or {}).get("location") or {}
    return {
        "name": rule.get("id") or "unknown rule",
        "severity": rule.get("security_severity_level") or rule.get("severity") or "unknown",
        "summary": rule.get("description") or "",
        "path": location.get("path") or "",
    }


def bug(kind: str, alert: dict) -> dict:
    d = describe(kind, alert)
    body = "\n".join(
        [
            f"**Severity:** {d['severity']}",
            f"**{'Advisory' if kind == 'dependabot' else 'Rule'}:** {d['summary']}",
            f"**{'Manifest' if kind == 'dependabot' else 'File'}:** `{d['path']}`",
            f"**Alert:** {alert.get('html_url')}",
            "",
            "Fix: upgrade or patch so that the alert closes; Closes this Bug.",
        ]
    )
    return {
        "title": f"Security alert: {d['name']} ({d['severity']})"[:256],
        "body": body,
        "labels": ["security-alert", f"alert-{kind}-{alert['number']}"],
        "type": "Bug",
    }


def sync_repo(api: Api, repo: str) -> dict:
    out: dict = {"open alerts": 0, "by severity": Counter(), "bugs created": 0, "bugs closed": 0}
    open_alerts: dict[str, tuple[str, dict]] = {}
    listed = set()  # kinds whose alerts were read; Bugs of an unread kind are never closed
    for kind, path in KINDS.items():
        try:
            alerts = api.pages(f"/repos/{ORG}/{repo}/{path}?state=open")
        except PermissionError as exc:
            out.setdefault("errors", []).append(str(exc))
            continue
        listed.add(kind)
        for alert in alerts:
            open_alerts[f"alert-{kind}-{alert['number']}"] = (kind, alert)
            out["by severity"][f"{kind}:{describe(kind, alert)['severity']}"] += 1
    out["open alerts"] = len(open_alerts)
    bugs: dict[str, dict] = {}
    for issue in api.pages(f"/repos/{ORG}/{repo}/issues?state=all&labels=security-alert"):
        for label in issue.get("labels", []):
            if label["name"].startswith("alert-") and "pull_request" not in issue:
                bugs.setdefault(label["name"], issue)
    for label, (kind, alert) in open_alerts.items():
        if label not in bugs:
            api.write("POST", f"/repos/{ORG}/{repo}/issues", bug(kind, alert))
            out["bugs created"] += 1
    for label, issue in bugs.items():
        if issue["state"] != "open" or label in open_alerts:
            continue
        kind, number = label[len("alert-") :].rsplit("-", 1)
        if kind not in listed:
            continue
        response = api.get(f"/repos/{ORG}/{repo}/{KINDS[kind]}/{number}")
        state = (
            response.json().get("state", "unknown") if response.status_code == 200 else f"HTTP {response.status_code}"
        )
        api.write(
            "POST",
            f"/repos/{ORG}/{repo}/issues/{issue['number']}/comments",
            {"body": f"The alert is no longer open (state: {state}); closing."},
        )
        api.write(
            "PATCH", f"/repos/{ORG}/{repo}/issues/{issue['number']}", {"state": "closed", "state_reason": "completed"}
        )
        out["bugs closed"] += 1
    out["by severity"] = dict(out["by severity"])
    return out


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    mode = argv[0] if argv else "plan"
    if mode not in ("plan", "apply") or len(argv) > 1:
        print("usage: python -m ops.security_alerts [plan|apply]", file=sys.stderr)
        return 2
    token = os.environ.get("GH_PROJECT_TOKEN") or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        annotate("error", "security alerts", {"error": "no GitHub token (GH_PROJECT_TOKEN)"})
        return 1
    api = Api(token, apply=mode == "apply")
    failed = False
    for repo in REPOS:
        try:
            result = sync_repo(api, repo)
        except Stop:
            annotate("warning", f"security alerts · {repo}", {"stopped": f"{WRITE_LIMIT} writes; rerun continues"})
            return 0
        except (RuntimeError, PermissionError, requests.RequestException) as exc:
            annotate("error", f"security alerts · {repo}", {"error": str(exc)[:600]})
            failed = True
            continue
        if mode == "plan":
            result["bugs to create"] = result.pop("bugs created")
            result["bugs to close"] = result.pop("bugs closed")
        failed |= bool(result.get("errors"))
        annotate("warning" if result.get("errors") else "notice", f"security alerts · {repo} · {mode}", result)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
