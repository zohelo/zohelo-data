#!/usr/bin/env python3
"""Read and update the organisation's GitHub Projects board through GraphQL.

Runs in the Ops workflow with a token of the `zohelo-automation` GitHub App (organisation
variable ORG_BOT_CLIENT_ID, secret ORG_BOT_PRIVATE_KEY; the workflow mints GH_PROJECT_TOKEN). Projects
live at organisation level and are reachable only through GraphQL, which neither the
workflow's GITHUB_TOKEN nor an AI session can use.

Modes:
  inspect                    list the organisation's projects with their fields, options, iterations and item count
  status <issue> [STATUS]    post the newest comment of <issue> (the weekly Work log) as a status update of
                             project 1; STATUS is ON_TRACK (default), AT_RISK, OFF_TRACK, COMPLETE or INACTIVE
  setup [--apply]            make the organisation ready for the backlog engine: issue types Area, Epic, Deliverable,
                             project 1 renamed to 'zohelo portfolio', fields Product, Phase, Effort and Sprint; without
                             --apply it only reports what is missing (the second run with --apply reports nothing)
"""

from __future__ import annotations

from datetime import date, timedelta
import json
import os
from pathlib import Path
import sys
from typing import Any

import requests

from ingestion.common.report import annotate, step_summary

API = "https://api.github.com/graphql"
REST = "https://api.github.com"
PROJECT = 1
PROJECT_TITLE = "zohelo portfolio"
STATUSES = {"ON_TRACK", "AT_RISK", "OFF_TRACK", "COMPLETE", "INACTIVE"}

STATUS_UPDATE = """
mutation($project: ID!, $body: String!, $status: ProjectV2StatusUpdateStatus!) {
  createProjectV2StatusUpdate(input: {projectId: $project, body: $body, status: $status}) {
    statusUpdate { id status createdAt }
  }
}
"""

SETUP_QUERY = """
query($org: String!, $n: Int!) {
  organization(login: $org) {
    projectV2(number: $n) {
      id title
      fields(first: 50) { nodes { __typename ... on ProjectV2FieldCommon { name dataType } } }
    }
  }
}
"""

RENAME = """
mutation($project: ID!, $title: String!) {
  updateProjectV2(input: {projectId: $project, title: $title}) { projectV2 { id title } }
}
"""

CREATE_SELECT = """
mutation($project: ID!, $name: String!, $options: [ProjectV2SingleSelectFieldOptionInput!]) {
  createProjectV2Field(
    input: {projectId: $project, dataType: SINGLE_SELECT, name: $name, singleSelectOptions: $options}
  ) {
    projectV2Field { ... on ProjectV2SingleSelectField { id name } }
  }
}
"""

CREATE_ITERATION = """
mutation($project: ID!, $name: String!, $config: ProjectV2IterationFieldConfigurationInput!) {
  createProjectV2Field(
    input: {projectId: $project, dataType: ITERATION, name: $name, iterationConfiguration: $config}
  ) {
    projectV2Field { ... on ProjectV2IterationField { id name } }
  }
}
"""

INSPECT = """
query($org: String!) {
  organization(login: $org) {
    projectsV2(first: 20) {
      nodes {
        number title shortDescription public closed url
        items { totalCount }
        fields(first: 50) {
          nodes {
            __typename
            ... on ProjectV2FieldCommon { id name dataType }
            ... on ProjectV2SingleSelectField { options { id name } }
            ... on ProjectV2IterationField {
              configuration { duration startDay iterations { id title startDate duration } }
            }
          }
        }
        views(first: 20) { nodes { number name layout } }
      }
    }
  }
}
"""


def graphql(token: str, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    """Run one GraphQL request; raise on HTTP or GraphQL errors."""
    response = requests.post(
        API, json={"query": query, "variables": variables}, headers={"Authorization": f"Bearer {token}"}, timeout=60
    )
    response.raise_for_status()
    body = response.json()
    if body.get("errors"):
        raise RuntimeError(json.dumps(body["errors"])[:2000])
    return body["data"]


def compact(project: dict[str, Any]) -> dict[str, Any]:
    """A project in a few hundred bytes: fields with types and options, iterations, views."""
    fields = []
    for field in project["fields"]["nodes"]:
        extra = [o["name"] for o in field.get("options") or []]
        iterations = (field.get("configuration") or {}).get("iterations")
        if iterations:
            extra = [f"{i['title']}@{i['startDate']}/{i['duration']}d" for i in iterations]
        fields.append(f"{field['name']}:{field['dataType']}" + (f"[{'|'.join(extra)}]" if extra else ""))
    return {
        "items": project["items"]["totalCount"],
        "closed": project["closed"],
        "public": project["public"],
        "fields": fields,
        "views": [f"{v['number']}:{v['name']}:{v['layout']}" for v in project["views"]["nodes"]],
    }


def project_id(token: str, org: str) -> str:
    """Node id of the backlog project."""
    query = "query($org: String!, $n: Int!) { organization(login: $org) { projectV2(number: $n) { id } } }"
    return graphql(token, query, {"org": org, "n": PROJECT})["organization"]["projectV2"]["id"]


# Organisation issue types the backlog engine uses; Task and Bug stay. The others are only reported.
NEW_TYPES = {
    "Area": ("A product area that groups epics", "BLUE"),
    "Epic": ("A body of work with an owner decision behind it", "PURPLE"),
    "Deliverable": ("A result that closes when its tasks are done", "GREEN"),
}
RETIRE_TYPES = ("Initiative", "Product", "Work package")

# Single select fields of the project: name -> option names (colours cycle through COLORS).
SELECT_FIELDS = {
    "Product": ["zohelo-data", "zohelo-bi", "zohelo.com"],
    "Phase": ["0 Clean & harden", "1 Production release", "2 Demo warehouse", "3 Remaining sources", "Run"],
    "Effort": ["S", "M", "L"],
}
COLORS = ["BLUE", "GREEN", "YELLOW", "ORANGE", "PURPLE", "PINK"]
SPRINT_COUNT = 12  # weekly iterations created up front; more are added in the UI


def rest_json(token: str, method: str, path: str, **kwargs: Any) -> Any:
    """One REST call; raise on HTTP errors."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    response = requests.request(method, f"{REST}{path}", headers=headers, timeout=60, **kwargs)
    response.raise_for_status()
    return response.json()


def last_monday(today: date) -> date:
    return today - timedelta(days=today.weekday())


def setup(token: str, org: str, argv: list[str]) -> int:
    """Create what the backlog engine needs (issue types, project title, fields); only report without --apply."""
    if argv not in ([], ["--apply"]):
        print("usage: setup [--apply]", file=sys.stderr)
        return 2
    apply = bool(argv)
    created: list[str] = []
    missing: list[str] = []

    types = {t["name"].lower(): t["name"] for t in rest_json(token, "GET", f"/orgs/{org}/issue-types")}
    for name, (description, color) in NEW_TYPES.items():
        if name.lower() in types:
            continue
        (created if apply else missing).append(f"issue type {name}")
        if apply:
            body = {"name": name, "description": description, "color": color.lower(), "is_enabled": True}
            rest_json(token, "POST", f"/orgs/{org}/issue-types", json=body)
    retire = [name for name in RETIRE_TYPES if name.lower() in types]
    annotate("notice", "issue types to retire (not deleted here)", retire)

    project = graphql(token, SETUP_QUERY, {"org": org, "n": PROJECT})["organization"]["projectV2"]
    if project["title"] != PROJECT_TITLE:
        (created if apply else missing).append(f"project title {project['title']!r} -> {PROJECT_TITLE!r}")
        if apply:
            graphql(token, RENAME, {"project": project["id"], "title": PROJECT_TITLE})

    existing = {f["name"] for f in project["fields"]["nodes"]}
    for name, options in SELECT_FIELDS.items():
        if name in existing:
            continue
        (created if apply else missing).append(f"field {name}")
        if apply:
            inputs = [
                {"name": option, "color": COLORS[i % len(COLORS)], "description": ""}
                for i, option in enumerate(options)
            ]
            graphql(token, CREATE_SELECT, {"project": project["id"], "name": name, "options": inputs})
    if "Sprint" not in existing:
        (created if apply else missing).append("field Sprint")
        if apply:
            start = last_monday(date.today())
            config = {
                "startDate": start.isoformat(),
                "duration": 7,
                "iterations": [
                    {
                        "title": f"Sprint {i + 1}",
                        "startDate": (start + timedelta(days=7 * i)).isoformat(),
                        "duration": 7,
                    }
                    for i in range(SPRINT_COUNT)
                ],
            }
            try:
                graphql(token, CREATE_ITERATION, {"project": project["id"], "name": "Sprint", "config": config})
            except RuntimeError as exc:
                created.pop()
                annotate("warning", "create Sprint in the UI", {"reason": str(exc)[:500]})

    if apply:
        annotate("notice", "project setup created", created or "nothing to create")
    else:
        annotate("notice", "project setup missing (plan; rerun with --apply)", missing or "nothing to create")
    return 0


# The repository is public: only comments by organisation owners or members, or by the owner's account
# (an app token sees private membership as CONTRIBUTOR), are published.
TRUSTED = {"OWNER", "MEMBER"}
AUTHORS = {"rutkala"}


def newest_comment(token: str, repo: str, issue: int) -> dict[str, Any]:
    """The newest comment of an issue written by an organisation owner or member (REST, newest page first)."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    response = requests.get(
        f"{REST}/repos/{repo}/issues/{issue}/comments", headers=headers, params={"per_page": 100}, timeout=60
    )
    response.raise_for_status()
    last = response.links.get("last", {}).get("url")
    if last:
        response = requests.get(last, headers=headers, timeout=60)
        response.raise_for_status()
    while True:
        trusted = [
            c
            for c in response.json()
            if c.get("author_association") in TRUSTED or (c.get("user") or {}).get("login") in AUTHORS
        ]
        if trusted:
            return trusted[-1]
        previous = response.links.get("prev", {}).get("url")
        if not previous:
            raise RuntimeError(f"issue {issue} has no comment by an organisation owner or member")
        response = requests.get(previous, headers=headers, timeout=60)
        response.raise_for_status()


def status(token: str, org: str, argv: list[str]) -> int:
    """Publish the newest Work log comment as a project status update."""
    valid = 1 <= len(argv) <= 2 and argv[0].isascii() and argv[0].isdigit()
    if not valid or (len(argv) == 2 and argv[1] not in STATUSES):
        print(f"usage: status <issue> [{'|'.join(sorted(STATUSES))}]", file=sys.stderr)
        return 2
    repo = os.environ.get("GITHUB_REPOSITORY", f"{org}/zohelo-data")
    comment = newest_comment(token, repo, int(argv[0]))
    body = f"{comment['body']}\n\n[Work log #{argv[0]}]({comment['html_url']})"
    state = argv[1] if len(argv) > 1 else "ON_TRACK"
    result = graphql(token, STATUS_UPDATE, {"project": project_id(token, org), "body": body, "status": state})
    annotate("notice", f"project {PROJECT} status update", result["createProjectV2StatusUpdate"]["statusUpdate"])
    return 0


def main(argv: list[str]) -> int:
    """Run one mode; results are published as annotations (inspect also keeps projects.json)."""
    token = os.environ.get("GH_PROJECT_TOKEN", "")
    org = os.environ.get("GITHUB_REPOSITORY_OWNER", "zohelo")
    if not token:
        print("GH_PROJECT_TOKEN is missing: is the zohelo-automation app configured?", file=sys.stderr)
        return 1
    mode = argv[0] if argv else "inspect"
    if mode == "status":
        return status(token, org, argv[1:])
    if mode == "setup":
        return setup(token, org, argv[1:])
    if mode != "inspect":
        print(f"unknown mode {mode!r} (available: inspect, status, setup)", file=sys.stderr)
        return 2
    projects = graphql(token, INSPECT, {"org": org})["organization"]["projectsV2"]["nodes"]
    text = json.dumps(projects, indent=2)
    print(text)
    for project in projects:  # one annotation each: readable through the REST API
        annotate("notice", f"project {project['number']}: {project['title']}", compact(project))
    step_summary(f"## Projects of {org}\n\n```json\n{text}\n```\n")
    out = Path(os.environ.get("OPS_OUTPUT_DIR", "."))
    out.mkdir(parents=True, exist_ok=True)
    (out / "projects.json").write_text(text)
    return 0


def run(argv: list[str]) -> int:
    """main() with failures published as an error annotation (job logs are not readable by AI sessions)."""
    try:
        return main(argv)
    except Exception as exc:
        annotate("error", "project failed", {"error": type(exc).__name__, "detail": str(exc)[:3000]})
        raise


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
