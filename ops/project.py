#!/usr/bin/env python3
"""Read the organisation's GitHub Projects board through GraphQL.

Runs in the Ops workflow with a token of the `zohelo-automation` GitHub App (PROJECT_APP_ID as a
variable or secret, secret PROJECT_APP_KEY; the workflow mints GH_PROJECT_TOKEN). Projects
live at organisation level and are reachable only through GraphQL, which neither the
workflow's GITHUB_TOKEN nor an AI session can use.

Modes:
  inspect   list the organisation's projects with their fields, options, iterations and item count
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from typing import Any

import requests

from ingestion.common.report import annotate, step_summary

API = "https://api.github.com/graphql"

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


def main(argv: list[str]) -> int:
    """Run one mode; print the result and keep it as projects.json in OPS_OUTPUT_DIR."""
    token = os.environ.get("GH_PROJECT_TOKEN", "")
    org = os.environ.get("GITHUB_REPOSITORY_OWNER", "zohelo")
    if not token:
        print("GH_PROJECT_TOKEN is missing: is the zohelo-automation app configured?", file=sys.stderr)
        return 1
    mode = argv[0] if argv else "inspect"
    if mode != "inspect":
        print(f"unknown mode {mode!r} (available: inspect)", file=sys.stderr)
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


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
