#!/usr/bin/env python3
"""Backlog engine (docs/backlog.md): checks and renders the plan files in `.github/backlog/`.

Standard library plus pyyaml only, because the validator also runs in zohelo-bi and zohelo.
Mode `validate [--root DIR ...]` loads areas.yaml, stages.yaml and epics/*.yaml of every root
(default: the repository root), prints `file: key: message` per error and exits 1 on any error.
Mode `render KEY [--root DIR ...]` prints the title, labels and body of the issue for one node (docs/backlog.md §5).
Mode `pr-check` (pull_request event, GITHUB_TOKEN) passes when the PR body closes an open Task or Bug.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
PREFIXES = {"data": "zohelo-data", "bi": "zohelo-bi", "com": "zohelo"}
PHASES = {0, 1, 2, 3, "run"}
PRIORITIES = {"P1", "P2", "P3"}
EFFORTS = {"S", "M", "L"}
FILE_OPS = {"edit", "create", "delete"}
BANNED = re.compile(r"(?<!\w)(investigate|consider|as needed|etc\.)(?!\w)", re.IGNORECASE)
MAX_TASK_BYTES = 3000
EPIC_FIELDS = ["key", "area", "title", "phase", "priority", "why", "outcome", "in_scope", "out_of_scope", "done_when"]
TASK_FIELDS = [
    "key",
    "type",
    "title",
    "effort",
    "state",
    "context",
    "files",
    "steps",
    "acceptance",
    "verify",
    "out_of_scope",
]


class Plan:
    """The loaded backlog files of all roots, and the errors found while loading and checking them."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.keys: dict[str, str] = {}
        self.areas: dict[str, set[str]] = {}  # root -> area keys
        self.roots: dict[str, str] = {}  # root -> key prefix
        self.epics: list[tuple[str, str, dict]] = []  # (root, file, epic)
        self.counts = {"areas": 0, "epics": 0, "deliverables": 0, "tasks": 0}
        self.created: set[tuple[str, str]] = set()  # (repo, path) created by a loaded Task
        self.depends: list[tuple[str, str, str]] = []  # (file, key, dependency)
        self.nodes: dict[str, tuple[str, dict]] = {}  # key -> (kind, node with inherited fields)
        self.stages: dict[str, int] = {}  # stage key -> number, from stages.yaml
        self.pending: list[tuple[str, str, str, list[dict]]] = []  # edit/delete paths to check

    def error(self, file: str, key: str, message: str) -> None:
        self.errors.append(f"{file}: {key}: {message}")

    def register(self, file: str, key: object) -> str:
        key = str(key)
        if key in self.keys:
            self.error(file, key, f"duplicate key (also in {self.keys[key]})")
        else:
            self.keys[key] = file
        if key.split("/")[0] not in PREFIXES or "/" not in key:
            self.error(file, key, "key must start with data/, bi/ or com/")
        return key


def missing(plan: Plan, file: str, key: str, item: dict, fields: list[str]) -> None:
    for field in fields:
        if field not in item or item[field] is None or item[field] == "":
            plan.error(file, key, f"missing required field {field}")


def too_long(plan: Plan, file: str, key: str, item: dict, limit: int) -> None:
    title = item.get("title")
    if isinstance(title, str) and len(title) > limit:
        plan.error(file, key, f"title is {len(title)} characters, the limit is {limit}")


def inherited(epic: dict) -> dict:
    """What a Deliverable or Task shows from its Epic: phase, priority, target date and source id."""
    found = {k: epic[k] for k in ("phase", "priority", "target") if k in epic}
    part = str(epic.get("key", "")).split("/")[1:2]
    if part and part[0].startswith("src-"):
        found["source"] = part[0][4:]
    return found


def stage_label(plan: Plan, deliverable_key: str) -> dict:
    """The stage of a source Epic's Deliverable (`stage:<n>-<name>`), from stages.yaml."""
    name = deliverable_key.split("/")[-1]
    if deliverable_key.split("/")[1:2][:1] and deliverable_key.split("/")[1].startswith("src-") and name in plan.stages:
        return {"stage": f"{plan.stages[name]}-{name}"}
    return {}


def load(plan: Plan, root: Path) -> None:
    import yaml

    base = root / ".github" / "backlog"

    def read(path: Path) -> object:
        try:
            return yaml.safe_load(path.read_text())
        except (OSError, yaml.YAMLError) as exc:
            plan.error(str(path), "-", f"cannot read: {str(exc).splitlines()[0]}")
            return None

    areas_file = base / "areas.yaml"
    plan.areas[str(root)] = set()
    data = read(areas_file) if areas_file.exists() else None
    if data is None and not areas_file.exists():
        plan.error(str(areas_file), "-", "file is missing")
    for area in (data or {}).get("areas") or []:
        key = plan.register(str(areas_file), area.get("key", "?"))
        plan.areas[str(root)].add(key)
        plan.roots.setdefault(str(root), key.split("/")[0])
        plan.nodes[key] = ("Area", {**area, "repo": PREFIXES.get(key.split("/")[0], "")})
        missing(plan, str(areas_file), key, area, ["key", "title", "purpose"])
        too_long(plan, str(areas_file), key, area, 60)
        plan.counts["areas"] += 1
    if (base / "stages.yaml").exists():
        stages = read(base / "stages.yaml")
        for stage in (stages or {}).get("stages") or []:
            plan.stages[str(stage.get("key"))] = stage.get("n")
    for path in sorted((base / "epics").glob("*.yaml")):
        epic = read(path)
        if isinstance(epic, dict):
            plan.epics.append((str(root), str(path), epic))
        elif epic is not None:
            plan.error(str(path), "-", "an epic file must be a mapping")


def check_epic(plan: Plan, root: str, file: str, epic: dict) -> None:
    key = plan.register(file, epic.get("key", "?"))
    plan.counts["epics"] += 1
    plan.nodes[key] = ("Epic", {**epic, "repo": PREFIXES.get(key.split("/")[0], "")})
    missing(plan, file, key, epic, EPIC_FIELDS)
    too_long(plan, file, key, epic, 60)
    if epic.get("area") and epic["area"] not in plan.areas[root]:
        plan.error(file, key, f"area {epic['area']} is not in this repo's areas.yaml")
    if epic.get("phase") not in PHASES or isinstance(epic.get("phase"), bool):
        plan.error(file, key, f"phase {epic.get('phase')!r} is not one of 0, 1, 2, 3, run")
    if epic.get("priority") not in PRIORITIES:
        plan.error(file, key, f"priority {epic.get('priority')!r} is not P1, P2 or P3")
    if "out_of_scope" in epic and not epic["out_of_scope"]:
        plan.error(file, key, "out_of_scope needs at least one bullet")
    if "done_when" in epic and not epic["done_when"]:
        plan.error(file, key, "done_when is empty")
    for dep in epic.get("depends_on") or []:
        plan.depends.append((file, key, str(dep)))
    deliverables = epic.get("deliverables") or []
    if not deliverables:
        plan.error(file, key, "missing required field deliverables")
    for deliverable in deliverables:
        check_deliverable(plan, root, file, epic, deliverable)


def check_deliverable(plan: Plan, root: str, file: str, epic: dict, item: dict) -> None:
    epic_key = str(epic.get("key"))
    key = plan.register(file, item.get("key", "?"))
    repo = item.get("repo") or PREFIXES.get(key.split("/")[0], "")
    stage = stage_label(plan, key)
    plan.nodes[key] = ("Deliverable", {**inherited(epic), **item, "repo": repo, **stage})
    plan.counts["deliverables"] += 1
    missing(plan, file, key, item, ["key", "title", "done_when"])
    too_long(plan, file, key, item, 60)
    if not key.startswith(epic_key + "/"):
        plan.error(file, key, f"key must start with {epic_key}/")
    if item.get("state", "open") not in {"open", "done"}:
        plan.error(file, key, f"state {item.get('state')!r} is not open or done")
    if item.get("state") == "done" and not item.get("notes"):
        plan.error(file, key, "state done needs evidence in notes")
    for dep in item.get("depends_on") or []:
        plan.depends.append((file, key, str(dep)))
    tasks = item.get("tasks") or []
    if not tasks and not item.get("todo") and item.get("state") != "done":
        plan.error(file, key, "missing required field tasks")
    for task in tasks:
        check_task(plan, root, file, epic, item, repo, task)


def check_task(plan: Plan, root: str, file: str, epic: dict, deliverable: dict, repo: str, task: dict) -> None:
    deliverable_key = str(deliverable.get("key"))
    key = plan.register(file, task.get("key", "?"))
    plan.counts["tasks"] += 1
    repo = task.get("repo") or repo
    plan.nodes[key] = ("Task", {**inherited(epic), **task, "repo": repo, **stage_label(plan, deliverable_key)})
    missing(plan, file, key, task, TASK_FIELDS)
    too_long(plan, file, key, task, 70)
    if not key.startswith(deliverable_key + "/"):
        plan.error(file, key, f"key must start with {deliverable_key}/")
    if task.get("type") not in {"Task", "Bug"}:
        plan.error(file, key, f"type {task.get('type')!r} is not Task or Bug")
    if task.get("effort") not in EFFORTS:
        plan.error(file, key, f"effort {task.get('effort')!r} is not S, M or L")
    if task.get("state") not in {"open", "done"}:
        plan.error(file, key, f"state {task.get('state')!r} is not open or done")
    if task.get("state") == "done" and not task.get("done_by"):
        plan.error(file, key, "state done needs done_by")
    for dep in task.get("depends_on") or []:
        plan.depends.append((file, key, str(dep)))
    for entry in task.get("files") or []:
        if isinstance(entry, dict) and len(entry) == 1 and next(iter(entry)) == "create":
            plan.created.add((repo, str(entry["create"]).rstrip("/")))
    if task.get("state") != "open":
        return
    for field in ("acceptance", "verify", "out_of_scope"):
        if field in task and not task[field]:
            plan.error(file, key, f"{field} is empty")
    for number, step in enumerate(task.get("steps") or [], 1):
        found = BANNED.search(str(step))
        if found:
            plan.error(file, key, f"step {number} uses the banned word {found.group(1)!r}")
    size = len(render(plan.nodes[key][1], "Task")["body"].encode())
    if size > MAX_TASK_BYTES:
        plan.error(file, key, f"rendered body is {size} bytes, the limit is {MAX_TASK_BYTES}")
    task_files = task.get("files") or []
    for entry in task_files:
        if not isinstance(entry, dict) or len(entry) != 1 or next(iter(entry)) not in FILE_OPS:
            plan.error(file, key, f"files entry {entry!r} must be one of edit, create, delete with a path")
    if task.get("verify_only") and task_files:
        plan.error(file, key, "verify_only Task must not list files")
    # edit/delete paths are checked after all Tasks are loaded
    plan.pending.append((file, key, repo, [e for e in task_files if isinstance(e, dict)]))


def finish(plan: Plan, strict: bool = False) -> None:
    repo_roots = {PREFIXES[prefix]: Path(root) for root, prefix in plan.roots.items()}
    for file, key, repo, entries in plan.pending:
        if repo not in repo_roots:
            continue
        for entry in entries:
            for op in ("edit", "delete"):
                if op not in entry:
                    continue
                path = str(entry[op]).rstrip("/")
                if (repo_roots[repo] / path).exists():
                    continue
                parents = {path, *(str(p) for p in Path(path).parents if str(p) != ".")}
                if any((repo, parent) in plan.created for parent in parents):
                    continue
                # A merged PR removes or moves paths before its Task is marked done in the YAML,
                # so in repo_check (not strict) a missing path is a warning, never a red PR.
                message = f"{op} path {path} does not exist in {repo}"
                if strict:
                    plan.error(file, key, message)
                else:
                    plan.warnings.append(f"warning: {file}: {key}: {message}")
    loaded = set(plan.roots.values())
    for file, key, dep in plan.depends:
        prefix = dep.split("/")[0]
        if dep not in plan.keys and prefix in loaded:  # other repos are checked by the sync (all roots)
            plan.error(file, key, f"depends_on {dep} is unknown")


MARKER = "<!-- zohelo:key={key} -->"
NOTES_MARKER = "<!-- zohelo:notes — everything below is free text and is never overwritten by the sync -->"
PRODUCTS = {"zohelo-data": "zohelo-data", "zohelo-bi": "zohelo-bi", "zohelo": "zohelo.com"}


def bullets(items: object, box: bool = False) -> list[str]:
    if isinstance(items, str):
        items = [items]
    return [f"- {'[ ] ' if box else ''}{item}" for item in items or []]


def section(title: str, lines: list[str]) -> list[str]:
    return ["", f"## {title}", *lines] if lines else []


def verify_line(item: object) -> str:
    if not isinstance(item, dict):
        return f"- {item}"
    command = f"`{item['run']}`" if "run" in item else str(item.get("workflow", ""))
    return f"- {command} → {item['expect']}" if item.get("expect") else f"- {command}"


def file_line(entry: object) -> str:
    if isinstance(entry, dict) and len(entry) == 1:
        ((op, path),) = entry.items()
        return f"- {op} `{path}`"
    return f"- {entry}"


def render(
    node: dict,
    kind: str,
    parent_number: int | None = None,
    issue_repo: str | None = None,
    owner_action_due: bool = False,
) -> dict:
    """The issue for one node: {title, body, labels, type, fields} (docs/backlog.md §5, §6).

    kind is Area, Epic, Deliverable or Task (a Task with type Bug is a Bug issue). parent_number is the parent's
    issue number (the parent's key until it exists); issue_repo is the repo the issue lives in (default: the
    node's repo); owner_action_due is set by the sync when every depends_on item of the Task is closed."""
    repo = node.get("repo") or PREFIXES.get(str(node["key"]).split("/")[0], "")
    issue_repo = issue_repo or repo
    parent = f"#{parent_number}" if parent_number else str(node["key"]).rsplit("/", 1)[0]
    facts = [f"**Phase:** {node['phase']}"] if "phase" in node else []
    if kind == "Task":
        facts = [f"**Parent:** {parent}", f"**Effort:** {node['effort']}", *facts]
        facts.append(f"**Contract:** {'yes' if node.get('contract') else 'no'}")
    elif kind in ("Epic", "Deliverable"):
        facts = [f"**Parent:** {parent}" if kind == "Deliverable" else f"**Area:** {node.get('area')}", *facts]
        if "priority" in node:
            facts.append(f"**Priority:** {node['priority']}")
    body = [MARKER.format(key=node["key"])]
    if kind != "Area":
        body.append(" · ".join(facts))
    if kind == "Area":
        body += section("Purpose", [str(node["purpose"]).strip()])
    elif kind == "Epic":
        body += section("Why", [str(node.get("why", "")).strip()])
        body += section("Outcome", [str(node.get("outcome", "")).strip()])
        body += section("In scope", bullets(node.get("in_scope")))
        body += section("Out of scope", bullets(node.get("out_of_scope")))
        body += section("Done when", bullets(node.get("done_when"), box=True))
        body += section("Decisions", bullets(node.get("decisions")))
    elif kind == "Deliverable":
        body += section("Done when", bullets(node.get("done_when"), box=True))
        body += section("Evidence", bullets(node.get("notes")))
        body += section("Not yet written as Tasks", bullets(node.get("todo")))
    else:
        body += section("Context", [str(node.get("context", "")).strip()])
        body += section("Files", [file_line(e) for e in node.get("files") or []])
        body += section("Steps", [f"{n}. {step}" for n, step in enumerate(node.get("steps") or [], 1)])
        body += section("Acceptance", bullets(node.get("acceptance"), box=True))
        body += section("Verify", [verify_line(v) for v in node.get("verify") or []])
        body += section("Out of scope", bullets(node.get("out_of_scope")))
        body += section("Owner action", [str(node["owner_action"]).strip()] if node.get("owner_action") else [])
        body += section("Notes", bullets(node.get("notes")))
        closes = "Closes #<n>" if repo == issue_repo else f"Closes zohelo/{issue_repo}#<n>"
        body += section(
            "How to deliver",
            [
                "Branch `<type>/<number>-<topic>` (feat, fix, chore, docs) from `main`. "
                "Read `AGENTS.md`, then this issue, nothing else first.",
                f"Open one PR titled like this issue with `{closes}` in the body. "
                "Stop and comment here if any step is impossible as written; do not improvise.",
            ],
        )
    body.append(NOTES_MARKER)
    labels = []
    if node.get("contract"):
        labels.append("contract")
    if node.get("owner_action") and owner_action_due:
        labels.append("owner:action")
    if node.get("verify_only"):
        labels.append("verify-only")
    if node.get("source"):
        labels.append(f"source:{node['source']}")
    if node.get("stage"):
        labels.append(f"stage:{node['stage']}")
    fields = {"Product": PRODUCTS.get(repo, repo)}
    for name, field in (("Phase", "phase"), ("Priority", "priority"), ("Effort", "effort"), ("Target date", "target")):
        if node.get(field) is not None:
            fields[name] = str(node[field])
    issue_type = node.get("type", "Task") if kind == "Task" else kind
    return {
        "title": node["title"],
        "body": "\n".join(body) + "\n",
        "labels": labels,
        "type": issue_type,
        "fields": fields,
    }


def build(roots: list[Path], strict: bool = False) -> Plan:
    plan = Plan()
    for root in roots:
        load(plan, root)
    for root, file, epic in plan.epics:
        check_epic(plan, root, file, epic)
    finish(plan, strict)
    return plan


def validate(roots: list[Path], strict: bool = False) -> int:
    plan = build(roots, strict)
    for warning in plan.warnings:
        print(warning)
    for error in plan.errors:
        print(error)
    if plan.errors:
        return 1
    c = plan.counts
    print(f"ok: {c['areas']} areas, {c['epics']} epics, {c['deliverables']} deliverables, {c['tasks']} tasks")
    return 0


def show(roots: list[Path], key: str) -> int:
    plan = build(roots)
    if key not in plan.nodes:
        print(f"error: {key} is not in the loaded backlog files", file=sys.stderr)
        return 2
    kind, node = plan.nodes[key]
    issue = render(node, kind)
    print(f"title: {issue['title']}")
    print(f"type: {issue['type']}")
    print(f"labels: {', '.join(issue['labels'])}")
    print(f"fields: {issue['fields']}")
    print()
    print(issue["body"], end="")
    return 0


LINK = re.compile(
    r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s*:?\s+"
    r"(?:(?:https://github\.com/)?(?P<repo>zohelo/[\w.-]+)(?:/issues/|#)|#)(?P<num>\d+)\b",
    re.IGNORECASE,
)
LINK_HELP = "Link an open Task or Bug: add Closes #<n> to the PR body, then re-run this check"


def fetch_issue(repo: str, number: str, token: str) -> dict | None:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/issues/{number}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as err:
        print(f"{repo}#{number}: GitHub answered {err.code}", file=sys.stderr)
        return None


def pr_check() -> int:
    """Every pull request closes an open Task or Bug (docs/backlog.md rule 2); Dependabot is exempt."""
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as handle:
        pull = json.load(handle).get("pull_request") or {}
    if pull.get("user", {}).get("login") == "dependabot[bot]":
        print("::notice title=Linked task::skipped, Dependabot pull request")
        return 0
    here = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GITHUB_TOKEN", "")
    for match in LINK.finditer(pull.get("body") or ""):
        repo, number = match.group("repo") or here, match.group("num")
        issue = fetch_issue(repo, number, token)
        if not issue or "pull_request" in issue or issue.get("state") != "open":
            continue
        if (issue.get("type") or {}).get("name") not in {"Task", "Bug"}:
            continue
        if any(label.get("name") == "verify-only" for label in issue.get("labels", [])):
            continue
        print(f"Linked task: ok #{number}")
        return 0
    print(f"::error title=Linked task::{LINK_HELP}")
    print(LINK_HELP, file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    check = modes.add_parser("validate", help="check the backlog files")
    check.add_argument("--root", action="append", type=Path, help="repository root (repeatable; default: this one)")
    check.add_argument(
        "--strict", action="store_true", help="missing edit/delete paths are errors (backlog sync), not warnings"
    )
    show_mode = modes.add_parser("render", help="print the issue title, labels and body of one node")
    show_mode.add_argument("key")
    show_mode.add_argument("--root", action="append", type=Path, help="repository root (repeatable; default: this one)")
    modes.add_parser("pr-check", help="check that the pull request closes an open Task or Bug")
    args = parser.parse_args()
    if args.mode == "pr-check":
        return pr_check()
    roots = [r.resolve() for r in args.root] if args.root else [ROOT]
    return validate(roots, args.strict) if args.mode == "validate" else show(roots, args.key)


if __name__ == "__main__":
    sys.exit(main())
