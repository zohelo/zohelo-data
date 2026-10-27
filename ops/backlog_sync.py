#!/usr/bin/env python3
"""Backlog engine (docs/backlog.md): checks and renders the plan files in `.github/backlog/`.

Standard library plus pyyaml only, because the validator also runs in zohelo-bi and zohelo.
Mode `validate [--root DIR ...]` loads areas.yaml, stages.yaml and epics/*.yaml of every root
(default: the repository root), prints `file: key: message` per error and exits 1 on any error.
Mode `render KEY [--root DIR ...]` prints the title, labels and body of the issue for one node (docs/backlog.md §5).
Mode `pr-check` (pull_request event, GITHUB_TOKEN) passes when the PR body closes an open Task or Bug.
Modes `plan` and `apply [--root DIR ...]` compare the nodes with the issues of zohelo-data, zohelo-bi and zohelo
(matched by the key in the first body line). plan only reads GitHub and reports the counts per repo as annotations;
apply creates, updates, re-parents and closes issues (at most 400 writes per run). The token comes from
GH_PROJECT_TOKEN, GITHUB_TOKEN or GH_TOKEN.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
import time
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
REGISTER = "ingestion/register.yaml"
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
        self.sources = {"file": 0, "generated": 0, "no_family": []}  # register entries by how their Epic is made

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
    if (base / "stages.yaml").exists() and (root / REGISTER).exists():
        load_sources(plan, root, read)


def fill(text: object, values: dict[str, str]) -> object:
    """Replace the {placeholders} of stages.yaml in a string, leaving every other brace as written."""
    if isinstance(text, str):
        return re.sub(r"\{(\w+)\}", lambda m: values.get(m.group(1), m.group(0)), text)
    if isinstance(text, list):
        return [fill(item, values) for item in text]
    if isinstance(text, dict):
        return {k: fill(v, values) for k, v in text.items()}
    return text


def source_epic(entry: dict, stages: list[dict]) -> dict:
    """The Epic of a register entry without a hand-written file: stage 1 with its Tasks, stage 2 as a stub."""
    ident = str(entry["id"])
    low = ident.lower()
    key = f"data/src-{low}"
    short = str(entry.get("short_name") or str(entry.get("name", ident))[:40])
    values = {
        "id": ident,
        "id_lower": low,
        "short_name": short,
        "source_system": str(entry.get("source_system") or low),
        "family_area": f"data/src-{str(entry['family']).lower()}",
        "register_file": REGISTER,
    }
    deliverables = []
    for stage in stages:
        if stage.get("n") not in (1, 2):
            continue
        item = {
            "key": f"{key}/{stage['key']}",
            "title": fill(stage["title"], values),
            "repo": stage.get("repo", "zohelo-data"),
            "done_when": fill(stage["done_when"], values),
        }
        if stage["n"] == 1:
            tasks = []
            for template in stage.get("tasks") or []:
                task = fill({k: v for k, v in template.items() if k != "key"}, values)
                task = {"key": f"{item['key']}/{template['key']}", "type": "Task", "state": "open", **task}
                task["depends_on"] = [f"{item['key']}/{dep}" for dep in task.get("depends_on") or []]
                tasks.append(task)
            item["tasks"] = tasks
        else:
            item["todo"] = [
                f"Tasks are rendered when stage 1 ({key}/review) is done and the register status is approved"
            ]
        deliverables.append(item)
    return {
        "key": key,
        "area": values["family_area"],
        "title": f"{short} [{ident}]",
        "phase": 3,
        "priority": "P3",
        "why": f"{entry.get('name', ident)} ({entry.get('provider', 'unknown provider')}) is a register candidate.",
        "outcome": "The source is reviewed, then onboarded through the standard stages or parked with the reason.",
        "in_scope": [
            "Stages 1 and 2 of the source template (stages.yaml); later stages follow when the predecessor is done"
        ],
        "out_of_scope": ["Other sources of the same provider"],
        "done_when": [
            f"Every Deliverable of this Epic is closed; register entry {ident} is approved and landed, or parked"
        ],
        "depends_on": [],
        "decisions": [],
        "deliverables": deliverables,
    }


def load_sources(plan: Plan, root: Path, read) -> None:
    """Add a generated Epic for every register entry that has no epics/src-<id lower>.yaml (docs/backlog.md §3)."""
    register = read(root / REGISTER)
    stages = read(root / ".github" / "backlog" / "stages.yaml")
    entries = (register or {}).get("sources") if isinstance(register, dict) else None
    if not entries or not isinstance(stages, dict):
        return
    files = {p.stem for p in (root / ".github" / "backlog" / "epics").glob("src-*.yaml")}
    for entry in entries:
        ident = str(entry.get("id", "?"))
        if f"src-{ident.lower()}" in files:
            plan.sources["file"] += 1
        elif not entry.get("family"):
            plan.sources["no_family"].append(ident)
        else:
            plan.sources["generated"] += 1
            plan.epics.append((str(root), f"{REGISTER} (generated)", source_epic(entry, stages.get("stages") or [])))


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
    parent_number: int | str | None = None,
    issue_repo: str | None = None,
    owner_action_due: bool = False,
) -> dict:
    """The issue for one node: {title, body, labels, type, fields} (docs/backlog.md §5, §6).

    kind is Area, Epic, Deliverable or Task (a Task with type Bug is a Bug issue). parent_number is the parent's
    issue number (the parent's key until it exists); issue_repo is the repo the issue lives in (default: the
    node's repo); owner_action_due is set by the sync when every depends_on item of the Task is closed."""
    repo = node.get("repo") or PREFIXES.get(str(node["key"]).split("/")[0], "")
    issue_repo = issue_repo or repo
    parent = str(node["key"]).rsplit("/", 1)[0]
    if parent_number:
        parent = parent_number if isinstance(parent_number, str) else f"#{parent_number}"
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
    found = plan.sources
    if found["no_family"]:
        print(f"warning: register entries without family, no Epic generated: {', '.join(found['no_family'])}")
    print(f"file sources: {found['file']}")
    print(f"generated sources: {found['generated']}")
    print(f"register entries without family: {len(found['no_family'])}")
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


# --- sync: plan and apply (docs/backlog.md §1) ---------------------------------------------------------------

REST = "https://api.github.com"
ORG = "zohelo"
KEY_LINE = re.compile(r"^<!-- zohelo:key=(\S+) -->")
NOTES_PREFIX = "<!-- zohelo:notes"
LAYERS = ("Area", "Epic", "Deliverable", "Task")
WRITE_LIMIT = 400
OWNER_ACTION_LIMIT = 3


class Stop(Exception):
    """The write limit of one run is reached; a rerun continues."""


class Github:
    """The few GitHub REST calls of the sync. With dry set, writes are counted and not sent."""

    def __init__(self, token: str, dry: bool) -> None:
        import requests

        self.http = requests.Session()
        self.http.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        self.dry = dry
        self.writes = 0

    def call(self, method: str, path: str, body: dict | None = None, ok: tuple[int, ...] = (200, 201, 204)):
        for attempt in range(4):
            response = self.http.request(method, REST + path, json=body, timeout=60)
            if response.status_code in (429, 502, 503) or (
                response.status_code == 403 and "retry-after" in response.headers
            ):
                time.sleep(min(int(response.headers.get("retry-after", 2 ** (attempt + 1))), 60))
                continue
            break
        if response.status_code == 404 and method == "GET":
            return None
        if response.status_code not in ok:
            raise RuntimeError(f"{method} {path}: HTTP {response.status_code} {response.text[:200]}")
        return response.json() if response.content else {}

    def read(self, path: str):
        return self.call("GET", path)

    def write(self, method: str, path: str, body: dict | None = None):
        if self.dry:
            return {"id": 0, "number": 0}
        if self.writes >= WRITE_LIMIT:
            raise Stop
        self.writes += 1
        result = self.call(method, path, body)
        time.sleep(1)
        return result


def index_issues(api: Github, repos: list[str]) -> dict[str, dict]:
    """key -> issue (with its repo) from the first body line of every issue of the repos."""
    found: dict[str, dict] = {}
    for repo in repos:
        page = 1
        while True:
            items = api.read(f"/repos/{ORG}/{repo}/issues?state=all&per_page=100&page={page}") or []
            for item in items:
                match = KEY_LINE.match((item.get("body") or "").split("\n", 1)[0])
                if "pull_request" not in item and match:
                    found.setdefault(match.group(1), {**item, "repo": repo})
            if len(items) < 100:
                break
            page += 1
    return found


def unadopted(root: Path, issues: dict[str, dict]) -> list[str]:
    """Keys that migration.yaml adopts for an old issue that carries no key marker yet."""
    import yaml

    path = root / ".github" / "backlog" / "migration.yaml"
    if not path.exists():
        return []
    entries = (yaml.safe_load(path.read_text()) or {}).get("issues") or []
    return [
        str(e["key"]) for e in entries if e.get("action") == "adopt" and e.get("key") and str(e["key"]) not in issues
    ]


def parent_key(kind: str, node: dict) -> str | None:
    if kind == "Area":
        return None
    return str(node["area"]) if kind == "Epic" else str(node["key"]).rsplit("/", 1)[0]


def managed(label: str) -> bool:
    return label in {"contract", "owner:action", "verify-only"} or label.startswith(("source:", "stage:"))


def split_notes(body: str) -> tuple[str, str]:
    """The sync's part of an issue body and the free text from the notes marker on (kept as it is)."""
    lines = body.split("\n")
    for number, line in enumerate(lines):
        if line.startswith(NOTES_PREFIX):
            return "\n".join(lines[:number]), "\n".join(lines[number:])
    return "\n".join(lines[:1]), NOTES_MARKER + "\n\n" + "\n".join(lines[1:]).strip("\n")


def run_sync(roots: list[Path], apply: bool) -> int:
    from ingestion.common.report import annotate

    plan = build(roots)
    for error in plan.errors:
        print(error)
    if plan.errors:
        return 1
    token = os.environ.get("GH_PROJECT_TOKEN") or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("error: no GitHub token (GH_PROJECT_TOKEN, GITHUB_TOKEN or GH_TOKEN)", file=sys.stderr)
        return 2
    api = Github(token, dry=not apply)
    loaded = {PREFIXES[prefix] for prefix in plan.roots.values()}
    issues = index_issues(api, list(PREFIXES.values()))
    blocked = [k for k in unadopted(roots[0], issues) if k in plan.nodes]
    verb = "refused" if apply else "plan blocked"
    if blocked:
        annotate(
            "error",
            f"backlog_sync {verb}",
            {"adopt keys without a marked issue": sorted(set(blocked))[:40], "count": len(set(blocked))},
        )
        if apply:
            return 1
    nodes = sorted(plan.nodes.items(), key=lambda item: LAYERS.index(item[1][0]))
    counts = {repo: {"create": 0, "update": 0, "reparent": 0, "close": 0} for repo in PREFIXES.values()}
    owner_open = sum(
        1 for i in issues.values() if i["state"] == "open" and any(n["name"] == "owner:action" for n in i["labels"])
    )
    try:
        for key, (kind, node) in nodes:
            repo = node["repo"]
            if repo not in loaded:
                continue
            parent = parent_key(kind, node)
            parent_issue = issues.get(parent) if parent else None
            ref = None
            if parent_issue:
                ref = (
                    parent_issue["number"]
                    if parent_issue["repo"] == repo
                    else f"{ORG}/{parent_issue['repo']}#{parent_issue['number']}"
                )
            due = bool(node.get("depends_on")) and all(
                issues.get(str(d), {}).get("state") == "closed" for d in node["depends_on"]
            )
            want = render(node, kind, ref, repo, due)
            existing = issues.get(key)
            finished = node.get("state") == "done" and kind in ("Task", "Deliverable")
            if existing is None:
                counts[repo]["create"] += 1
                created = api.write(
                    "POST",
                    f"/repos/{ORG}/{repo}/issues",
                    {"title": want["title"], "body": want["body"], "labels": want["labels"], "type": want["type"]},
                )
                existing = issues[key] = {
                    **created,
                    "repo": repo,
                    "state": "open",
                    "labels": [{"name": n} for n in want["labels"]],
                }
                owner_open += "owner:action" in want["labels"]
                if finished:
                    close_issue(api, counts, repo, existing, completed_comment(node, kind), "completed")
            else:
                head, tail = split_notes(existing.get("body") or "")
                have = [item["name"] for item in existing["labels"]]
                labels = [name for name in have if not managed(name)] + want["labels"]
                current_type = (existing.get("type") or {}).get("name")
                if (head.rstrip(), existing["title"], sorted(have), current_type) != (
                    want["body"].split(NOTES_MARKER)[0].rstrip(),
                    want["title"],
                    sorted(labels),
                    want["type"],
                ):
                    counts[repo]["update"] += 1
                    api.write(
                        "PATCH",
                        f"/repos/{ORG}/{repo}/issues/{existing['number']}",
                        {
                            "title": want["title"],
                            "labels": labels,
                            "type": want["type"],
                            "body": want["body"].split(NOTES_MARKER)[0] + tail,
                        },
                    )
                    owner_open += ("owner:action" in labels) - ("owner:action" in have)
                if finished and existing["state"] == "open":
                    close_issue(api, counts, repo, existing, completed_comment(node, kind), "completed")
            if parent_issue is not None:
                current = (
                    api.read(f"/repos/{ORG}/{repo}/issues/{existing['number']}/parent") if existing["number"] else None
                )
                if (current or {}).get("number") != parent_issue["number"] or (current or {}).get(
                    "repository_url", ""
                ).split("/")[-1] != parent_issue["repo"]:
                    counts[repo]["reparent"] += 1
                    api.write(
                        "POST",
                        f"/repos/{ORG}/{parent_issue['repo']}/issues/{parent_issue['number']}/sub_issues",
                        {"sub_issue_id": existing["id"], "replace_parent": True},
                    )
        for key, issue in issues.items():
            if key not in plan.nodes and issue["state"] == "open" and issue["repo"] in loaded:
                close_issue(api, counts, issue["repo"], issue, "Removed from the plan", "not_planned")
    except Stop:
        annotate("warning", "backlog_sync stopped", {"writes": WRITE_LIMIT, "note": "limit reached; rerun continues"})
    labels = (
        ("created", "updated", "reparented", "closed")
        if apply
        else ("to create", "to update", "to reparent", "to close")
    )
    for repo, found in counts.items():
        if repo in loaded:
            annotate(
                "notice",
                f"backlog_sync {'apply' if apply else 'plan'} {repo}",
                dict(zip(labels, found.values(), strict=True)),
            )
        else:
            annotate("notice", f"backlog_sync {repo}", {"skipped": "root not present"})
    if owner_open > OWNER_ACTION_LIMIT:
        annotate(
            "warning",
            "backlog_sync owner actions",
            {"open owner:action issues": owner_open, "limit": OWNER_ACTION_LIMIT},
        )
    return 0


def completed_comment(node: dict, kind: str) -> str:
    if kind == "Task":
        return f"Done by {node['done_by']}"
    return "Done. Evidence:\n" + "\n".join(f"> {n}" for n in node.get("notes") or [])


def close_issue(api: Github, counts: dict, repo: str, issue: dict, comment: str, reason: str) -> None:
    counts[repo]["close"] += 1
    base = f"/repos/{ORG}/{repo}/issues/{issue['number']}"
    api.write("POST", base + "/comments", {"body": comment})
    api.write("PATCH", base, {"state": "closed", "state_reason": reason})
    issue["state"] = "closed"


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
    for name in ("plan", "apply"):
        sync_mode = modes.add_parser(
            name,
            help="compare the backlog files with the issues" + (" and write" if name == "apply" else ", read-only"),
        )
        sync_mode.add_argument(
            "--root", action="append", type=Path, help="repository root (repeatable; default: this one and _repos/*)"
        )
    args = parser.parse_args()
    if args.mode == "pr-check":
        return pr_check()
    roots = [r.resolve() for r in args.root] if args.root else [ROOT]
    if args.mode in ("plan", "apply"):
        if not args.root:
            roots += [d for d in (ROOT / "_repos" / "zohelo-bi", ROOT / "_repos" / "zohelo") if d.is_dir()]
        return run_sync(roots, args.mode == "apply")
    return validate(roots, args.strict) if args.mode == "validate" else show(roots, args.key)


if __name__ == "__main__":
    sys.exit(main())
