#!/usr/bin/env python3
"""Backlog engine (docs/backlog.md): checks the plan files in `.github/backlog/`.

Standard library plus pyyaml only, because the validator also runs in zohelo-bi and zohelo.
Mode `validate [--root DIR ...]` loads areas.yaml, stages.yaml and epics/*.yaml of every root
(default: the repository root), prints `file: key: message` per error and exits 1 on any error.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

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
        missing(plan, str(areas_file), key, area, ["key", "title", "purpose"])
        too_long(plan, str(areas_file), key, area, 60)
        plan.counts["areas"] += 1
    if (base / "stages.yaml").exists():
        read(base / "stages.yaml")
    for path in sorted((base / "epics").glob("*.yaml")):
        epic = read(path)
        if isinstance(epic, dict):
            plan.epics.append((str(root), str(path), epic))
        elif epic is not None:
            plan.error(str(path), "-", "an epic file must be a mapping")


def check_epic(plan: Plan, root: str, file: str, epic: dict) -> None:
    key = plan.register(file, epic.get("key", "?"))
    plan.counts["epics"] += 1
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
        check_deliverable(plan, root, file, key, deliverable)


def check_deliverable(plan: Plan, root: str, file: str, epic_key: str, item: dict) -> None:
    key = plan.register(file, item.get("key", "?"))
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
    repo = item.get("repo") or PREFIXES.get(key.split("/")[0], "")
    for task in tasks:
        check_task(plan, root, file, key, repo, task)


def check_task(plan: Plan, root: str, file: str, deliverable_key: str, repo: str, task: dict) -> None:
    key = plan.register(file, task.get("key", "?"))
    plan.counts["tasks"] += 1
    repo = task.get("repo") or repo
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
    import yaml

    size = len(yaml.dump(task, allow_unicode=True).encode())
    if size > MAX_TASK_BYTES:
        plan.error(file, key, f"rendered body about {size} bytes, the limit is {MAX_TASK_BYTES}")
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
        if dep not in plan.keys and not (prefix in {"bi", "com"} and prefix not in loaded):
            plan.error(file, key, f"depends_on {dep} is unknown")


def validate(roots: list[Path], strict: bool = False) -> int:
    plan = Plan()
    for root in roots:
        load(plan, root)
    for root, file, epic in plan.epics:
        check_epic(plan, root, file, epic)
    finish(plan, strict)
    for warning in plan.warnings:
        print(warning)
    for error in plan.errors:
        print(error)
    if plan.errors:
        return 1
    c = plan.counts
    print(f"ok: {c['areas']} areas, {c['epics']} epics, {c['deliverables']} deliverables, {c['tasks']} tasks")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    check = modes.add_parser("validate", help="check the backlog files")
    check.add_argument("--root", action="append", type=Path, help="repository root (repeatable; default: this one)")
    check.add_argument(
        "--strict", action="store_true", help="missing edit/delete paths are errors (backlog sync), not warnings"
    )
    args = parser.parse_args()
    return validate([r.resolve() for r in args.root] if args.root else [ROOT], args.strict)


if __name__ == "__main__":
    sys.exit(main())
