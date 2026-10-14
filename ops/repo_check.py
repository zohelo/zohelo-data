#!/usr/bin/env python3
"""Check a change against the [auto] rules of docs/quality.md (no credentials needed).

The `check` job runs this on every pull request; anyone can run it locally before
pushing. Each check prints its findings; the exit code is 1 when any check fails.
dbt parse and the secret scan run as their own steps of the job.
"""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys

import yaml

ROOT = Path(__file__).resolve().parent.parent
ROOT_ALLOWED = {
    ".github",
    "docs",
    "ingestion",
    "ops",
    "portal",
    "warehouse",
    "run.py",
    "requirements.txt",
    "pyproject.toml",
    "README.md",
    "AGENTS.md",
    "CLAUDE.md",
    "GEMINI.md",
    ".gitignore",
    ".gitattributes",
    ".python-version",
    ".node-version",
}
WORKFLOWS = ["deploy-portal.yml", "dev.yml", "ops.yml", "pipeline.yml"]
FORBIDDEN_SUFFIXES = {".parquet", ".ipynb", ".zip", ".gz", ".7z", ".xlsx", ".pem", ".key", ".env"}
MAX_BYTES = 1_000_000
PINNED_ACTION = re.compile(r"uses:\s*(\./\S+|[\w./-]+@[0-9a-f]{40}\s+#\s*v\S+)")
PYTHON_PATHS = ["ingestion", "ops", "warehouse", "run.py", "portal/publish_index.py"]
# Long description lines are prose for the reader; they are not wrapped.
YAMLLINT = (
    "{extends: default, ignore: [portal/app/node_modules/], rules: {line-length: disable, "
    "indentation: {indent-sequences: whatever}, document-start: disable, truthy: {check-keys: false}, "
    "comments: {min-spaces-from-content: 1}}}"
)


def tracked() -> list[Path]:
    """Files tracked by git (what a merge would put on main)."""
    out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True)
    return [Path(line) for line in out.stdout.splitlines()]


def repository(files: list[Path]) -> list[str]:
    """Root allowlist, no dev/, the four workflows, file size, type and emptiness."""
    problems = [f"root: {name} is not allowed" for name in sorted({f.parts[0] for f in files} - ROOT_ALLOWED)]
    if any(f.parts[0] == "dev" for f in files):
        problems.append("dev/ must be deleted before merge")
    workflows = sorted(f.name for f in files if f.parent == Path(".github/workflows"))
    if workflows != WORKFLOWS:
        problems.append(f"workflows must be exactly {WORKFLOWS}, found {workflows}")
    for f in files:
        size = (ROOT / f).stat().st_size
        if size > MAX_BYTES:
            problems.append(f"{f}: {size:,} bytes is above 1 MB")
        if size == 0 and f.name != "__init__.py":
            problems.append(f"{f}: empty file")
        if f.suffix.lower() in FORBIDDEN_SUFFIXES:
            problems.append(f"{f}: {f.suffix} files are not committed")
    return problems


def workflows() -> list[str]:
    """Every action is pinned by a full commit SHA with a version comment."""
    problems = []
    for path in sorted((ROOT / ".github/workflows").glob("*.yml")):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if "uses:" in line and not PINNED_ACTION.search(line):
                problems.append(f"{path.name}:{number}: action not pinned by SHA with a version comment")
    return problems


def tool(command: list[str]) -> list[str]:
    """Run a linter; its output is the finding."""
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    if result.returncode == 0:
        return []
    return [(result.stdout + result.stderr).strip()[-6000:] or f"{command[0]} failed"]


def configuration() -> list[str]:
    """Every YAML parses; sources, runs and schedules are consistent (run.py)."""
    sys.path.insert(0, str(ROOT))
    from ingestion.common import scheduler
    import run

    for path in sorted(ROOT.rglob("*.y*ml")):
        if "node_modules" not in path.parts:
            yaml.safe_load(path.read_text())
    config = run.load_config()
    problems = list(run.config_problems(config))
    for source in [*config["sources"], "platform"]:
        missing = set(run.authorized(config, source)) - set(run.implemented(source))
        if missing:
            problems.append(f"{source}: authorized but not implemented: {sorted(missing)}")
    for name, activities in run.RUNS.items():
        unknown = set(activities) - set(run.ORDER)
        if unknown:
            problems.append(f"run {name!r}: unknown activities {sorted(unknown)}")
    problems += scheduler.validate(config, run.plan)
    return problems


def main(argv: list[str] | None = None) -> int:
    files = tracked()
    checks = {
        "repository": lambda: repository(files),
        "workflows": workflows,
        "compile": lambda: tool([sys.executable, "-m", "compileall", "-q", *PYTHON_PATHS]),
        "ruff check": lambda: tool(["ruff", "check", *PYTHON_PATHS]),
        "ruff format": lambda: tool(["ruff", "format", "--check", *PYTHON_PATHS]),
        "yamllint": lambda: tool(["yamllint", "-s", "-d", YAMLLINT, "ingestion", "warehouse", "portal", ".github"]),
        "configuration": configuration,
    }
    failed = 0
    for name, check in checks.items():
        problems = check()
        print(f"{'FAIL' if problems else 'ok  '} {name}", flush=True)
        for problem in problems:
            print("     " + problem.replace("\n", "\n     "), flush=True)
        failed += bool(problems)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
