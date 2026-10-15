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
FORBIDDEN_SUFFIXES = {
    ".parquet",
    ".ipynb",
    ".csv",
    ".tsv",
    ".db",
    ".duckdb",
    ".sqlite",
    ".xls",
    ".xlsx",
    ".zip",
    ".gz",
    ".bz2",
    ".xz",
    ".tar",
    ".7z",
    ".pem",
    ".key",
    ".p12",
}
MAX_BYTES = 1_000_000
USES_LINE = re.compile(r"^\s*-?\s*uses:")
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
        if not (ROOT / f).exists():  # deleted locally, not yet committed
            continue
        size = (ROOT / f).stat().st_size
        if size > MAX_BYTES:
            problems.append(f"{f}: {size:,} bytes is above 1 MB")
        if size == 0 and f.name != "__init__.py":
            problems.append(f"{f}: empty file")
        if f.suffix.lower() in FORBIDDEN_SUFFIXES:
            problems.append(f"{f}: {f.suffix} files are not committed")
        if f.name.startswith(".env") and f.name != ".env.example":
            problems.append(f"{f}: environment files are not committed (only .env.example)")
    return problems


def workflows(files: list[Path]) -> list[str]:
    """Every action in workflows and local actions is pinned by a full commit SHA with a version comment."""
    problems = []
    for f in files:
        if f.parts[:2] != (".github", "workflows") and not (
            f.parts[:2] == (".github", "actions") and f.stem == "action"
        ):
            continue
        for number, line in enumerate((ROOT / f).read_text().splitlines(), 1):
            if USES_LINE.match(line) and not PINNED_ACTION.search(line):
                problems.append(f"{f}:{number}: action not pinned by SHA with a version comment")
    return problems


SQLFLUFF = "sqlfluff==4.4.0"  # pinned here so the workflow does not change when the version does


def sqlfluff() -> list[str]:
    """dbt models pass sqlfluff (settings in pyproject.toml), with the pinned version."""
    from importlib import metadata

    try:
        installed = metadata.version("sqlfluff")
    except metadata.PackageNotFoundError:
        installed = None
    if installed != SQLFLUFF.split("==")[1]:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", SQLFLUFF], capture_output=True, text=True
        )
        if result.returncode:
            return [f"could not install {SQLFLUFF}: {result.stderr.strip()[-500:]}"]
    return tool(
        [sys.executable, "-m", "sqlfluff", "lint", "warehouse/models", "--processes", "2", "--disable-progress-bar"]
    )


def tool(command: list[str]) -> list[str]:
    """Run a linter; its output is the finding."""
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    if result.returncode == 0:
        return []
    return [(result.stdout + result.stderr).strip()[-6000:] or f"{command[0]} failed"]


def configuration(files: list[Path]) -> list[str]:
    """Every YAML parses; sources, runs and schedules are consistent (run.py)."""
    sys.path.insert(0, str(ROOT))
    from ingestion.common import scheduler
    import run

    problems = []
    for f in files:
        if f.suffix in (".yml", ".yaml") and (ROOT / f).exists():
            try:
                yaml.safe_load((ROOT / f).read_text())
            except yaml.YAMLError as exc:
                problems.append(f"{f}: {exc}")
    if problems:
        return problems
    problems += register(files)
    config = run.load_config()
    problems += run.config_problems(config)
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


SCHEDULER_PACKAGES = {"pyyaml", "requests"}  # what the scheduler job in pipeline.yml installs
IMPORT_NAMES = {"dbt-core": "dbt", "dbt-duckdb": "dbt"}
BLOCKER = """
import runpy, sys
class Block:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in BLOCKED:
            raise ImportError("the scheduler job does not install " + name)
sys.meta_path.insert(0, Block())
sys.argv = ["run.py", "schedule", "--check"]
runpy.run_path("run.py", run_name="__main__")
"""


def scheduler_imports() -> list[str]:
    """`run.py schedule` works with only the packages the scheduler job installs (#788 broke it)."""
    names = [re.split(r"[=<>\[ ]", line.strip())[0] for line in (ROOT / "requirements.txt").read_text().splitlines()]
    blocked = {IMPORT_NAMES.get(n, n) for n in names if n and not n.startswith("#") and n not in SCHEDULER_PACKAGES}
    result = subprocess.run(
        [sys.executable, "-c", f"BLOCKED = {sorted(blocked)!r}\n{BLOCKER}"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={"PATH": "", "PYTHONPATH": str(ROOT)},
    )
    if result.returncode == 0:
        return []
    return [f"run.py schedule --check fails without {sorted(blocked)}: {result.stderr.strip().splitlines()[-1:]}"]


STATUSES = {"registered", "landing", "bronze", "silver"}


def register(files: list[Path]) -> list[str]:
    """The source register (ingestion/register.yaml) and the source declarations agree."""
    entries = yaml.safe_load((ROOT / "ingestion/register.yaml").read_text())["sources"]
    problems, ids = [], [e.get("id") for e in entries]
    problems += [f"register: duplicate id {i}" for i in sorted({i for i in ids if ids.count(i) > 1})]
    problems += [
        f"register: {e.get('id')}: status {e.get('status')!r}" for e in entries if e.get("status") not in STATUSES
    ]
    linked = {e["source_system"] for e in entries if e.get("source_system")}
    declared = {f.stem for f in files if f.parent == Path("ingestion/sources") and f.suffix == ".yaml"}
    problems += [f"register: source_system {s} has no ingestion/sources/{s}.yaml" for s in sorted(linked - declared)]
    problems += [f"register: ingestion/sources/{s}.yaml is not in the register" for s in sorted(declared - linked)]
    return problems


def main() -> int:
    files = tracked()
    checks = {
        "repository": lambda: repository(files),
        "workflows": lambda: workflows(files),
        "compile": lambda: tool([sys.executable, "-m", "compileall", "-q", *PYTHON_PATHS]),
        "ruff check": lambda: tool(["ruff", "check", *PYTHON_PATHS]),
        "ruff format": lambda: tool(["ruff", "format", "--check", *PYTHON_PATHS]),
        "yamllint": lambda: tool(["yamllint", "-s", "-d", YAMLLINT, "ingestion", "warehouse", "portal", ".github"]),
        "sqlfluff": sqlfluff,
        "configuration": lambda: configuration(files),
        "scheduler imports": scheduler_imports,
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
