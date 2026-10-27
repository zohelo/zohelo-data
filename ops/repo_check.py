#!/usr/bin/env python3
"""Check a change against the [auto] rules of docs/quality.md (no credentials needed).

The `check` job runs this on every pull request; anyone can run it locally before
pushing. Each check prints its findings; the exit code is 1 when any check fails.
dbt parse and the secret scan run as their own steps of the job.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

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
    "requirements.in",
    "requirements.txt",
    "pyproject.toml",
    "README.md",
    "AGENTS.md",
    "CLAUDE.md",
    ".gitignore",
    ".gitleaks.toml",
    ".gitattributes",
    ".python-version",
    ".node-version",
}
WORKFLOWS = ["branch-cleanup.yml", "deploy-portal.yml", "dev.yml", "ops.yml", "pipeline.yml"]
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
PYTHON_PATHS = ["ingestion", "ops", "warehouse", "run.py", "portal/publish_index.py", "portal/cloudflare_setup.py"]
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
    return problems + alert_job()


def alert_job() -> list[str]:
    """pipeline.yml's notify job waits for every other job and sees none of the workflow-level secrets."""
    workflow = yaml.safe_load((ROOT / ".github/workflows/pipeline.yml").read_text())
    jobs = workflow["jobs"]
    notify = jobs.get("notify") or {}
    problems = [
        f"pipeline.yml: notify.needs misses {job}"
        for job in sorted(set(jobs) - {"notify"} - set(notify.get("needs") or []))
    ]
    secrets = sorted(k for k, v in (workflow.get("env") or {}).items() if "secrets." in str(v))
    exposed = [k for k in secrets if (notify.get("env") or {}).get(k, None) != ""]
    if exposed:
        problems.append(f"pipeline.yml: notify job must blank the workflow-level secrets {exposed}")
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
    problems += scheduler_worker(scheduler.DISPATCH_RUN)
    problems += full_rebuild(files)
    return problems


INCREMENTAL = re.compile(r"materialized\s*=\s*['\"]incremental['\"]")
POLICY_ROWS = 1_000_000  # owner policy #822: only tables below ~1M rows are rebuilt in full
EXCEPTION = re.compile(r"^#\d+$")


def full_rebuild(files: list[Path]) -> list[str]:
    """Every dbt model, SQL or Python, that is not incremental (the project default is a full-rebuild table)
    has a row ceiling in warehouse/warehouse.yaml `full_rebuild`; a ceiling above 1M rows
    names the issue that makes it incremental or records the owner's exception (#868)."""
    declared = yaml.safe_load((ROOT / "warehouse/warehouse.yaml").read_text()).get("full_rebuild") or {}
    models = {
        f.stem: INCREMENTAL.search((ROOT / f).read_text()) is not None
        for f in files
        if f.parts[:2] == ("warehouse", "models") and f.suffix in (".sql", ".py") and (ROOT / f).exists()
    }
    problems = [
        f"warehouse.yaml full_rebuild: model {name} is not incremental and has no row ceiling"
        for name, incremental in sorted(models.items())
        if not incremental and name not in declared
    ]
    for name, entry in sorted(declared.items()):
        if name not in models:
            problems.append(f"warehouse.yaml full_rebuild: {name} is not a model")
            continue
        if models[name]:
            problems.append(f"warehouse.yaml full_rebuild: {name} is incremental; remove it")
        rows = entry.get("rows") if isinstance(entry, dict) else None
        if not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0:
            problems.append(f"warehouse.yaml full_rebuild: {name} needs rows: <positive integer>")
        elif rows > POLICY_ROWS and not EXCEPTION.match(str(entry.get("exception", ""))):
            problems.append(f'warehouse.yaml full_rebuild: {name} ceiling {rows:,} > 1M needs exception: "#<issue>"')
    return problems


SCHEDULER_WORKER = Path("ops/scheduler_worker/wrangler.toml")


def scheduler_worker(dispatch_run: str) -> list[str]:
    """The external scheduler clock (#802) dispatches a Pipeline run that exists and starts the scheduler."""
    worker = tomllib.loads((ROOT / SCHEDULER_WORKER).read_text())["vars"]
    workflow = ROOT / ".github/workflows" / worker["WORKFLOW"]
    if not workflow.exists():
        return [f"{SCHEDULER_WORKER}: no workflow {worker['WORKFLOW']}"]
    triggers = yaml.safe_load(workflow.read_text())
    inputs = (triggers.get("on") or triggers.get(True))["workflow_dispatch"]["inputs"]
    problems = []
    if worker["RUN"] != dispatch_run or worker["RUN"] not in inputs["run"]["options"]:
        problems.append(f"{SCHEDULER_WORKER}: RUN {worker['RUN']!r} is not the scheduler dispatch {dispatch_run!r}")
    if worker["SOURCE"] not in inputs["source"]["options"]:
        problems.append(f"{SCHEDULER_WORKER}: SOURCE {worker['SOURCE']!r} is not a Pipeline source")
    if worker["REF"] != "main":
        problems.append(f"{SCHEDULER_WORKER}: REF must be main (scheduler dispatches count only on main)")
    return problems


SCHEDULER_PACKAGES = {"pyyaml", "requests"}  # direct requirements `run.py schedule` may import
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
    names = [re.split(r"[=<>\[ ]", line.strip())[0] for line in (ROOT / "requirements.in").read_text().splitlines()]
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


STATUSES = {
    "registered",
    "reviewing",
    "approved",
    "landing",
    "bronze",
    "silver",
    "gold",
    "semantic",
    "published",
    "done",
    "parked",
}
FAMILIES = set("ABCDEFGHIJK")


def register(files: list[Path]) -> list[str]:
    """The source register (ingestion/register.yaml) and the source declarations agree."""
    entries = yaml.safe_load((ROOT / "ingestion/register.yaml").read_text())["sources"]
    problems, ids = [], [e.get("id") for e in entries]
    problems += [f"register: duplicate id {i}" for i in sorted({i for i in ids if ids.count(i) > 1})]
    problems += [
        f"register: {e.get('id')}: status {e.get('status')!r}" for e in entries if e.get("status") not in STATUSES
    ]
    problems += [
        f"register: {e.get('id')}: family {e['family']!r}" for e in entries if e.get("family", "A") not in FAMILIES
    ]
    problems += [
        f"register: {e.get('id')}: kpi {e['kpi']!r}" for e in entries if not isinstance(e.get("kpi", True), bool)
    ]
    problems += [
        f"register: {e.get('id')}: short_name {e['short_name']!r}"
        for e in entries
        if "short_name" in e and not (isinstance(e["short_name"], str) and len(e["short_name"]) <= 40)
    ]
    linked = {e["source_system"] for e in entries if e.get("source_system")}
    declared = {f.stem for f in files if f.parent == Path("ingestion/sources") and f.suffix == ".yaml"}
    problems += [f"register: source_system {s} has no ingestion/sources/{s}.yaml" for s in sorted(linked - declared)]
    problems += [f"register: ingestion/sources/{s}.yaml is not in the register" for s in sorted(declared - linked)]
    return problems


def backlog() -> list[str]:
    """The backlog files in .github/backlog/ pass ops/backlog_sync.py validate (docs/backlog.md)."""
    result = subprocess.run(
        [sys.executable, "-m", "ops.backlog_sync", "validate"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    return [] if result.returncode == 0 else result.stdout.strip().splitlines()[-40:] or [result.stderr.strip()[-500:]]


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
        "backlog": backlog,
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
