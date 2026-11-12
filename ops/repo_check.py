#!/usr/bin/env python3
"""Check a change against the [auto] rules of docs/quality.md (no credentials needed).

The `check` job runs this on every pull request; anyone can run it locally before
pushing. Each check prints its findings; the exit code is 1 when any check fails.
dbt parse and the secret scan run as their own steps of the job.
"""

from __future__ import annotations

import ast
import csv
from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

import yaml

ROOT = Path(__file__).resolve().parent.parent
ROOT_ALLOWED = {
    ".claude",  # project agents only (.claude/agents/*.md); everything else there is git-ignored
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


TEXT_SUFFIXES = {".md", ".py", ".yml", ".yaml", ".sql", ".toml", ".json", ".js", ".ts", ".tsx", ".txt", ".in"}
CONFLICT_MARKER = re.compile(r"^(<{7} |>{7} |={7}$)", re.MULTILINE)


def repository(files: list[Path]) -> list[str]:
    """Root allowlist, no dev/, the workflows, file size, type, emptiness and merge-conflict markers."""
    problems = [f"root: {name} is not allowed" for name in sorted({f.parts[0] for f in files} - ROOT_ALLOWED)]
    if any(f.parts[0] == "dev" for f in files):
        problems.append("dev/ must be deleted before merge")
    for f in files:
        if f.parts[0] == ".claude" and not (len(f.parts) == 3 and f.parts[1] == "agents" and f.suffix == ".md"):
            problems.append(f"{f}: only project agents (.claude/agents/*.md) are committed under .claude/")
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
        seed = f.suffix.lower() == ".csv" and f.as_posix().startswith("warehouse/seeds/")  # dbt seeds (code lists)
        if f.suffix.lower() in FORBIDDEN_SUFFIXES and not seed:
            problems.append(f"{f}: {f.suffix} files are not committed")
        if f.name.startswith(".env") and f.name != ".env.example":
            problems.append(f"{f}: environment files are not committed (only .env.example)")
        if f.suffix.lower() in TEXT_SUFFIXES:
            text = (ROOT / f).read_text(encoding="utf-8", errors="replace")
            if CONFLICT_MARKER.search(text):
                problems.append(f"{f}: merge-conflict markers")
    return problems


PIP_INSTALL = re.compile(r"pip3? install\s+(.*)")


def workflows(files: list[Path]) -> list[str]:
    """Every action in workflows and local actions is pinned by a full commit SHA with a version comment;
    every pip install there uses --require-hashes or pins each package with ==; every workflow declares
    workflow-level permissions, and neither the workflow nor a job uses write-all or read-all."""
    problems = []
    for f in files:
        if f.parts[:2] != (".github", "workflows") and not (
            f.parts[:2] == (".github", "actions") and f.stem == "action"
        ):
            continue
        for number, line in enumerate((ROOT / f).read_text().splitlines(), 1):
            if USES_LINE.match(line) and not PINNED_ACTION.search(line):
                problems.append(f"{f}:{number}: action not pinned by SHA with a version comment")
            pip = PIP_INSTALL.search(line)
            if pip and "--require-hashes" not in line:
                packages = [word for word in pip.group(1).split() if not word.startswith("-")]
                if not packages or any("==" not in word for word in packages):
                    problems.append(f"{f}:{number}: pip install without --require-hashes or == pins")
        if f.parts[:2] == (".github", "workflows"):
            workflow = yaml.safe_load((ROOT / f).read_text()) or {}
            if "permissions" not in workflow:
                problems.append(f"{f}: no workflow-level permissions")
            scopes = [("workflow", workflow.get("permissions"))]
            scopes += [(job, (spec or {}).get("permissions")) for job, spec in (workflow.get("jobs") or {}).items()]
            for scope, value in scopes:
                if value in ("write-all", "read-all"):
                    problems.append(f"{f}: {scope} permissions {value}")
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
    problems += gold_list(files)
    problems += gold_blocks(files)
    problems += source_declarations(files)
    return problems


INCREMENTAL = re.compile(r"materialized\s*=\s*['\"]incremental['\"]")
POLICY_ROWS = 1_000_000  # owner policy #822: only tables below ~1M rows are rebuilt in full
EXCEPTION = re.compile(r"^#\d+$")


def full_rebuild(files: list[Path]) -> list[str]:
    """Every dbt model, SQL or Python, that is not incremental (the project default is a full-rebuild table)
    has a row ceiling under `full_rebuild`, in its source's ingestion/sources/<source>.yaml or, for a shared
    model, in warehouse/warehouse.yaml (warehouse/declared.py), and in one place only; a ceiling above 1M rows
    names the issue that makes it incremental or records the owner's exception (#868)."""
    from warehouse import declared as declarations

    declared = declarations.full_rebuild()
    models = {
        f.stem: INCREMENTAL.search((ROOT / f).read_text()) is not None
        for f in files
        if f.parts[:2] == ("warehouse", "models") and f.suffix in (".sql", ".py") and (ROOT / f).exists()
    }
    problems = [
        f"full_rebuild: model {name} is not incremental and has no row ceiling (its source's "
        "ingestion/sources/<source>.yaml, or warehouse.yaml for a shared model)"
        for name, incremental in sorted(models.items())
        if not incremental and name not in declared
    ]
    problems += [f"full_rebuild: {twice} is declared twice" for twice in declarations.full_rebuild_twice()]
    for name, entry in sorted(declared.items()):
        if name not in models:
            problems.append(f"full_rebuild: {name} is not a model")
            continue
        if models[name]:
            problems.append(f"full_rebuild: {name} is incremental; remove it")
        rows = entry.get("rows") if isinstance(entry, dict) else None
        if not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0:
            problems.append(f"full_rebuild: {name} needs rows: <positive integer>")
        elif rows > POLICY_ROWS and not EXCEPTION.match(str(entry.get("exception", ""))):
            problems.append(f'full_rebuild: {name} ceiling {rows:,} > 1M needs exception: "#<issue>"')
    return problems


GOLD_ALIAS = re.compile(r"alias\s*=\s*['\"]([a-z0-9_]+)['\"]")
NOT_DBT_GOLD = {"gold.coverage"}  # written by each source's bronze.verify (ingestion/common/coverage.py), not dbt


def gold_list(files: list[Path]) -> list[str]:
    """The declared Gold list (warehouse.yaml `gold` plus each source's `gold:`, warehouse/declared.py) holds every
    Gold model (by alias) and seed of the dbt project plus gold.coverage, and nothing else (ops/retire_tables.py
    retires unlisted Gold tables)."""
    from warehouse import declared

    built = set()
    for f in files:
        if not (ROOT / f).exists():
            continue
        if f.parts[:3] == ("warehouse", "models", "gold") and f.suffix in (".sql", ".py"):
            alias = GOLD_ALIAS.search((ROOT / f).read_text())
            built.add("gold." + (alias.group(1) if alias else f.stem))
        elif f.parent == Path("warehouse/seeds") and f.suffix == ".csv":
            built.add("gold." + f.stem)
    listed = set(declared.gold())
    problems = [
        f"gold: {name} is built by the dbt project but not listed (`gold:` of its source's "
        "ingestion/sources/<source>.yaml, or warehouse.yaml `gold` for a shared table)"
        for name in built - listed
    ]
    problems += [f"gold: {name} is listed but is not a Gold model or seed" for name in listed - built - NOT_DBT_GOLD]
    return sorted(problems)


GOLD_BLOCK_TAG = re.compile(r"tags\s*=\s*\[[^\]]*['\"]gold_block['\"]")
GOLD_BLOCK_LIST = re.compile(r"macro poc_legacy_block_models\(\).*?endmacro", re.S)
GOLD_ACTIVATED = re.compile(r"macro poc_legacy_activated_blocks\(\).*?endmacro", re.S)
GOLD_BLOCK_EXEMPT = {"poc_build_check"}  # reads the blocks; not a block
# Per-source registration (docs/source-registration.md): warehouse/macros/sources/<source_system>.sql
SOURCE_MACROS = Path("warehouse/macros/sources")
REGISTRATION = re.compile(r"\{%-?\s*macro\s+poc_(source|geography)__(\w+)\s*\(\s*\)\s*-?%\}(.*?)\{%-?\s*endmacro", re.S)
REGISTERED_MODEL = re.compile(r"['\"]block_model['\"]\s*:\s*['\"](\w+)['\"]")
REGISTERED_BLOCKS = re.compile(r"['\"](activated|blocks)['\"]\s*:")
CTE_NAME = re.compile(r"^\s*(\w+) as \(", re.M)
MAP_GEOGRAPHY = Path("warehouse/models/gold/poc_map_geography.sql")


def registrations(files: list[Path]) -> tuple[dict[str, dict], list[str]]:
    """({source_system: {"block_model": model or None, "activated": bool, "geography": bool}}, problems) of the
    per-source Gold registrations: the macros poc_source__<source_system>() and poc_geography__<source_system>()
    live in warehouse/macros/sources/<source_system>.sql and nowhere else, and <source_system> is a declared
    source (ingestion/sources/<source_system>.yaml). The CTEs of a geography macro are named <source_system>
    or <source_system>_<x>, and no name is used twice: not by a CTE written in poc_map_geography.sql (a source
    that is still mapped there) and not by another source (dbt compiles two CTEs of one name; DuckDB fails on
    them only in the build)."""
    found: dict[str, dict] = {}
    problems = []
    taken = {name: str(MAP_GEOGRAPHY) for name in CTE_NAME.findall((ROOT / MAP_GEOGRAPHY).read_text())}
    for f in files:
        if f.parts[:2] != ("warehouse", "macros") or f.suffix != ".sql" or not (ROOT / f).exists():
            continue
        for kind, source, body in REGISTRATION.findall((ROOT / f).read_text()):
            if f != SOURCE_MACROS / f"{source}.sql":
                problems.append(f"{f}: macro poc_{kind}__{source} belongs in {SOURCE_MACROS}/{source}.sql")
            if not (ROOT / "ingestion/sources" / f"{source}.yaml").exists():
                problems.append(
                    f"{f}: poc_{kind}__{source}: {source} is not a source (ingestion/sources/{source}.yaml)"
                )
            entry = found.setdefault(source, {"block_model": None, "activated": False, "geography": False})
            if kind == "geography":
                entry["geography"] = True
                names = CTE_NAME.findall(body)
                if source not in names:
                    problems.append(f"{f}: poc_geography__{source} must define the CTE `{source} as (...)`")
                for name in names:
                    if name != source and not name.startswith(f"{source}_"):
                        problems.append(f"{f}: CTE {name} of poc_geography__{source} must be named {source}_<x>")
                    if name in taken:
                        problems.append(f"{f}: CTE {name} of poc_geography__{source} already exists in {taken[name]}")
                    taken[name] = f"poc_geography__{source}"
                continue
            model = REGISTERED_MODEL.search(body)
            entry["block_model"] = model.group(1) if model else None
            entry["activated"] = REGISTERED_BLOCKS.search(body) is not None
            if model is None:
                problems.append(f"{f}: poc_source__{source} must return 'block_model': '<poc_build_ model>'")
        if f.parent == SOURCE_MACROS and not (ROOT / "ingestion/sources" / f"{f.stem}.yaml").exists():
            problems.append(f"{f}: the file name must be a source_system (ingestion/sources/{f.stem}.yaml)")
    return found, problems


def gold_blocks(files: list[Path]) -> list[str]:
    """Every Gold build stage model (models/gold/poc_build_*.sql except poc_build_check) is tagged `gold_block`
    and declared exactly once: registered by its source (poc_source__<source_system>() in
    warehouse/macros/sources/<source_system>.sql) or, for sources older than D34, listed in
    `poc_legacy_block_models()` (warehouse/macros/poc_blocks.sql); the tagged models are exactly the declared
    ones, and each declared source_system has activated blocks. warehouse/changed.py lets a waiting block's
    children build; poc_observations() unions only the declared blocks."""
    stages, tagged = set(), set()
    for f in files:
        if f.parts[:3] != ("warehouse", "models", "gold") or f.suffix != ".sql" or not (ROOT / f).exists():
            continue
        if f.stem.startswith("poc_build_") and f.stem not in GOLD_BLOCK_EXEMPT:
            stages.add(f.stem)
        if GOLD_BLOCK_TAG.search((ROOT / f).read_text()):
            tagged.add(f.stem)
    macros = (ROOT / "warehouse/macros/poc_blocks.sql").read_text()
    macro = GOLD_BLOCK_LIST.search(macros)
    legacy = dict(re.findall(r"^\s*(\w+):(\w+)\s*$", macro.group(0), re.M)) if macro else {}
    activated = GOLD_ACTIVATED.search(macros)
    registered, problems = registrations(files)
    entries = dict(legacy)
    for source, entry in sorted(registered.items()):
        model = entry["block_model"]
        if model is None:
            continue
        if source in legacy.values():
            problems.append(
                f"poc_source__{source}: {source} already has a Gold block in poc_legacy_block_models(); a source "
                "is declared in one of the two places"
            )
        if model in entries:
            problems.append(f"Gold block {model} is declared twice (poc_source__{source} and {entries[model]})")
        entries[model] = source
        if not entry["activated"]:
            problems.append(f"poc_source__{source}: no `activated` select and no `blocks` list")
    problems += [f"{m}: a Gold build stage must be tagged gold_block" for m in stages - tagged]
    declare = "register it in warehouse/macros/sources/<source_system>.sql (docs/source-registration.md)"
    problems += [f"{m} is a Gold build stage but not declared: {declare}" for m in stages - set(entries)]
    problems += [f"{m} is tagged gold_block but not declared: {declare}" for m in tagged - set(entries)]
    problems += [f"{m} is declared a Gold block but not tagged gold_block" for m in set(entries) - tagged]
    problems += [
        f"poc_legacy_activated_blocks(): no blocks of {source} ({m})"
        for m, source in legacy.items()
        if not activated or f"'{source}'" not in activated.group(0)
    ]
    return sorted(problems)


SILVER_ALIAS = GOLD_ALIAS


def source_declarations(files: list[Path]) -> list[str]:
    """A source is declared in every place that reads it (docs/source-registration.md): each table of a
    source's `bronze:` is a dbt source table in warehouse/models/bronze/*.yml (so lineage starts at Bronze),
    declared once; and each dbt Silver model is in the `silver:` list of exactly one source (a Gold table may
    be shared by a family, as gold.build_public_finance is). The source.csv and licence.csv rows are checked
    by `register`, the Gold list and row ceilings by `gold_list` and `full_rebuild`, the Gold block by
    `gold_blocks`."""
    from warehouse import declared

    sources = declared.sources()
    problems = []
    dbt_tables: dict[str, list[str]] = {}
    for f in files:
        if f.parts[:3] == ("warehouse", "models", "bronze") and f.suffix in (".yml", ".yaml") and (ROOT / f).exists():
            for block in (yaml.safe_load((ROOT / f).read_text()) or {}).get("sources") or []:
                for table in block.get("tables") or []:
                    name = f"{block.get('schema', block['name'])}.{table.get('identifier') or table['name']}"
                    dbt_tables.setdefault(name, []).append(f"{f.name}: {block['name']}")
    problems += [
        f"dbt source table {name} is declared twice ({', '.join(where)})"
        for name, where in sorted(dbt_tables.items())
        if len(where) > 1
    ]
    silver_models = {}
    for f in files:
        if f.parts[:3] == ("warehouse", "models", "silver") and f.suffix in (".sql", ".py") and (ROOT / f).exists():
            alias = SILVER_ALIAS.search((ROOT / f).read_text())
            silver_models["silver." + (alias.group(1) if alias else f.stem)] = f
    owners: dict[str, list[str]] = {}
    for source, config in sources.items():
        for name in config.get("bronze") or []:
            if name not in dbt_tables:
                problems.append(
                    f"{source}: {name} is in ingestion/sources/{source}.yaml but is not a dbt source table: add it "
                    f"to warehouse/models/bronze/{source}.yml"
                )
        for name in config.get("silver") or []:
            owners.setdefault(name, []).append(source)
    problems += [
        f"{name} is listed by more than one source ({', '.join(where)})"
        for name, where in sorted(owners.items())
        if len(where) > 1
    ]
    problems += [
        f"{f}: Silver model {name} is in no source's `silver:` list (ingestion/sources/<source>.yaml)"
        for name, f in sorted(silver_models.items())
        if name not in owners
    ]
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
    if worker["SOURCE"] != "platform" and not (ROOT / "ingestion/sources" / f"{worker['SOURCE']}.yaml").exists():
        problems.append(f"{SCHEDULER_WORKER}: SOURCE {worker['SOURCE']!r} is not a Pipeline source")
    if inputs["source"].get("type") != "string" or "options" in inputs["source"]:
        problems.append(
            f"{worker['WORKFLOW']}: the `source` input is free text checked by the plan job, not a choice list "
            "(a list needs a line per source; docs/source-registration.md)"
        )
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
    "built",
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
REQUIRED = ("family", "kpi", "short_name")


def register(files: list[Path]) -> list[str]:
    """The source register (ingestion/register.yaml) and the source declarations agree."""
    entries = yaml.safe_load((ROOT / "ingestion/register.yaml").read_text())["sources"]
    problems, ids = [], [e.get("id") for e in entries]
    problems += [f"register: duplicate id {i}" for i in sorted({i for i in ids if ids.count(i) > 1})]
    problems += [
        f"register: {e.get('id')}: status {e.get('status')!r}" for e in entries if e.get("status") not in STATUSES
    ]
    problems += [f"register: {e.get('id')}: missing {f}" for e in entries for f in REQUIRED if f not in e]
    problems += [
        f"register: {e.get('id')}: family {e.get('family')!r}" for e in entries if e.get("family") not in FAMILIES
    ]
    problems += [
        f"register: {e.get('id')}: kpi {e.get('kpi')!r}" for e in entries if not isinstance(e.get("kpi"), bool)
    ]
    problems += [
        f"register: {e.get('id')}: short_name {e.get('short_name')!r}"
        for e in entries
        if "short_name" in e and not (isinstance(e["short_name"], str) and 0 < len(e["short_name"]) <= 40)
    ]
    linked = {e["source_system"] for e in entries if e.get("source_system")}
    declared = {f.stem for f in files if f.parent == Path("ingestion/sources") and f.suffix == ".yaml"}
    problems += [f"register: source_system {s} has no ingestion/sources/{s}.yaml" for s in sorted(linked - declared)]
    problems += [f"register: ingestion/sources/{s}.yaml is not in the register" for s in sorted(declared - linked)]
    seeds = ROOT / "warehouse" / "seeds"
    with open(seeds / "licence.csv", newline="", encoding="utf-8") as f:
        licences = {r["licence_key"] for r in csv.DictReader(f)}
    with open(seeds / "source.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # One source system may serve several register entries (news feeds: one entry per outlet, docs/topics.md);
    # source.csv names one of them.
    by_system: dict[str, set[str]] = {}
    for e in entries:
        if e.get("source_system"):
            by_system.setdefault(e["source_system"], set()).add(e.get("id"))
    problems += [
        f"register: source.csv {r['source_key']}: register_id {r['register_id']}"
        f" is not one of {sorted(by_system.get(r['source_key'], set()))}"
        for r in rows
        if r["register_id"] not in by_system.get(r["source_key"], set())
    ]
    problems += [
        f"register: source_system {s} has no row in source.csv"
        for s in sorted(linked - {r["source_key"] for r in rows})
    ]
    problems += [
        f"register: source.csv {r['source_key']}: licence_key {r['licence_key']} is not in licence.csv"
        for r in rows
        if r["licence_key"] not in licences
    ]
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


PUBLISHED = ROOT / "warehouse" / "published"
# Fixture path pattern (relative to warehouse/published/fixtures) → schema name in warehouse/published/schemas.
PUBLISHED_SCHEMAS = {  # Path.match matches from the right: v2 entries first ("status.json" matches "v2/status.json")
    "v2/catalog.json": "v2/catalog",
    "v2/latest.json": "v2/latest",
    "v2/calendar.json": "v2/calendar",
    "v2/status.json": "v2/status",
    "v2/briefs.json": "v2/briefs",
    "v2/kpi/*/series.json": "v2/series",
    "catalog/catalog.json": "catalog",
    "manifest.json": "manifest",
    "status.json": "status",
    "kpi/*/latest.json": "kpi_latest",
    "feed/changes.json": "feed_changes",
    "geo/*.geojson": "geo",
    "v2/topics.json": "v2/topics",
}
JSON_TYPES = {
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
    "null": lambda v: v is None,
}


def published_contract() -> list[str]:
    """Each JSON or GeoJSON fixture of the published layer (docs/published.md) has every required key of
    its schema, and every present property has the schema's JSON type (recursing into objects and array
    items); every feature of a GeoJSON fixture has the level named by the file. Every published KPI of the
    catalog fixture outside publish.SKIP has a kpi/<kpi_key>/latest.json fixture (D26)."""
    import json

    fixtures = PUBLISHED / "fixtures"
    problems = []
    for fixture in sorted([*fixtures.rglob("*.json"), *fixtures.rglob("*.geojson")]):
        rel = fixture.relative_to(fixtures)
        name = next((n for pattern, n in PUBLISHED_SCHEMAS.items() if rel.match(pattern)), None)
        if name is None:
            problems.append(f"{rel}: no schema in PUBLISHED_SCHEMAS")
            continue
        schema = json.loads((PUBLISHED / "schemas" / f"{name}.schema.json").read_text(encoding="utf-8"))
        data = json.loads(fixture.read_text(encoding="utf-8"))
        problems += [f"{rel}: {p}" for p in _schema_problems(data, schema, "$")]
        if fixture.suffix == ".geojson":
            for i, feature in enumerate(data.get("features") or []):
                level = ((feature or {}).get("properties") or {}).get("level")
                if level != fixture.stem:
                    problems.append(f"{rel}: feature {i} level {level!r} is not {fixture.stem!r}")
    # D26: every published KPI outside publish.SKIP has a latest.json fixture. SKIP is read with ast
    # because warehouse.publish imports duckdb.
    tree = ast.parse((ROOT / "warehouse" / "publish.py").read_text(encoding="utf-8"))
    skip = next(
        set(ast.literal_eval(node.value))
        for node in tree.body
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "SKIP" for t in node.targets)
    )
    catalog = json.loads((fixtures / "catalog" / "catalog.json").read_text(encoding="utf-8"))
    for kpi in catalog.get("kpis") or []:
        key = kpi.get("kpi_key")
        if kpi.get("status") != "published":
            continue
        latest = fixtures / "kpi" / str(key) / "latest.json"
        if key in skip:
            if latest.exists():
                problems.append(f"kpi/{key}/latest.json: {key!r} is in publish.SKIP and must have no latest.json (D26)")
        elif not latest.exists():
            problems.append(f"kpi/{key}/latest.json: missing for published KPI {key!r} (D26)")
        elif (found := json.loads(latest.read_text(encoding="utf-8")).get("kpi_key")) != key:
            problems.append(f"kpi/{key}/latest.json: kpi_key {found!r} is not {key!r}")
    v2 = {
        f.relative_to(fixtures / "v2").as_posix(): json.loads(f.read_text(encoding="utf-8"))
        for f in sorted((fixtures / "v2").rglob("*.json"))
        if f.name != "topics.json"
    }
    problems += [f"v2: {p}" for p in published_v2_problems(v2)]
    return problems


PERIOD_FORMATS = {"M": r"[0-9]{4}-(0[1-9]|1[0-2])", "Q": r"[0-9]{4}-Q[1-4]", "A": r"[0-9]{4}"}


def published_v2_problems(files: dict[str, dict]) -> list[str]:
    """Cross-file rules of published contract v2 (docs/published.md) over the files of 06_published/v2/ by path
    (catalog.json, latest.json, calendar.json, status.json, kpi/<kpi_key>/series.json): every catalog KPI has its
    series.json and the reverse; units, geographies, sources, dimensions and members used are declared in the
    catalog; periods are sorted, unique and of the KPI's frequency, with first_period/last_period at the ends; a
    series with its own frequency (another than the KPI's) follows periods_by_frequency of it, whose lists are
    sorted, unique, of their frequency and each used; one series per geography × source × members; every series has
    one value or null per period of its own and at least one value; flags only on its periods; latest.json has one
    value per KPI and geography, taken from the shown-frequency total series of its source (period and value), never
    from a series of another frequency; next_release is the KPI's first calendar.json date.
    gold.publish runs it before writing v2 (warehouse/publish_v2.py); the published contract check on the fixtures."""
    import json

    problems = []
    for name in ("catalog.json", "latest.json", "calendar.json", "status.json"):
        if name not in files:
            problems.append(f"{name}: missing")
    if "catalog.json" not in files:
        return problems
    catalog = files["catalog.json"]
    kpis = {k["kpi_key"]: k for k in catalog.get("kpis", [])}
    units = {u["unit_key"] for u in catalog.get("units", [])}
    geos = {g["key"] for g in catalog.get("geographies", [])}
    sources = {s["source_key"] for s in catalog.get("sources", [])}
    members = {d["dimension_key"]: {m["key"] for m in d.get("members", [])} for d in catalog.get("dimensions", [])}
    if len(kpis) != len(catalog.get("kpis", [])):
        problems.append("catalog.json: a kpi_key occurs twice")
    for dim, keys in members.items():
        if "_T" in keys:
            problems.append(f"catalog.json: dimension {dim} lists the total _T as a member")
    series = {name.split("/")[1]: doc for name, doc in files.items() if name.startswith("kpi/")}
    problems += [f"kpi/{k}/series.json: missing for catalog KPI {k}" for k in kpis if k not in series]
    problems += [f"kpi/{k}/series.json: {k} is not in catalog.json" for k in series if k not in kpis]
    for key, kpi in kpis.items():
        where = f"catalog.json {key}"
        if kpi["unit_key"] not in units:
            problems.append(f"{where}: unit {kpi['unit_key']} not in units")
        problems += [f"{where}: geography {g} not in geographies" for g in kpi["geographies"] if g not in geos]
        problems += [f"{where}: source {s} not in sources" for s in kpi["source_keys"] if s not in sources]
        problems += [f"{where}: dimension {d} not in dimensions" for d in kpi["dimension_keys"] if d not in members]
    for key, doc in series.items():
        kpi = kpis.get(key)
        where = f"kpi/{key}/series.json"
        if kpi is None:
            continue
        periods = doc["periods"]
        if doc["kpi_key"] != key:
            problems.append(f"{where}: kpi_key {doc['kpi_key']} is not {key}")
        if doc["frequency"] != kpi["frequency"]:
            problems.append(f"{where}: frequency {doc['frequency']} is not the catalog's {kpi['frequency']}")
        if periods != sorted(set(periods)):
            problems.append(f"{where}: periods are not sorted and unique")
        fmt = PERIOD_FORMATS.get(doc["frequency"], ".*")
        problems += [f"{where}: period {p} is not {doc['frequency']}" for p in periods if not re.fullmatch(fmt, p)]
        if periods and (periods[0], periods[-1]) != (kpi["first_period"], kpi["last_period"]):
            problems.append(f"{where}: periods {periods[0]}…{periods[-1]} differ from the catalog's range")
        others = doc.get("periods_by_frequency", {})
        for frequency, axis in others.items():
            at = f"{where} periods_by_frequency.{frequency}"
            if frequency == doc["frequency"] or frequency not in PERIOD_FORMATS:
                problems.append(f"{at}: not another frequency than {doc['frequency']} of M, Q, A")
                continue
            if not axis or axis != sorted(set(axis)):
                problems.append(f"{at}: periods are empty or not sorted and unique")
            fmt = PERIOD_FORMATS[frequency]
            problems += [f"{at}: period {p} is not {frequency}" for p in axis if not re.fullmatch(fmt, p)]
            if not any(s.get("frequency") == frequency for s in doc["series"]):
                problems.append(f"{at}: no series has frequency {frequency}")
        series_keys = set()
        for i, s in enumerate(doc["series"]):
            at = f"{where} series[{i}]"
            series_key = (s["geography"], s["source_key"], json.dumps(s["members"], sort_keys=True))
            if series_key in series_keys:
                problems.append(f"{at}: geography, source and members occur twice")
            series_keys.add(series_key)
            own: list[str] | None = periods
            if "frequency" in s:
                own = others.get(s["frequency"])
                if own is None:
                    problems.append(f"{at}: frequency {s['frequency']} has no periods_by_frequency")
            if own is not None and len(s["values"]) != len(own):
                problems.append(f"{at}: {len(s['values'])} values for {len(own)} periods")
            if all(v is None for v in s["values"]):
                problems.append(f"{at}: no value")
            if s["geography"] not in kpi["geographies"]:
                problems.append(f"{at}: geography {s['geography']} not in the KPI's geographies")
            if s["source_key"] not in kpi["source_keys"]:
                problems.append(f"{at}: source {s['source_key']} not in the KPI's source_keys")
            if s["unit_key"] not in units:
                problems.append(f"{at}: unit {s['unit_key']} not in units")
            for dim, member in s["members"].items():
                if dim not in kpi["dimension_keys"] or member not in members.get(dim, set()):
                    problems.append(f"{at}: member {dim}={member} not declared")
            if own is not None:
                problems += [f"{at}: flag on unknown period {p}" for p in s.get("flags", {}) if p not in own]
    seen = set()
    for v in files.get("latest.json", {}).get("values", []):
        where = f"latest.json {v['kpi_key']} {v['geography']}"
        if (v["kpi_key"], v["geography"]) in seen:
            problems.append(f"{where}: occurs twice")
        seen.add((v["kpi_key"], v["geography"]))
        if v["kpi_key"] not in kpis:
            problems.append(f"{where}: KPI not in catalog.json")
            continue
        if v["geography"] not in kpis[v["kpi_key"]]["geographies"]:
            problems.append(f"{where}: geography not in the KPI's geographies")
        if v["kpi_key"] in series:
            doc = series[v["kpi_key"]]
            if v["period"] not in doc["periods"]:
                problems.append(f"{where}: period {v['period']} not in series.json periods")
            else:
                at = doc["periods"].index(v["period"])
                shown = [
                    s
                    for s in doc["series"]
                    if (s["geography"], s["source_key"]) == (v["geography"], v["source_key"])
                    and not s["members"]
                    and "frequency" not in s
                ]
                if not shown or shown[0]["values"][at] != v["value"]:
                    problems.append(f"{where}: value is not the shown-frequency series of {v['source_key']}")
        if v["unit_key"] not in units:
            problems.append(f"{where}: unit {v['unit_key']} not in units")
    first: dict[str, str] = {}
    for r in sorted(files.get("calendar.json", {}).get("releases", []), key=lambda r: r["date"]):
        for k in r["kpi_keys"]:
            if k not in kpis:
                problems.append(f"calendar.json {r['date']}: KPI {k} not in catalog.json")
            first.setdefault(k, r["date"])
    for key, kpi in kpis.items():
        if kpi["next_release"] != first.get(key):
            problems.append(f"catalog.json {key}: next_release {kpi['next_release']} is not {first.get(key)}")
    if "briefs.json" in files:
        problems += briefs_problems(
            files["briefs.json"], kpis, series, files.get("latest.json", {}), files.get("calendar.json", {})
        )
    return problems


def period_ordinal(period: str) -> int:
    """A v2 period as a number that grows by one per period of its frequency ('2025' → 2025, '2025-Q3' → 8102,
    '2025-09' → 24308)."""
    year = int(period[:4])
    if len(period) == 4:
        return year
    if period[5] == "Q":
        return year * 4 + int(period[6]) - 1
    return year * 12 + int(period[5:7]) - 1


def brief_change(value: float, before: float | None, change_unit: str) -> float | None:
    """The change of a brief against an earlier value: the difference for `pp` and `diff`, the relative change in %
    for `%` (None when the earlier value is not above zero); rounded to 6 decimals; None without an earlier value."""
    if before is None:
        return None
    if change_unit in ("pp", "diff"):
        return round(value - before, 6)
    if before <= 0:
        return None
    return round((value - before) / before * 100, 6)


def brief_extreme(periods: list[str], values: list[float | None], at: int, min_periods: int) -> dict | None:
    """Whether values[at] is the highest or lowest for at least ``min_periods`` observed values back: walking back
    from ``at``, every observed value is below it (highest) or above it (lowest) until ``since_period``, the last
    period with a value as high (as low) or more; ``since_period`` is None when no earlier value reaches it (a record
    over the whole series). ``periods`` counts the observed values passed. None when neither holds."""
    value = values[at]
    earlier = [(periods[i], values[i]) for i in range(at - 1, -1, -1) if values[i] is not None]
    for kind, beaten in (("highest", lambda v: v < value), ("lowest", lambda v: v > value)):
        count = 0
        since = None
        for period, v in earlier:
            if not beaten(v):
                since = period
                break
            count += 1
        if count >= min_periods:
            return {"kind": kind, "since_period": since, "periods": count}
    return None


PP_UNITS = ("index_py_100", "index_pp_100")  # 100 + a rate in %: the difference is in percentage points
DIFF_UNITS = ("gini_0_100", "balance")  # points on a bounded scale or a balance: a relative change means nothing


def brief_change_unit(aggregation: str, unit_key: str, values: list[float | None]) -> str:
    """How a brief states a change (docs/published.md, briefs.json): `pp` (the difference, in percentage points)
    for KPIs with aggregation `rate`, units in % (`pct…`) and the indices against the previous year or period;
    `diff` (the difference, in the value's own unit) for the Gini scale, balances and every series with a negative
    value (signed sums such as current-account or FDI balances in mln EUR); `%` (the relative change) for the
    other levels (counts, money, index levels, ratios). ``values`` are the series' values."""
    if aggregation == "rate" or unit_key.startswith("pct") or unit_key in PP_UNITS:
        return "pp"
    if unit_key in DIFF_UNITS or any(v is not None and v < 0 for v in values):
        return "diff"
    return "%"


def _utc(stamp: str) -> datetime:
    return datetime.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)


def briefs_problems(
    briefs: dict, kpis: dict[str, dict], series: dict[str, dict], latest: dict, calendar: dict
) -> list[str]:
    """Cross-file rules of briefs.json: each brief is one latest.json value (same source, unit, period, value,
    previous and year-ago value, flags), of a geography the file allows, at most once per KPI and geography;
    year_ago_period and year_ago_value are both set or both null, the year-ago value is the series' value of that
    period; changes, the change unit and the extreme are what the numbers give (brief_change, brief_change_unit,
    brief_extreme on the series of series.json); detected_at is at most window_days before generated_at and has its
    entry in detections (also within the window, one per KPI, geography and period); `detected` release dates are
    the detection day; next_release is the KPI's first calendar.json date after released_at; newest first."""
    problems = []
    shown = {(v["kpi_key"], v["geography"]): v for v in latest.get("values", [])}
    allowed = set(briefs.get("geographies", []))
    generated = _utc(briefs["generated_at"])
    window = timedelta(days=briefs.get("window_days", 0))
    dates: dict[str, list[str]] = {}
    for r in calendar.get("releases", []):
        for k in r["kpi_keys"]:
            dates.setdefault(k, []).append(r["date"])
    detections = {}
    for d in briefs.get("detections", []):
        key = (d["kpi_key"], d["geography"], d["period"])
        if key in detections:
            problems.append(f"briefs.json detections {key}: occurs twice")
        detections[key] = d
        if not timedelta(0) <= generated - _utc(d["detected_at"]) <= window:
            problems.append(f"briefs.json detections {key}: detected_at {d['detected_at']} outside the window")
    seen = set()
    entries = briefs.get("briefs", [])
    order = [(b["detected_at"], b["released_at"]) for b in entries]
    if order != sorted(order, reverse=True):
        problems.append("briefs.json: briefs are not sorted newest first (detected_at, released_at)")
    for b in entries:
        where = f"briefs.json {b['kpi_key']} {b['geography']}"
        if (b["kpi_key"], b["geography"]) in seen:
            problems.append(f"{where}: occurs twice")
        seen.add((b["kpi_key"], b["geography"]))
        if b["geography"] not in allowed:
            problems.append(f"{where}: geography not in the file's geographies")
        if not timedelta(0) <= generated - _utc(b["detected_at"]) <= window:
            problems.append(f"{where}: detected_at {b['detected_at']} is not within window_days of generated_at")
        found = detections.get((b["kpi_key"], b["geography"], b["period"]))
        if found is None or found["detected_at"] != b["detected_at"]:
            problems.append(f"{where}: no detections entry with its period and detected_at")
        if b["released_at_source"] == "detected" and b["released_at"] != b["detected_at"][:10]:
            problems.append(f"{where}: released_at {b['released_at']} of source detected is not the detection day")
        later = sorted(d for d in dates.get(b["kpi_key"], []) if d > b["released_at"])
        if b["next_release"] != (later[0] if later else None):
            problems.append(
                f"{where}: next_release {b['next_release']} is not the first calendar date after released_at"
            )
        if b["next_release"] is not None and b["next_release"] <= b["released_at"]:
            problems.append(f"{where}: next_release {b['next_release']} is not after released_at {b['released_at']}")
        v = shown.get((b["kpi_key"], b["geography"]))
        kpi = kpis.get(b["kpi_key"])
        doc = series.get(b["kpi_key"])
        if v is None or kpi is None or doc is None:
            problems.append(f"{where}: no latest.json value, catalog KPI or series.json")
            continue
        for field in ("source_key", "unit_key", "period", "value", "previous_period", "previous_value"):
            if b[field] != v[field]:
                problems.append(f"{where}: {field} {b[field]!r} is not latest.json's {v[field]!r}")
        for field in ("year_ago_value", "flags"):
            if b[field] != v[field]:
                problems.append(f"{where}: {field} {b[field]!r} is not latest.json's {v[field]!r}")
        if (b["year_ago_period"] is None) != (b["year_ago_value"] is None):
            problems.append(f"{where}: year_ago_period and year_ago_value must both be set or both be null")
        if b["year_ago_period"] is not None and period_ordinal(b["period"]) - period_ordinal(b["year_ago_period"]) != {
            "M": 12,
            "Q": 4,
        }.get(kpi["frequency"]):
            problems.append(f"{where}: year_ago_period {b['year_ago_period']} is not a year before {b['period']}")
        candidates = [
            s
            for s in doc["series"]
            if s["geography"] == b["geography"]
            and s["source_key"] == b["source_key"]
            and not s["members"]
            and "frequency" not in s
        ]
        if len(candidates) != 1 or b["period"] not in doc["periods"]:
            problems.append(f"{where}: no single total series of {b['source_key']} with period {b['period']}")
            continue
        values = candidates[0]["values"]
        at = doc["periods"].index(b["period"])
        if values[at] != b["value"]:
            problems.append(f"{where}: value {b['value']} is not series.json's {values[at]}")
        if b["year_ago_period"] is not None:
            ago = doc["periods"].index(b["year_ago_period"]) if b["year_ago_period"] in doc["periods"] else None
            if ago is None or values[ago] != b["year_ago_value"]:
                problems.append(f"{where}: year_ago_value {b['year_ago_value']} is not series.json's value")
        unit = brief_change_unit(kpi["aggregation"], b["unit_key"], values)
        if b["change_unit"] != unit:
            problems.append(f"{where}: change_unit {b['change_unit']} is not {unit}")
        if b["change"] != brief_change(b["value"], b["previous_value"], unit):
            problems.append(f"{where}: change {b['change']} does not follow from the values")
        if b["year_ago_change"] != brief_change(b["value"], b["year_ago_value"], unit):
            problems.append(f"{where}: year_ago_change {b['year_ago_change']} does not follow from the values")
        expected = brief_extreme(doc["periods"], values, at, briefs.get("extreme_min_periods", 12))
        if b["extreme"] != expected:
            problems.append(f"{where}: extreme {b['extreme']} is not {expected}")
    return problems


def published_v2_seeds() -> list[str]:
    """Seeds of published contract v2: frequency (M, Q or A), aggregation and polarity are one value per v2 key of
    kpi_v2_map.csv; a concept with a ready kpi_v2_map row (in v2 whatever its status, O37) has definition_en and
    definition_pl in kpi.csv; every unit of
    unit.csv has decimals in unit_decimals.csv; kpi_member.csv names v2 keys of the map."""
    seeds = ROOT / "warehouse" / "seeds"

    def rows(name: str) -> list[dict[str, str]]:
        with open(seeds / f"{name}.csv", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    problems = []
    v2map = rows("kpi_v2_map")
    for field in ("frequency", "aggregation", "polarity"):
        values: dict[str, set[str]] = {}
        for r in v2map:
            values.setdefault(r["v2_key"], set()).add(r[field])
        problems += [f"kpi_v2_map.csv {k}: several {field} values {sorted(v)}" for k, v in values.items() if len(v) > 1]
    problems += [
        f"kpi_v2_map.csv {r['v2_key']}: frequency {r['frequency']} is not M, Q or A"
        for r in v2map
        if r["frequency"] not in PERIOD_FORMATS
    ]
    ready = {r["concept_key"] for r in v2map if r["concept_key"] and r["status"] == "ready"}
    for kpi in rows("kpi"):  # O37: a concept with a ready map row reaches v2 whatever its status
        if kpi["kpi_key"] in ready:
            for field in ("definition_en", "definition_pl"):
                if not kpi[field]:
                    problems.append(f"kpi.csv {kpi['kpi_key']}: v2 concept (ready map row) without {field}")
    decimals = {r["unit_key"] for r in rows("unit_decimals")}
    problems += [
        f"unit_decimals.csv: no decimals for unit {u['unit_key']}"
        for u in rows("unit")
        if u["unit_key"] not in decimals
    ]
    keys = {r["v2_key"] for r in v2map}
    problems += [
        f"kpi_member.csv: {r['v2_key']} is not a v2 key of kpi_v2_map.csv"
        for r in rows("kpi_member")
        if r["v2_key"] not in keys
    ]
    return problems


def _schema_problems(value: object, schema: dict, where: str) -> list[str]:
    types = schema.get("type")
    types = [types] if isinstance(types, str) else types or []
    if types and not any(JSON_TYPES[t](value) for t in types):
        return [f"{where}: expected {'|'.join(types)}, got {type(value).__name__}"]
    problems = []
    if "enum" in schema and value not in schema["enum"]:
        problems.append(f"{where}: {value!r} is not one of {schema['enum']}")
    pattern = schema.get("pattern")
    if isinstance(value, str) and pattern and re.search(pattern, value) is None:
        problems.append(f"{where}: {value!r} does not match {pattern}")
    if isinstance(value, dict):
        problems += [f"{where}: missing required key {k!r}" for k in schema.get("required", []) if k not in value]
        for key, sub in schema.get("properties", {}).items():
            if key in value:
                problems += _schema_problems(value[key], sub, f"{where}.{key}")
    if isinstance(value, list) and len(value) < schema.get("minItems", 0):
        problems.append(f"{where}: {len(value)} items, fewer than {schema['minItems']}")
    if isinstance(value, list) and "items" in schema:
        for i, item in enumerate(value):
            problems += _schema_problems(item, schema["items"], f"{where}[{i}]")
    return problems


def semantic_models() -> list[str]:
    """Every published KPI of warehouse/seeds/kpi.csv has a metrics model and a semantic model on it."""
    semantic = yaml.safe_load((ROOT / "warehouse" / "semantic_models" / "gold.yml").read_text(encoding="utf-8"))
    models = set()
    for entry in semantic.get("semantic_models") or []:
        match = re.fullmatch(r"ref\('(?:poc_)?metrics_(\w+)'\)", str(entry.get("model", "")).strip())
        if match:
            models.add(match.group(1))
    problems = []
    with open(ROOT / "warehouse" / "seeds" / "kpi.csv", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["status"] != "published":
                continue
            k = row["kpi_key"]
            if k not in models:
                problems.append(f"semantic models: published KPI {k} has no semantic model on metrics_{k}")
            if not (ROOT / "warehouse" / "models" / "gold" / f"poc_metrics_{k}.sql").exists():
                problems.append(f"semantic models: published KPI {k} has no warehouse/models/gold/poc_metrics_{k}.sql")
    return problems


CONFIG_LINE = re.compile(r"^\s*\{\{\s*config\(.*\)\s*\}\}\s*$")


def model_headers(files: list[Path]) -> list[str]:
    """Every dbt model starts with a header comment; every semantic model and metric has a description
    (docs/quality.md §4)."""
    problems = []
    for f in files:
        if f.suffix == ".sql" and f.parts[:2] == ("warehouse", "models"):
            lines = [
                line
                for line in (ROOT / f).read_text(encoding="utf-8").splitlines()
                if line.strip() and not CONFIG_LINE.match(line)
            ]
            if not lines or not lines[0].strip().startswith(("{#", "--")):
                problems.append(f"{f}: no header comment before the SQL")
        elif f.suffix == ".yml" and f.parts[:2] == ("warehouse", "semantic_models"):
            spec = yaml.safe_load((ROOT / f).read_text(encoding="utf-8")) or {}
            for item in [*(spec.get("semantic_models") or []), *(spec.get("metrics") or [])]:
                if not str(item.get("description") or "").strip():
                    problems.append(f"{f}: {item.get('name')} has no description")
    return problems


def module_docstrings(files: list[Path]) -> list[str]:
    """Every Python module except __init__.py has a module docstring (docs/quality.md §3)."""
    problems = []
    for f in files:
        if f.suffix != ".py" or f.name == "__init__.py":
            continue
        tree = ast.parse((ROOT / f).read_text(encoding="utf-8"))
        if ast.get_docstring(tree) is None:
            problems.append(f"{f}: no module docstring")
    return problems


def main() -> int:
    files = tracked()
    checks = {
        "repository": lambda: repository(files),
        "workflows": lambda: workflows(files),
        "compile": lambda: tool([sys.executable, "-m", "compileall", "-q", *PYTHON_PATHS]),
        "module docstrings": lambda: module_docstrings(files),
        "ruff check": lambda: tool(["ruff", "check", *PYTHON_PATHS]),
        "ruff format": lambda: tool(["ruff", "format", "--check", *PYTHON_PATHS]),
        "yamllint": lambda: tool(["yamllint", "-s", "-d", YAMLLINT, "ingestion", "warehouse", "portal", ".github"]),
        "sqlfluff": sqlfluff,
        "model headers": lambda: model_headers(files),
        "configuration": lambda: configuration(files),
        "scheduler imports": scheduler_imports,
        "backlog": backlog,
        "published contract": published_contract,
        "published v2 seeds": published_v2_seeds,
        "semantic models": semantic_models,
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
