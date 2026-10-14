"""Pipeline entry point: plan a run, or execute one activity for one source.

    python run.py plan --source eurostat --run "landing → bronze (all steps)"
    python run.py activity --source eurostat --activity landing.extract [--filter ...]
    python run.py schedule [--check]    start the declared runs that are due (hourly trigger);
                                        --check validates the source declarations and schedules

A run is an ordered list of activities (RUNS). An activity is allowed only when it is
implemented and authorized:
  implemented  ingestion/to_landing/<source>/__init__.py (landing.inspect/extract/verify)
               ingestion/to_bronze/<source>/__init__.py (bronze.*, landing.archive)
               PLATFORM_ACTIVITIES below (silver_gold.promote/build, serving.refresh_index)
  authorized   ingestion/sources/<source>.yaml "activities";
               warehouse/warehouse.yaml and portal/portal.yaml for platform activities
Activities are idempotent, so a scheduled run and a manual run are the same thing and a
failed run resumes.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import importlib
import json
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parent
SOURCES = ROOT / "ingestion" / "sources"
PLATFORM_CONFIG = (ROOT / "warehouse" / "warehouse.yaml", ROOT / "portal" / "portal.yaml")

# Activity order is the dataflow order; the workflow has one job per activity.
ORDER = ("landing.inspect", "landing.extract", "landing.verify", "bronze.load",
         "bronze.verify", "landing.archive", "archive.prune", "silver_gold.promote",
         "silver_gold.build", "serving.refresh_index")
WRITES = {"landing.extract", "bronze.load", "landing.archive", "archive.prune", "silver_gold.promote",
          "silver_gold.build"}

RUNS = {
    "landing: inspect": ["landing.inspect"],
    "landing: extract": ["landing.extract"],
    "landing: verify": ["landing.verify"],
    "bronze: load": ["bronze.load"],
    "bronze: verify": ["bronze.verify"],
    "archive: move Bronze-loaded Landing": ["landing.archive"],
    "archive: apply retention": ["archive.prune"],
    "landing → bronze (all steps)": ["landing.inspect", "landing.extract", "landing.verify",
                                     "bronze.load", "bronze.verify", "landing.archive", "archive.prune"],
    "silver/gold: promote": ["silver_gold.promote"],
    "silver/gold: build models": ["silver_gold.build"],
    "serving: refresh portal index": ["serving.refresh_index"],
}


@dataclass
class Params:
    filter: str = ""
    max_items: int = 0
    parallel_jobs: int = 1
    job_index: int = 0
    parallel_downloads: int = 4
    budget_minutes: float = 300.0


def _promote(p: Params) -> int:
    from warehouse.passthrough import promote, zero_copy
    promote.main()
    zero_copy.main()
    return 0


def _refresh_index(p: Params) -> int:
    from portal import publish_index
    return publish_index.main() or 0


def _build(p: Params) -> int:
    from warehouse import build
    return build.main(p.filter)


PLATFORM_ACTIVITIES = {"silver_gold.promote": _promote, "silver_gold.build": _build,
                       "serving.refresh_index": _refresh_index}


def load_config() -> dict:
    sources = {path.stem: yaml.safe_load(path.read_text())
               for path in sorted(SOURCES.glob("*.yaml"))}
    platform = [a for path in PLATFORM_CONFIG
                for a in (yaml.safe_load(path.read_text()).get("activities") or [])]
    schedule = [e for path in PLATFORM_CONFIG
                for e in (yaml.safe_load(path.read_text()).get("schedule") or [])]
    return {"sources": sources, "platform": {"activities": platform, "schedule": schedule}}


SOURCE_KEYS = {"source_system", "description", "extraction", "landing", "archive_policy", "archive_folder",
               "bronze", "silver", "gold", "legacy_tables", "activities"}
EXTRACTION_KEYS = {"initial", "production", "incremental_policy", "schedule"}


def config_problems(config: dict) -> list[str]:
    """Problems with the source declarations (one documented key set; docs/operating.md)."""
    problems = []
    for source, cfg in config["sources"].items():
        cfg = cfg or {}
        unknown = set(cfg) - SOURCE_KEYS
        missing = {"source_system", "description", "landing", "bronze", "activities"} - set(cfg)
        if unknown:
            problems.append(f"{source}: unknown keys {sorted(unknown)}")
        if missing:
            problems.append(f"{source}: missing keys {sorted(missing)}")
        if cfg.get("source_system") != source:
            problems.append(f"{source}: source_system must equal the file name")
        if set(cfg.get("extraction") or {}) - EXTRACTION_KEYS:
            problems.append(f"{source}: unknown extraction keys {sorted(set(cfg['extraction']) - EXTRACTION_KEYS)}")
        for layer in ("bronze", "silver", "gold"):
            bad = [t for t in cfg.get(layer) or [] if not str(t).startswith(f"{layer}.")]
            if bad:
                problems.append(f"{source}: {layer} lists tables of another layer: {bad}")
        if "bronze.load" in (cfg.get("activities") or []):
            policy = cfg.get("archive_policy") or {}
            if not {"move", "retention"} <= set(policy):
                problems.append(f"{source}: bronze.load needs archive_policy with move and retention")
            if not any(str(t).endswith("rejected_files") for t in cfg.get("bronze") or []):
                problems.append(f"{source}: bronze.load needs a *_rejected_files table in bronze")
    return problems


def implemented(source: str) -> dict:
    if source == "platform":
        return PLATFORM_ACTIVITIES
    code = {}
    for package in ("ingestion.to_landing", "ingestion.to_bronze"):
        try:
            module = importlib.import_module(f"{package}.{source}")
        except ModuleNotFoundError as error:
            if error.name != f"{package}.{source}":
                raise
            continue
        code.update(module.ACTIVITIES)
    return code


def authorized(config: dict, source: str) -> list[str]:
    entry = config["platform"] if source == "platform" else config["sources"][source]
    return list(entry.get("activities") or [])


def plan(source: str, run: str, config: dict) -> list[str]:
    if source != "platform" and source not in config["sources"]:
        raise SystemExit(f"unknown source {source!r}; sources: {sorted(config['sources'])}")
    if run not in RUNS:
        raise SystemExit(f"unknown run {run!r}")
    activities = RUNS[run]
    platform_run = all(a in PLATFORM_ACTIVITIES for a in activities)
    if platform_run != (source == "platform"):
        raise SystemExit("silver/gold and serving runs use source 'platform'; "
                         "landing/bronze runs use a data source")
    code, allowed = implemented(source), authorized(config, source)
    refused = [a for a in activities if a not in code or a not in allowed]
    if refused:
        raise SystemExit(
            f"{source}: {', '.join(refused)} not available "
            f"(implemented: {sorted(code)}; authorized: {allowed})")
    steps = sorted(activities, key=ORDER.index)
    if source != "platform" and WRITES & set(steps):
        steps.append("serving.refresh_index")
    return steps


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p_plan = sub.add_parser("plan", help="print the activities of a run as JSON")
    p_plan.add_argument("--source", required=True)
    p_plan.add_argument("--run", required=True)
    p_plan.add_argument("--parallel-jobs", type=int, default=1)
    p_act = sub.add_parser("activity", help="execute one activity")
    p_act.add_argument("--source", required=True)
    p_act.add_argument("--activity", required=True, choices=ORDER)
    p_act.add_argument("--filter", default="")
    p_act.add_argument("--max-items", type=int, default=0)
    p_act.add_argument("--parallel-jobs", type=int, default=1)
    p_act.add_argument("--job-index", type=int, default=0)
    p_act.add_argument("--parallel-downloads", type=int, default=4)
    p_act.add_argument("--budget-minutes", type=float, default=300.0)
    p_sched = sub.add_parser("schedule", help="dispatch the declared runs that are due")
    p_sched.add_argument("--check", action="store_true", help="only validate the declared schedules")
    args = parser.parse_args(argv)
    config = load_config()

    if args.command == "schedule":
        from ingestion.common import scheduler
        if args.check:
            problems = config_problems(config) + scheduler.validate(config, plan)
            print(json.dumps({"schedules": len(scheduler.entries(config)), "problems": problems}))
            return 1 if problems else 0
        return scheduler.main(config, plan)

    if args.command == "plan":
        if not 1 <= args.parallel_jobs <= 8:
            parser.error("parallel jobs must be 1-8")
        steps = plan(args.source, args.run, config)
        print(json.dumps({"activities": steps,
                          "extract_matrix": list(range(args.parallel_jobs))}))
        return 0

    # Platform activities run inside a data-source run (portal index refresh after writes)
    # or as a platform run; data-source activities must be authorized for that source.
    if args.activity in PLATFORM_ACTIVITIES:
        if args.activity not in authorized(config, "platform"):
            parser.error(f"{args.activity} is not authorized for platform")
        func = PLATFORM_ACTIVITIES[args.activity]
    else:
        if args.source == "platform" or args.source not in config["sources"]:
            parser.error(f"unknown data source {args.source!r}")
        if args.activity not in authorized(config, args.source):
            parser.error(f"{args.activity} is not authorized for {args.source} (ingestion/sources/{args.source}.yaml)")
        code = implemented(args.source)
        if args.activity not in code:
            parser.error(f"{args.activity} is not implemented for {args.source}")
        func = code[args.activity]
    params = Params(args.filter, args.max_items, args.parallel_jobs, args.job_index,
                    args.parallel_downloads, args.budget_minutes)
    print(json.dumps({"pipeline": args.source, "activity": args.activity, **vars(params)}), flush=True)
    return int(func(params) or 0)


if __name__ == "__main__":
    sys.exit(main())
