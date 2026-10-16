#!/usr/bin/env python3
"""Data availability tracker: one page per source and layer, published as a pinned GitHub issue.

Measures what exists (read-only): the source declarations (ingestion/sources/*.yaml), the dbt
models in warehouse/models, Landing/Archive folders in R2, the Iceberg tables of the catalog
(rows, snapshots, last write) and the latest Silver/Gold build. Open GitHub issues labelled
`source:<id>` and `layer:<layer>` are shown in their cell, as planned / in progress / problem.

Every catalog table is classified: current (declared and written by today's code), copy
(pass-through copy or old-code table of a source that is not on today's code yet), legacy
(written by retired code, `legacy_tables` in the declaration) or leftover (dbt temp/backup,
or claimed by nobody). Nothing is changed in R2 or the catalog.

Args: --issue <number> (the tracker issue whose body is replaced; without it the page is only
printed and kept in $OPS_OUTPUT_DIR). Needs the R2 credentials of the Ops workflow and, for the
issue, GH_PROJECT_TOKEN with issues write and actions read on this repository.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, datetime
import fnmatch
import os
from pathlib import Path
import re
from typing import Any

import requests
import yaml

from ingestion.common.r2 import catalog, req, s3, sources
from ingestion.common.report import annotate, step_summary
from ops.platform_inventory import folders

ROOT = Path(__file__).resolve().parents[1]
REST = "https://api.github.com"
LAYERS = ("bronze", "silver", "gold")
STAGES = ("landing", "bronze", "silver", "gold", "semantic", "catalog", "portal")
TEMP = re.compile(r"__dbt_(tmp|backup)$")
STALE_DAYS = 3  # a scheduled source whose newest Landing file is older than this is flagged


def table_facts(cat) -> dict[str, dict[str, Any]]:
    """Rows, snapshot count and last write of every Iceberg table (metadata only, no scan)."""
    out = {}
    for layer in LAYERS:
        for identifier in cat.list_tables(layer):
            table = cat.load_table(identifier)
            snapshot = table.current_snapshot()
            summary = snapshot.summary if snapshot is not None and snapshot.summary is not None else {}
            written = datetime.fromtimestamp(snapshot.timestamp_ms / 1000, UTC) if snapshot else None
            out[f"{layer}.{identifier[-1]}"] = {
                "rows": int(summary.get("total-records") or 0),
                "snapshots": len(table.metadata.snapshots),
                "written": written,
            }
    return out


def models() -> dict[str, list[str]]:
    """dbt Silver models per source (folder name = source id)."""
    base = ROOT / "warehouse" / "models" / "silver"
    return {d.name: sorted(p.stem for p in d.glob("*.sql")) for d in sorted(base.iterdir()) if d.is_dir()}


def gold_feeds(silver_models: dict[str, list[str]]) -> dict[str, list[str]]:
    """Shared Gold models per source, from the ref()s of warehouse/models/gold/*.sql."""
    owner = {m: s for s, names in silver_models.items() for m in names}
    feeds: dict[str, set[str]] = defaultdict(set)
    for path in (ROOT / "warehouse" / "models" / "gold").glob("*.sql"):
        for ref in re.findall(r"ref\(\s*['\"]([a-z0-9_]+)['\"]\s*\)", path.read_text()):
            if ref in owner:
                feeds[owner[ref]].add(path.stem)
    return {s: sorted(v) for s, v in feeds.items()}


def issues(token: str, repo: str) -> list[dict[str, Any]]:
    """Open issues (not PRs) with their labels and milestone."""
    out, url = [], f"{REST}/repos/{repo}/issues"
    params: dict[str, Any] | None = {"state": "open", "per_page": 100}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    while url:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        out += [i for i in response.json() if "pull_request" not in i]
        url, params = response.links.get("next", {}).get("url"), None
    return out


def last_build(token: str, repo: str) -> dict[str, Any] | None:
    """The newest finished Silver/Gold build run of the Pipeline."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    response = requests.get(
        f"{REST}/repos/{repo}/actions/workflows/pipeline.yml/runs",
        headers=headers,
        params={"per_page": 50, "status": "completed"},
        timeout=60,
    )
    response.raise_for_status()
    for run in response.json().get("workflow_runs", []):
        if "silver/gold: build" in (run.get("display_title") or ""):
            return run
    return None


def day(moment: datetime | None) -> str:
    return moment.strftime("%d %b") if moment else "–"


def number(rows: int) -> str:
    for unit, size in (("bn", 1e9), ("M", 1e6), ("k", 1e3)):
        if rows >= size:
            return f"{rows / size:.1f}{unit}"
    return str(rows)


def issue_marks(found: list[dict[str, Any]]) -> str:
    """Issue links for a cell: 🔴 problem, 🔵 in progress, ⬜ planned."""
    marks = []
    for issue in sorted(found, key=lambda i: i["number"]):
        labels = {label["name"] for label in issue["labels"]}
        if labels & {"bug", "alert"}:
            icon = "🔴"
        elif "status:in-progress" in labels:
            icon = "🔵"
        else:
            icon = "⬜"
        marks.append(f"{icon} #{issue['number']}")
    return " ".join(marks)


def classify(name: str, registry: dict[str, dict[str, Any]], shared_gold: list[str]) -> tuple[str, str | None]:
    """(class, source) of a catalog table: current, copy, legacy or leftover."""
    if TEMP.search(name):
        return "leftover", None
    if name in shared_gold:
        return "current", "platform"
    layer = name.split(".")[0]
    for source, config in registry.items():  # declared tables first: legacy globs may be broad
        if name in (config.get(layer) or []):
            built = layer == "bronze" and "bronze.load" in (config.get("activities") or [])
            modelled = layer == "silver" and (ROOT / "warehouse" / "models" / "silver" / source).is_dir()
            return ("current" if built or modelled else "copy"), source
    for source, config in registry.items():
        if any(fnmatch.fnmatch(name, g) for g in config.get("legacy_tables") or []):
            return "legacy", source
    return "leftover", None


def main(argv=None) -> int:
    """Measure, build the page, publish it (annotation summary, step summary, tracker issue)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--issue", type=int, default=None)
    args = parser.parse_args(argv)
    token, repo = os.environ.get("GH_PROJECT_TOKEN", ""), os.environ.get("GITHUB_REPOSITORY", "zohelo/zohelo-data")
    now = datetime.now(UTC)

    registry = sources()
    platform = yaml.safe_load((ROOT / "warehouse" / "warehouse.yaml").read_text()) or {}
    shared_gold = list(platform.get("gold") or [])
    silver_models = models()
    feeds = gold_feeds(silver_models)
    client = s3()
    landing_bucket = req("R2_LANDING_BUCKET")
    landing = folders(client, landing_bucket, "01_landing/", 2)
    archive = folders(client, landing_bucket, "05_archive/", 2)
    tables = table_facts(catalog())
    open_issues = issues(token, repo) if token else []
    build = last_build(token, repo) if token else None

    def tagged(source: str, stage: str) -> list[dict[str, Any]]:
        return [
            i for i in open_issues if {f"source:{source}", f"layer:{stage}"} <= {label["name"] for label in i["labels"]}
        ]

    classes: dict[str, list[str]] = defaultdict(list)
    for name in sorted(tables):
        classes[classify(name, registry, shared_gold)[0]].append(name)

    rows = []
    for source, config in registry.items():
        acts = config.get("activities") or []
        schedule = (config.get("extraction") or {}).get("schedule")
        folder = (config.get("landing") or f"01_landing/{source}/").rstrip("/")
        land = landing.get(folder, {"objects": 0, "latest": ""})
        kept = archive.get((config.get("archive_folder") or f"05_archive/{source}/").rstrip("/"), {"objects": 0})
        history = f" · archive {kept['objects']:,}" if kept["objects"] else " · no archive"
        latest = datetime.fromisoformat(land["latest"]).replace(tzinfo=UTC) if land["latest"] else None
        cells: dict[str, str] = {}

        if not acts:
            cells["landing"] = f"⏸ paused, old code · {land['objects']:,} files · {day(latest)}{history}"
        elif latest and schedule and (now - latest).days > STALE_DAYS:
            cells["landing"] = f"⚠️ stale since {day(latest)} · {land['objects']:,} files{history}"
        else:
            how = "scheduled" if schedule else "manual"
            cells["landing"] = f"✅ {how} · {land['objects']:,} files · {day(latest)}{history}"

        def layer_cell(layer: str, ok: str, config: dict[str, Any] = config) -> str:
            declared = [n for n in config.get(layer) or [] if not n.endswith("_rejected_files")]
            present = [n for n in declared if n in tables]
            if not declared:
                return ""
            rows_ = sum(tables[n]["rows"] for n in present)
            written = max((tables[n]["written"] for n in present if tables[n]["written"]), default=None)
            missing = len(declared) - len(present)
            text = f"{ok} · {len(present)}/{len(declared)} tables · {number(rows_)} rows · {day(written)}"
            return text + (f" · {missing} missing" if missing else "")

        bronze_ok = "✅" if "bronze.load" in acts else "⏸ old load"
        cells["bronze"] = layer_cell("bronze", bronze_ok) or "⬜ not built"

        if source in silver_models:
            cells["silver"] = layer_cell("silver", f"✅ modelled ({len(silver_models[source])})") or "⬜ not built"
        elif config.get("silver"):
            cells["silver"] = layer_cell("silver", "🟨 copy, not modelled")
        else:
            cells["silver"] = "⬜ not modelled"

        if source in feeds:
            cells["gold"] = "✅ shared: " + ", ".join(feeds[source])
        elif config.get("gold"):
            cells["gold"] = layer_cell("gold", "🟨 copy, not modelled")
        else:
            cells["gold"] = "⬜ not modelled"

        cells["semantic"] = "⬜"
        cells["catalog"] = "⬜"
        cells["portal"] = "📁 raw files" if land["objects"] else "⬜"
        for stage in STAGES:
            marks = issue_marks(tagged(source, stage))
            if marks:
                cells[stage] = f"{cells[stage]} {marks}".strip()
        rows.append((source, cells))

    gold_rows = [tables[n] for n in shared_gold if n in tables]
    gold_written = max((t["written"] for t in gold_rows if t["written"]), default=None)
    build_line = "no finished build found"
    if build:
        icon = "✅" if build["conclusion"] == "success" else "🔴"
        started = build["created_at"][:16].replace("T", " ")
        build_line = f"{icon} {build['conclusion']} · {started} UTC ({build['html_url']})"
    extra_snapshots = sum(max(0, t["snapshots"] - 1) for t in tables.values())

    lines = [
        "# Data availability tracker",
        "",
        f"_Generated {now:%d %b %Y %H:%M} UTC by `ops/tracker.py` from R2, the catalog, the code and the open "
        "issues. Do not edit by hand: it is replaced on every run._",
        "",
        "**Legend:** ✅ done (current code) · 🟨 copy made by old or pass-through code, not modelled · "
        "⏸ paused · ⚠️ stale · ⬜ not started · 📁 raw files browsable in the portal · "
        "issues: 🔴 problem · 🔵 in progress · ⬜ planned",
        "",
        "## Sources × layers",
        "",
        "| Source | Landing | Bronze | Silver | Gold | Semantic | Catalog | Portal |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for source, cells in rows:
        lines.append(f"| **{source}** | " + " | ".join(cells[s] for s in STAGES) + " |")
    register = yaml.safe_load((ROOT / "ingestion" / "register.yaml").read_text()).get("sources", [])
    unregistered = sum(1 for e in register if not e.get("source_system"))
    lines += [
        "",
        f"Plus **{unregistered} researched sources** in `ingestion/register.yaml` that are not onboarded yet.",
        "",
        "## Platform",
        "",
        f"- **Shared Gold** ({len(gold_rows)}/{len(shared_gold)} tables): last written {day(gold_written)}.",
        f"- **Latest Silver/Gold build:** {build_line}",
        f"- **Catalog tables:** {len(tables)} — current {len(classes['current'])}, "
        f"old-code copies {len(classes['copy'])}, legacy {len(classes['legacy'])}, "
        f"leftovers {len(classes['leftover'])}.",
        f"- **Table history:** {extra_snapshots} snapshots beyond the current one (policy #822: 0).",
        "",
        "## Tables to retire (legacy and leftovers)",
        "",
        "Not written by today's code. Removal goes to the owner as an exact list.",
        "",
    ]
    for kind in ("legacy", "leftover"):
        names = classes[kind]
        if names:
            lines.append(f"<details><summary>{kind}: {len(names)} tables</summary>\n")
            lines += [f"- `{n}` · {number(tables[n]['rows'])} rows · {day(tables[n]['written'])}" for n in names]
            lines.append("\n</details>\n")
    body = "\n".join(lines) + "\n"

    print(body)
    step_summary(body)
    out = Path(os.environ.get("OPS_OUTPUT_DIR", ".")) / "tracker.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(body)
    annotate(
        "notice",
        "tracker",
        {
            "sources": len(rows),
            "tables": {k: len(v) for k, v in classes.items()},
            "extra_snapshots": extra_snapshots,
            "build": build["conclusion"] if build else None,
            "issue": args.issue,
        },
    )
    if args.issue:
        if not token:
            raise RuntimeError("--issue needs GH_PROJECT_TOKEN")
        response = requests.patch(
            f"{REST}/repos/{repo}/issues/{args.issue}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json={"body": body},
            timeout=60,
        )
        response.raise_for_status()
    return 0


def run(argv=None) -> int:
    """main() with failures published as an error annotation (job logs are not readable by AI sessions)."""
    try:
        return main(argv)
    except Exception as exc:
        annotate("error", "tracker failed", {"error": type(exc).__name__, "detail": str(exc)[:3000]})
        raise


if __name__ == "__main__":
    raise SystemExit(run())
