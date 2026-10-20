#!/usr/bin/env python3
"""Data availability tracker: one page per source and layer, published as a pinned GitHub issue.

Measures what exists (read-only): the source declarations (ingestion/sources/*.yaml), the dbt
models in warehouse/models and semantic models in warehouse/semantic_models, Landing/Archive
folders in R2, the Iceberg tables of the catalog (rows, snapshots, last write), the published
data catalog file and the newest finished Pipeline runs (per source and the Silver/Gold build).
A cell is ✅ only when every declared table exists. Open GitHub issues labelled
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
from importlib import import_module
import json
import os
from pathlib import Path
import re
from typing import Any

import requests
import yaml

from ingestion.common.bronze_files import retry
from ingestion.common.r2 import catalog, get_with_backoff, req, s3, sources
from ingestion.common.report import annotate, step_summary
from ops.platform_inventory import folders

ROOT = Path(__file__).resolve().parents[1]
REST = "https://api.github.com"
LAYERS = ("bronze", "silver", "gold")
STAGES = ("landing", "bronze", "silver", "gold", "semantic", "catalog", "portal")
TEMP = re.compile(r"__dbt_(tmp|backup)$")
RUN_PAGES = 10  # up to 1,000 finished Pipeline runs, newest first
CATALOG_KEY = "_portal_catalog/v1/catalog.json"


# Sources whose loaders replace whole entities (design #875): their Bronze and Silver cells show
# completeness against the layer below, measured from table metadata.
ENTITY_SOURCES = {
    "eurostat": "ingestion.to_bronze.eurostat.entities",
    "gus_bdl": "ingestion.to_bronze.gus_bdl.entities",
}


def completeness_cell(report: dict[str, Any], layer: str, against: str) -> str:
    """'✅ 100% of … · n/m · rows' — ✅ at 99% or more (owner rule R6), ⚠️ below, ❌ at 0."""
    pct = report[f"{layer}_pct"]
    done = report[f"{layer}_complete"]
    total = report["entities"] if layer == "bronze" else report["bronze_complete"]
    icon = "✅" if pct >= 99 else ("⚠️" if done else "❌")
    extra = f" · {report['rejected_count']} rejected" if layer == "bronze" and report.get("rejected_count") else ""
    return f"{icon} {pct:g}% {against} · {done:,}/{total:,} · {number(report[f'{layer}_rows'])} rows{extra}"


def table_facts(cat) -> dict[str, dict[str, Any]]:
    """Rows, snapshot count and last write of every Iceberg table (metadata only, no scan)."""
    out = {}
    for layer in LAYERS:
        for identifier in retry(lambda layer=layer: cat.list_tables(layer), f"list {layer}"):
            table = retry(lambda identifier=identifier: cat.load_table(identifier), f"load {identifier}")
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


def runs(token: str, repo: str) -> list[dict[str, Any]]:
    """The newest finished Pipeline runs, newest first."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    out: list[dict[str, Any]] = []
    for page in range(1, RUN_PAGES + 1):
        response = requests.get(
            f"{REST}/repos/{repo}/actions/workflows/pipeline.yml/runs",
            headers=headers,
            params={"per_page": 100, "status": "completed", "page": page},
            timeout=60,
        )
        response.raise_for_status()
        batch = response.json().get("workflow_runs", [])
        out += batch
        if len(batch) < 100:
            break
    return out


def run_mark(run: dict[str, Any] | None, since: str = "") -> str:
    """✅ / 🔴 / ⚪ and the date of a finished run."""
    if not run:
        return f"no Pipeline run since {since}" if since else "no Pipeline run on record"
    icon = {"success": "✅", "cancelled": "⚪", "skipped": "⚪"}.get(run["conclusion"], "🔴")
    return f"{icon} [{run['conclusion']} {run['created_at'][8:10]}.{run['created_at'][5:7]}]({run['html_url']})"


def semantic_models() -> dict[str, list[str]]:
    """Gold model → semantic models built on it (warehouse/semantic_models/*.yml)."""
    out: dict[str, list[str]] = defaultdict(list)
    for path in sorted((ROOT / "warehouse" / "semantic_models").glob("*.yml")):
        for model in (yaml.safe_load(path.read_text()) or {}).get("semantic_models") or []:
            found = re.search(r"ref\(\s*['\"]([a-z0-9_]+)['\"]\s*\)", str(model.get("model", "")))
            if found:
                out[found.group(1)].append(model.get("name", path.stem))
    return out


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
    semantic = semantic_models()
    client = s3()
    landing_bucket, lakehouse_bucket = req("R2_LANDING_BUCKET"), req("R2_LAKEHOUSE_BUCKET")
    landing = folders(client, landing_bucket, "01_landing/", 2)
    archive = folders(client, landing_bucket, "05_archive/", 2)
    tables = table_facts(catalog())
    try:
        catalog_doc = json.loads(get_with_backoff(client, lakehouse_bucket, CATALOG_KEY))
        catalog_written = client.head_object(Bucket=lakehouse_bucket, Key=CATALOG_KEY)["LastModified"]
        catalog_tables: set[str] | None = {
            t["table"].split(".", 1)[-1] if t["table"].count(".") > 1 else t["table"]
            for t in catalog_doc.get("tables") or []
            if isinstance(t, dict) and t.get("table")
        }
    except Exception as exc:  # the page still renders; the catalog column says why
        catalog_tables, catalog_written = None, None
        annotate("warning", "tracker: data catalog not readable", {"key": CATALOG_KEY, "error": str(exc)[:300]})
    open_issues = issues(token, repo) if token else []
    finished = runs(token, repo) if token else []

    history_start = f"{finished[-1]['created_at'][8:10]}.{finished[-1]['created_at'][5:7]}" if finished else ""

    def tagged(source: str, stage: str) -> list[dict[str, Any]]:
        return [
            i for i in open_issues if {f"source:{source}", f"layer:{stage}"} <= {label["name"] for label in i["labels"]}
        ]

    def newest(prefix: str) -> dict[str, Any] | None:
        return next((r for r in finished if (r.get("display_title") or "").startswith(prefix)), None)

    classes: dict[str, list[str]] = defaultdict(list)
    for name in sorted(tables):
        classes[classify(name, registry, shared_gold)[0]].append(name)

    def measured(names: list[str], label: str) -> str:
        """'<✅|⚠️> label · n/m tables · rows · last write' for declared tables, ✅ only if all exist."""
        present = [n for n in names if n in tables]
        rows_ = sum(tables[n]["rows"] for n in present)
        written = max((tables[n]["written"] for n in present if tables[n]["written"]), default=None)
        icon = "✅" if present and len(present) == len(names) else ("⚠️" if present else "❌")
        if label.startswith("🟨"):  # written by old code: one icon, 🟨 when complete
            label = label[1:].lstrip()
            icon = "🟨" if icon == "✅" else icon
        missing = len(names) - len(present)
        text = f"{icon} {label} · {len(present)}/{len(names)} · {number(rows_)} rows · {day(written)}"
        return text + (f" · {missing} missing" if missing else "")

    rows = []
    for source, config in registry.items():
        acts = config.get("activities") or []
        schedule = (config.get("extraction") or {}).get("schedule")
        folder = (config.get("landing") or f"01_landing/{source}/").rstrip("/")
        land = landing.get(folder, {"objects": 0, "latest": ""})
        latest = datetime.fromisoformat(land["latest"]).replace(tzinfo=UTC) if land["latest"] else None
        kept = archive.get((config.get("archive_folder") or f"05_archive/{source}/").rstrip("/"), {"objects": 0})
        history = f" · archive {kept['objects']:,}" if kept["objects"] else " · no archive"
        cells: dict[str, str] = {}

        if not acts:
            cells["landing"] = f"⏸ paused, old code · {land['objects']:,} objects · new content {day(latest)}{history}"
        else:
            how = "scheduled" if schedule else "manual only"
            last = run_mark(newest(f"Pipeline · {source} · "), history_start)
            cells["landing"] = (
                f"{how} · last run {last} · {land['objects']:,} objects · new content {day(latest)}{history}"
            )

        bronze = [n for n in config.get("bronze") or [] if not n.endswith("_rejected_files")]
        if bronze:
            label = "loaded by current code" if "bronze.load" in acts else "🟨 old-code copy"
            cells["bronze"] = measured(bronze, label)
        else:
            cells["bronze"] = "⬜ not built"

        silver = list(config.get("silver") or [])
        if source in silver_models and silver:
            cells["silver"] = measured(silver, f"modelled ({len(silver_models[source])} dbt)")
        elif silver:
            cells["silver"] = measured(silver, "🟨 old-code copy, not modelled")
        else:
            cells["silver"] = "⬜ not modelled"

        if source in feeds:
            names = [f"gold.{m}" for m in feeds[source]]
            cells["gold"] = measured(names, "shared: " + ", ".join(feeds[source]))
        elif config.get("gold"):
            cells["gold"] = measured(list(config["gold"]), "🟨 old-code copy, not modelled")
        else:
            cells["gold"] = "⬜ not modelled"

        metrics = sorted({m for g in feeds.get(source, []) for m in semantic.get(g, [])})
        cells["semantic"] = ("✅ " + ", ".join(metrics)) if metrics else "⬜ none"
        # dbt-built tables of the source: its Silver models and the shared Gold they feed
        own = [f"silver.{m}" for m in silver_models.get(source, [])] + [f"gold.{m}" for m in feeds.get(source, [])]
        if not own:
            cells["catalog"] = "⬜ not modelled"
        elif catalog_tables is None:
            cells["catalog"] = "❔ catalog file not readable"
        else:
            listed = [n for n in own if n in catalog_tables or n.split(".")[1] in catalog_tables]
            icon = "✅" if len(listed) == len(own) else ("⚠️" if listed else "❌")
            cells["catalog"] = f"{icon} {len(listed)}/{len(own)} · {day(catalog_written)}"
        cells["portal"] = "📁 raw files only" if land["objects"] else "⬜"
        for stage in STAGES:
            marks = issue_marks(tagged(source, stage))
            if marks:
                cells[stage] = f"{cells[stage]} {marks}".strip()
        if source in ENTITY_SOURCES:  # completeness against the layer below, not table existence (#875)
            try:
                report = import_module(ENTITY_SOURCES[source]).completeness_report()
                cells["bronze"] = completeness_cell(report, "bronze", "of newest Landing versions")
                cells["silver"] = completeness_cell(report, "silver", "of Bronze")
            except Exception as exc:  # the page still renders; the cell says why
                cells["bronze"] += f" · ❔ completeness not measured ({type(exc).__name__})"
        rows.append((source, cells))

    gold_rows = [tables[n] for n in shared_gold if n in tables]
    gold_written = max((t["written"] for t in gold_rows if t["written"]), default=None)
    build = newest("Pipeline · platform · silver/gold: build")
    extra_snapshots = sum(max(0, t["snapshots"] - 1) for t in tables.values())
    register = yaml.safe_load((ROOT / "ingestion" / "register.yaml").read_text()).get("sources", [])
    not_onboarded = sum(1 for e in register if not e.get("source_system"))

    lines = [
        "# Data availability tracker",
        "",
        f"_Generated {now:%d %b %Y %H:%M} UTC by `ops/tracker.py` from R2, the catalog, the code, Pipeline runs "
        "and the open issues. Do not edit by hand: it is replaced on every run._",
        "",
        "**Legend:** ✅ done and measured · ⚠️ partly there · ❌ declared but missing · "
        "🟨 old-code copy, not on current code · ⏸ paused · ⬜ not started · ⚪ cancelled run · "
        "📁 Landing files browsable in the portal (tables are not in the portal yet) · "
        "issues: 🔴 problem · 🔵 in progress · ⬜ planned. "
        '"New content" is the newest Landing object: a run that finds unchanged bytes writes nothing.',
        "",
        "## Sources × layers",
        "",
        "| Source | Landing | Bronze | Silver | Gold | Semantic layer | Data catalog | Portal |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for source, cells in rows:
        lines.append(f"| **{source}** | " + " | ".join(cells[s] for s in STAGES) + " |")
    lines += [
        "",
        f"Plus **{not_onboarded} researched sources** in `ingestion/register.yaml` that are not onboarded yet.",
        "",
        "## Platform",
        "",
        f"- **Shared Gold** ({len(gold_rows)}/{len(shared_gold)} tables): last written {day(gold_written)}.",
        f"- **Latest Silver/Gold build:** {run_mark(build)}",
        f"- **Data catalog file** `{CATALOG_KEY}`: written {day(catalog_written)}.",
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
