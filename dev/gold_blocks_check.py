"""Read-only check for #2903: the changed-only plan with this branch's code against production, row counts and
snapshot times of the Gold facts and blocks (Iceberg metadata), the last build_check, and the geography map
compiled and counted with UNESCO UIS waiting. Writes nothing to R2."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build, changed

PROJECT = Path("warehouse").resolve()
DBT = ["--target", "prod", "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]


def ts(ms: int | None) -> str | None:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%m-%d %H:%M") if ms else None


def dbt(args: list[str]) -> tuple[int, str]:
    done = subprocess.run(["dbt", *args, *DBT], cwd=PROJECT, env={**os.environ}, text=True, capture_output=True)
    return done.returncode, done.stdout


def show(sql: str) -> list[dict] | str:
    code, out = dbt(["show", "--inline", sql, "--limit", "500", "--output", "json", "--log-format", "json"])
    for line in out.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        preview = event.get("data", {}).get("preview")
        if preview:
            return json.loads(preview) if isinstance(preview, str) else preview
    return f"exit {code}: {out[-700:]}"


def main() -> None:
    cat = catalog()
    meta = changed.manifest("prod")
    graph = {m["name"]: m for m in changed.models(meta).values()}

    # 1. The plan with this branch's code.
    plan = changed.plan(cat, meta)
    kinds: dict[str, int] = {}
    for reason in plan["reasons"].values():
        kinds[reason.split(":")[0]] = kinds.get(reason.split(":")[0], 0) + 1
    blocks = sorted(n for n, m in graph.items() if m["block"])
    annotate(
        "notice",
        "1 plan with the fix",
        {
            "waiting": plan["waiting"],
            "empty_blocks": plan["empty_blocks"],
            "stalled": plan["stalled"],
            "select_count": len(plan["select"]),
            "why": kinds,
            "blocks_selected": [b for b in blocks if b in plan["select"]],
            "map_reason": plan["reasons"].get("poc_map_geography"),
        },
    )

    # 2. Rows and snapshot times (metadata only).
    names = [
        "poc_fact_annual", "poc_fact_quarterly", "poc_fact_monthly", "poc_fact_daily",
        "poc_dim_series", "poc_dim_measure", "poc_map_geography", "poc_build_check", *blocks,
    ]  # fmt: skip
    rows = {}
    for name in names:
        model = graph[name]
        try:
            table = cat.load_table((model["namespace"], model["table"]))
        except Exception as exc:
            rows[name] = type(exc).__name__
            continue
        snapshot = table.current_snapshot()
        ceiling = (build.FULL_REBUILD.get(name) or {}).get("rows")
        history = [
            [ts(s.timestamp_ms), int(s.summary.get("total-records") or -1) if s.summary else -1]
            for s in table.snapshots()[-4:]
        ]
        rows[name] = {
            "rows": build.table_rows(table),
            "ceiling": ceiling,
            "snapshot": ts(snapshot.timestamp_ms) if snapshot else None,
            "built_from": ts(int(table.properties[changed.BUILT_FROM])) if changed.BUILT_FROM in table.properties else None,
            "history": history if len(history) > 1 else None,
        }
    compact = {
        n: v if isinstance(v, str) else [v["rows"], v["ceiling"], v["snapshot"], v["built_from"]]
        for n, v in rows.items()
    }
    annotate("notice", "2 rows [rows, ceiling, snapshot UTC, built_from UTC]", compact)
    annotate("notice", "2b snapshot history", {n: v["history"] for n, v in rows.items() if not isinstance(v, str) and v["history"]})

    # 3. The last build_check: rows per source as the facts' inputs were read by that build.
    try:
        model = graph["poc_build_check"]
        check = cat.load_table((model["namespace"], model["table"])).scan().to_arrow().to_pylist()
        per: dict[str, list] = {}
        for r in check:
            entry = per.setdefault(r["source_system"], [0, 0, 0, None])
            entry[0] += 1
            entry[1] += int(r["row_count"] > 0)
            entry[2] += r["row_count"]
            if r["note"] and r["note"].startswith("waiting"):
                entry[3] = "waiting"
        annotate("notice", "3 build_check per source [blocks, blocks with rows, rows, waiting]", per)
    except Exception as exc:
        annotate("warning", "3 build_check", {"error": f"{type(exc).__name__}: {str(exc)[:300]}"})

    # 4. The map compiled with UNESCO UIS waiting, and without the declaration.
    compiled = PROJECT / "target/compiled/zohelo/models/gold/poc_map_geography.sql"
    waiting = json.dumps({"topics": build.TOPICS, "waiting_models": plan["waiting"]})
    code, out = dbt(["compile", "--select", "poc_map_geography", "--vars", waiting])
    sql = compiled.read_text() if code == 0 and compiled.exists() else ""
    code2, out2 = dbt(["compile", "--select", "poc_map_geography", "--vars", json.dumps({"topics": build.TOPICS})])
    result = {
        "compile_waiting_exit": code,
        "empty_relations": sql.count("where false)"),
        "unesco_part": sql[sql.find("unesco_uis as (") :][:420] if sql else out[-500:],
        "compile_undeclared_exit": code2,
        "undeclared_message": next((line.strip()[:300] for line in out2.splitlines() if "Optional input" in line), out2[-300:]),
    }
    annotate("notice", "4 map compiled", result)
    if sql:
        new = show(f"select source_system, count(*) as n, count(*) filter (where geography_key = '_X') as x from ({sql}) group by 1 order by 1")
        model = graph["poc_map_geography"]
        old = show(f"select source_system, count(*) as n, count(*) filter (where geography_key = '_X') as x from r2.{model['namespace']}.{model['table']} group by 1 order by 1")
        fmt = lambda t: {r["source_system"]: [r["n"], r["x"]] for r in t} if isinstance(t, list) else t  # noqa: E731
        annotate("notice", "5 map rows per source [rows, unmatched]: new SQL vs production table", {"new": fmt(new), "production": fmt(old)})


if __name__ == "__main__":
    main()
