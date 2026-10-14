"""Declarative schedules: start Pipeline runs that are due.

Schedules are declared, not coded: each ``ingestion/sources/<source>.yaml`` (and
``warehouse/warehouse.yaml`` for source ``platform``) may carry

    schedule:
      - cron: "30 11 * * 1-5"          # UTC, 5 fields: minute hour day month weekday
        run: "landing → bronze (all steps)"
        filter: ""                      # optional, as in a manual run

The Pipeline workflow's ``schedule`` trigger (twice an hour; GitHub may drop some) runs ``python run.py schedule``,
which dispatches every run whose cron time fell after the previous scheduled run was
queued, up to this run's queue time (GitHub often starts schedules late or skips them, so a
fixed one-hour window would skip or repeat runs; at most 360 minutes are caught up). The dispatched run is
an ordinary Pipeline run (same plan, authorization and concurrency), so a scheduled run
and a manual run are the same thing. ``schedule: null`` or ``[]`` means manual only.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import os

import requests

from ingestion.common.report import annotate, step_summary

FIELDS = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))


def _field(spec: str, low: int, high: int) -> set[int]:
    values: set[int] = set()
    for part in spec.split(","):
        part, _, step = part.partition("/")
        if part == "*":
            start, end = low, high
        elif "-" in part:
            start, end = (int(x) for x in part.split("-"))
        else:
            start = end = int(part)
        if not (low <= start <= end <= high or (high == 6 and end == 7)):
            raise ValueError(f"cron value out of range: {spec}")
        values.update(range(start, end + 1, int(step or 1)))
    return {0 if high == 6 and v == 7 else v for v in values}


def parse(cron: str) -> list[set[int]]:
    parts = cron.split()
    if len(parts) != 5:
        raise ValueError(f"cron needs 5 fields: {cron!r}")
    return [_field(p, lo, hi) for p, (lo, hi) in zip(parts, FIELDS, strict=False)]


def matches(fields: list[set[int]], when: datetime) -> bool:
    minute, hour, day, month, weekday = fields
    return (
        when.minute in minute
        and when.hour in hour
        and when.day in day
        and when.month in month
        and (when.weekday() + 1) % 7 in weekday
    )


def due(cron: str, now: datetime, window_minutes: int = 60) -> bool:
    """True when the cron time fell in (now - window, now]."""
    fields = parse(cron)
    start = now.replace(second=0, microsecond=0)
    return any(matches(fields, start - timedelta(minutes=m)) for m in range(window_minutes))


def entries(config: dict) -> list[dict]:
    out = []
    for source, cfg in config["sources"].items():
        for item in (cfg.get("extraction") or {}).get("schedule") or []:
            out.append({"source": source, **item})
    for item in config["platform"].get("schedule") or []:
        out.append({"source": "platform", **item})
    return out


def validate(config: dict, plan) -> list[str]:
    """Problems with declared schedules (used by the check job and before dispatching)."""
    problems = []
    for e in entries(config):
        try:
            parse(str(e.get("cron", "")))
            plan(e["source"], e.get("run", ""), config)
        except (ValueError, SystemExit, KeyError) as exc:
            problems.append(f"{e.get('source')}: {e.get('run')!r}: {exc}")
    return problems


DEFAULT_WINDOW, MAX_WINDOW = 60, 360


def _minute(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(second=0, microsecond=0)


def window(repo: str | None, token: str | None, now: datetime) -> tuple[datetime, int, int | None]:
    """(end minute, window minutes, raw gap) for this scheduled run.

    Both ends are the runs' queue times (created_at): the window is (previous run's
    minute, this run's minute], so consecutive runs cover each minute once and a skipped
    or late run is caught up by the next one. A failed previous run still counts as
    covering its minutes. Unknown history: the last 60 minutes up to now."""
    fallback = (now.replace(second=0, microsecond=0), DEFAULT_WINDOW, None)
    if not repo or not token:
        return fallback
    try:
        response = requests.get(
            f"https://api.github.com/repos/{repo}/actions/workflows/pipeline.yml/runs",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            params={"event": "schedule", "per_page": 10},
            timeout=30,
        )
        response.raise_for_status()
        runs = response.json()["workflow_runs"]
        current_id = os.environ.get("GITHUB_RUN_ID", "")
        current = [r for r in runs if str(r["id"]) == current_id]
        earlier = [r for r in runs if str(r["id"]) != current_id]
        if not current or not earlier:
            return fallback
        end = _minute(current[0]["created_at"])
        start = max(_minute(r["created_at"]) for r in earlier if _minute(r["created_at"]) < end)
    except (requests.RequestException, ValueError, KeyError, TypeError):
        return fallback
    gap = int((end - start).total_seconds() // 60)
    return end, max(1, min(MAX_WINDOW, gap)), gap


def main(config: dict, plan, now: datetime | None = None) -> int:
    now = now or datetime.now(UTC)
    problems = validate(config, plan)
    started, skipped = [], []
    repo, token = os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GITHUB_TOKEN")
    end, minutes, gap = window(repo, token, now)
    for e in entries(config):
        if not due(str(e["cron"]), end, minutes):
            continue
        inputs = {"source": e["source"], "run": e["run"], "filter": str(e.get("filter") or "")}
        if not repo or not token:
            skipped.append({**inputs, "reason": "no GITHUB_REPOSITORY/GITHUB_TOKEN"})
            continue
        response = requests.post(
            f"https://api.github.com/repos/{repo}/actions/workflows/pipeline.yml/dispatches",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json={"ref": "main", "inputs": inputs},
            timeout=30,
        )
        (started if response.status_code == 204 else skipped).append(
            {**inputs, **({} if response.status_code == 204 else {"reason": response.text[:200]})}
        )
    summary = {
        "at": now.isoformat(timespec="minutes"),
        "window": {"end": end.isoformat(timespec="minutes"), "minutes": minutes, "gap_minutes": gap},
        "started": started,
        "skipped": skipped,
        "invalid_schedules": problems,
    }
    clamped = gap is not None and gap > MAX_WINDOW
    if clamped:
        summary["warning"] = f"{gap - MAX_WINDOW} minutes before the window were not checked"
    annotate("error" if skipped or problems else "warning" if clamped else "notice", "scheduler", summary)
    step_summary(f"## scheduler\n\n```json\n{json.dumps(summary, indent=2, ensure_ascii=False)}\n```\n")
    return 1 if skipped or problems else 0
