"""Declarative schedules: start Pipeline runs that are due.

Schedules are declared, not coded: each ``ingestion/sources/<source>.yaml`` (and
``warehouse/warehouse.yaml`` for source ``platform``) may carry

    schedule:
      - cron: "30 11 * * 1-5"          # UTC, 5 fields: minute hour day month weekday
        run: "landing → bronze (all steps)"
        filter: ""                      # optional, as in a manual run

The Pipeline workflow's ``schedule`` trigger (twice an hour; GitHub drops many, #802) or a
dispatch of run ``scheduler: dispatch due runs`` runs ``python run.py schedule``, which
dispatches every run whose cron time fell after the previous scheduler run was queued, up
to this run's queue time (GitHub often starts schedules late or skips them, so a fixed
one-hour window would skip or repeat runs; a long gap is walked in 360-minute chunks). A
weekly or monthly schedule whose latest slot has no successful run since (the dispatched run
failed, or a scheduler run failed) is dispatched again, at most twice per slot (#839). The
dispatched run is an ordinary Pipeline run (same plan, authorization and concurrency), so
a scheduled run and a manual run are the same thing. ``schedule: null`` or ``[]`` means
manual only.
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
DISPATCH_RUN = "scheduler: dispatch due runs"  # Pipeline run that starts the scheduler by dispatch (#802)


def _minute(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(second=0, microsecond=0)


def window(repo: str | None, token: str | None, now: datetime) -> tuple[datetime, int, int | None, datetime | None]:
    """(end minute, window minutes, raw gap, start minute) for this scheduler run.

    Scheduler runs are schedule events and dispatches of DISPATCH_RUN (their run name ends
    with it), so the two triggers share one window and never repeat a minute. Both ends
    are the runs' queue times (created_at): the window is (previous run's minute, this
    run's minute], so consecutive runs cover each minute once and a skipped
    or late run is caught up by the next one. Runs are ordered by queue time, then id; when the
    previous run was queued in the same minute the window is empty. A failed previous run
    still counts as covering its minutes; a cancelled one (replaced while queued) does not;
    scheduler dispatches count only on main. Unknown history: the last 60 minutes up to now.

    When the raw gap between two scheduler runs exceeds MAX_WINDOW minutes, only the last
    MAX_WINDOW minutes are returned as the window; the caller is responsible for walking the
    rest of the gap in MAX_WINDOW chunks so weekly/monthly cron slots are never skipped
    (#839)."""
    fallback = (now.replace(second=0, microsecond=0), DEFAULT_WINDOW, None, None)
    if not repo or not token:
        return fallback
    try:
        runs = []
        for event in ("schedule", "workflow_dispatch"):
            response = requests.get(
                f"https://api.github.com/repos/{repo}/actions/workflows/pipeline.yml/runs",
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                params={"event": event, "per_page": 10 if event == "schedule" else 50},
                timeout=30,
            )
            response.raise_for_status()
            runs += [
                r
                for r in response.json()["workflow_runs"]
                if event == "schedule"
                or (r.get("head_branch") == "main" and str(r.get("display_title", "")).endswith(DISPATCH_RUN))
            ]
        # A run cancelled while queued (a newer one replaced it in the concurrency group) never ran.
        runs = [r for r in runs if r.get("conclusion") != "cancelled"]
        current_id = os.environ.get("GITHUB_RUN_ID", "")
        current = [r for r in runs if str(r["id"]) == current_id]
        if not current:
            return fallback
        key = lambda r: (datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")), int(r["id"]))  # noqa: E731
        earlier = [r for r in runs if key(r) < key(current[0])]
        if not earlier:
            return fallback
        end = _minute(current[0]["created_at"])
        start = _minute(max(earlier, key=key)["created_at"])
    except (requests.RequestException, ValueError, KeyError, TypeError):
        return fallback
    gap = int((end - start).total_seconds() // 60)
    # gap 0: the previous scheduler run was queued in this same minute and already covered it.
    return end, max(0, min(MAX_WINDOW, gap)), gap, start


def _windows(end: datetime, start: datetime | None, minutes: int, gap: int | None) -> list[tuple[datetime, int]]:
    """(end minute, minutes) chunks covering (start, end] in MAX_WINDOW steps.

    A single chunk when gap <= MAX_WINDOW; otherwise walk back from ``end`` in
    MAX_WINDOW chunks so a cron slot anywhere in the gap is checked (#839)."""
    if start is None or gap is None or gap <= MAX_WINDOW:
        return [(end, minutes)]
    chunks: list[tuple[datetime, int]] = []
    chunk_end = end
    while chunk_end > start:
        chunk_start = max(start, chunk_end - timedelta(minutes=MAX_WINDOW))
        chunks.append((chunk_end, int((chunk_end - chunk_start).total_seconds() // 60)))
        chunk_end = chunk_start
    return chunks


CATCH_UP_ATTEMPTS = 2  # runs per weekly/monthly slot before the alert issue is the only signal
CATCH_UP_DAYS = 32  # how far back the latest slot of a periodic schedule is searched


def periodic(cron: str) -> bool:
    """A schedule that does not fire every day (day-of-month or weekday restricted)."""
    _, _, day, _, weekday = parse(cron)
    return len(day) < 31 or len(weekday) < 7


def last_slot(cron: str, end: datetime) -> datetime | None:
    """The latest cron minute at or before ``end`` within CATCH_UP_DAYS."""
    fields = parse(cron)
    day = end.replace(second=0, microsecond=0)
    for back in range(CATCH_UP_DAYS + 1):
        date = day - timedelta(days=back)
        for hour in sorted(fields[1], reverse=True):
            for minute in sorted(fields[0], reverse=True):
                when = date.replace(hour=hour, minute=minute)
                if when <= end and matches(fields, when):
                    return when
    return None


def pipeline_runs(repo: str, token: str, since: datetime, until: datetime) -> dict[str, list[dict]]:
    """Dispatched Pipeline runs created from ``since``'s day to ``until``'s day, by run name.

    Read one day at a time: the runs API returns at most 1,000 results per query, and the
    scheduler's own dispatches add ~48 runs a day, so one query over weeks would drop the
    oldest runs (#855 review). A day that fills every page raises (history incomplete)."""
    out: dict[str, list[dict]] = {}
    day = since.date()
    while day <= until.date():
        for page in range(1, 11):
            response = requests.get(
                f"https://api.github.com/repos/{repo}/actions/workflows/pipeline.yml/runs",
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                params={"event": "workflow_dispatch", "created": day.isoformat(), "per_page": 100, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            runs = response.json()["workflow_runs"]
            for r in runs:
                out.setdefault(str(r.get("display_title", "")), []).append(r)
            if len(runs) < 100:
                break
        else:
            raise ValueError(f"run history incomplete for {day}: over 1,000 runs")
        day += timedelta(days=1)
    return out


def catch_up(config: dict, end: datetime, start: datetime, repo: str, token: str) -> list[dict]:
    """Weekly/monthly entries whose latest slot before this window has no successful run (#839).

    Slots inside this run's window are left to the normal path. A slot is dispatched again when
    no run since it succeeded or is still queued/running, and fewer than CATCH_UP_ATTEMPTS runs
    started since it (so a run that keeps failing is retried once, not every 30 minutes)."""
    slots = []
    for e in entries(config):
        cron = str(e["cron"])
        if not periodic(cron):
            continue
        slot = last_slot(cron, end)
        if slot is None or slot > start:
            continue
        slots.append((e, slot))
    if not slots:
        return []
    runs = pipeline_runs(repo, token, min(slot for _, slot in slots), end)
    due_again = []
    for e, slot in slots:
        since = [r for r in runs.get(f"Pipeline · {e['source']} · {e['run']}", []) if _minute(r["created_at"]) >= slot]
        if any(r.get("conclusion") == "success" or r.get("status") != "completed" for r in since):
            continue
        if len(since) < CATCH_UP_ATTEMPTS:
            due_again.append({**e, "slot": slot.isoformat(timespec="minutes"), "runs_since": len(since)})
    return due_again


def _dispatch(repo: str, token: str, inputs: dict) -> str | None:
    """Start a Pipeline run; None on success, else the reason."""
    try:
        response = requests.post(
            f"https://api.github.com/repos/{repo}/actions/workflows/pipeline.yml/dispatches",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            json={"ref": "main", "inputs": inputs},
            timeout=30,
        )
    except requests.RequestException as error:
        return f"{type(error).__name__}: {error}"[:200]
    return None if response.status_code == 204 else response.text[:200]


def main(config: dict, plan, now: datetime | None = None) -> int:
    now = now or datetime.now(UTC)
    problems = validate(config, plan)
    started, skipped = [], []
    repo, token = os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GITHUB_TOKEN")
    end, minutes, gap, start = window(repo, token, now)
    chunks = _windows(end, start, minutes, gap)
    for e in entries(config):
        for chunk_end, chunk_minutes in chunks:
            if not due(str(e["cron"]), chunk_end, chunk_minutes):
                continue
            inputs = {"source": e["source"], "run": e["run"], "filter": str(e.get("filter") or "")}
            if not repo or not token:
                skipped.append({**inputs, "reason": "no GITHUB_REPOSITORY/GITHUB_TOKEN"})
                break
            reason = _dispatch(repo, token, inputs)
            (started if reason is None else skipped).append({**inputs, **({"reason": reason} if reason else {})})
            break
    caught_up = []
    if repo and token:
        try:
            window_start = start or end - timedelta(minutes=minutes)
            just_started = {(i["source"], i["run"]) for i in started}
            again = [
                e
                for e in catch_up(config, end, window_start, repo, token)
                if (e["source"], e["run"]) not in just_started
            ]
        except (requests.RequestException, ValueError, KeyError, TypeError) as error:
            again = []
            skipped.append({"catch_up": "not checked", "reason": f"{type(error).__name__}: {error}"[:200]})
        for e in again:
            inputs = {"source": e["source"], "run": e["run"], "filter": str(e.get("filter") or "")}
            reason = _dispatch(repo, token, inputs)
            item = {**inputs, "slot": e["slot"], "runs_since": e["runs_since"]}
            (caught_up if reason is None else skipped).append({**item, **({"reason": reason} if reason else {})})
    summary = {
        "at": now.isoformat(timespec="minutes"),
        "window": {"end": end.isoformat(timespec="minutes"), "chunks": len(chunks), "gap_minutes": gap},
        "started": started,
        "caught_up": caught_up,
        "skipped": skipped,
        "invalid_schedules": problems,
    }
    annotate("error" if skipped or problems else "notice", "scheduler", summary)
    step_summary(f"## scheduler\n\n```json\n{json.dumps(summary, indent=2, ensure_ascii=False)}\n```\n")
    return 1 if skipped or problems else 0
