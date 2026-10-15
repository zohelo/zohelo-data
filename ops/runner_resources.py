#!/usr/bin/env python3
"""Runner disk and memory while a pipeline activity runs, published when the activity fails.

A run that dies from outside (exit code 143 or 137) writes no annotation of its own. The
activity action starts `sample` in the background before the activity, and runs `report`
only when the activity failed or was cancelled: the report shows free disk and memory over
the last minutes and the kernel's out-of-memory lines, so the cause is visible without the
job log. A runner that is shut down as a whole skips that report step too, so `sample` also
publishes live annotations while the activity runs: a notice every HEARTBEAT_MINUTES (at most
MAX_HEARTBEATS, so the activity keeps notices for its own results) and a warning the first
time free memory or disk falls below each threshold. GitHub keeps annotations already written
when the job dies (at most 10 notices and 10 warnings per step).
"""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

from ingestion.common.report import annotate

INTERVAL_SECONDS = 30
MOUNTS = ("/", "/mnt")
REPORT_SAMPLES = 16  # last eight minutes; keeps the annotation under its 3,900 characters
OOM_LINES = 5
HEARTBEAT_MINUTES = 25
MAX_HEARTBEATS = 4  # first 100 min; leaves 6 of the step's 10 notices to the activity itself
MEMORY_THRESHOLDS_GB = (3.0, 1.5, 0.5)
DISK_THRESHOLDS_GB = (5.0, 1.0)  # per mount: 3 + 2 × 2 = 7 warnings at most


def _gb(value: int) -> float:
    return round(value / 1e9, 1)


def snapshot() -> dict:
    """Free disk per mount and available memory and swap, in GB."""
    meminfo = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, value = line.split(":", 1)
        meminfo[name] = int(value.split()[0]) * 1024
    sample = {"time": time.strftime("%H:%M:%S", time.gmtime())}
    for mount in MOUNTS:
        if Path(mount).is_dir():
            sample[f"disk_free{mount}"] = _gb(shutil.disk_usage(mount).free)
    sample["mem_available"] = _gb(meminfo.get("MemAvailable", 0))
    sample["swap_free"] = _gb(meminfo.get("SwapFree", 0))
    return sample


def crossed(current: dict, reported: set[tuple[str, float]]) -> list[tuple[str, float]]:
    """Thresholds newly passed by this snapshot: (key, threshold) pairs not reported before,
    lowest threshold last; the caller adds them to `reported` so each warns once."""
    limits = {"mem_available": MEMORY_THRESHOLDS_GB}
    limits.update({f"disk_free{mount}": DISK_THRESHOLDS_GB for mount in MOUNTS})
    new = []
    for key, thresholds in limits.items():
        if key not in current:
            continue
        new.extend((key, t) for t in thresholds if current[key] < t and (key, t) not in reported)
    return new


def sample(log: Path) -> None:
    """Append one snapshot per interval until the job ends and publish live annotations (see
    the module docstring); a failed write (full disk) is reported on stderr, which the action
    keeps next to the log, and sampling goes on."""
    reported: set[tuple[str, float]] = set()
    started = last_heartbeat = time.monotonic()
    heartbeats = 0
    while True:
        current = snapshot()
        try:
            with log.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(current) + "\n")
        except OSError as error:
            print(f"{time.strftime('%H:%M:%S', time.gmtime())} {error}", file=sys.stderr, flush=True)
        minutes = round((time.monotonic() - started) / 60)
        for key, threshold in crossed(current, reported):
            reported.add((key, threshold))
            annotate(
                "warning", "runner resources low (live)", {"below_gb": {key: threshold}, "minute": minutes, **current}
            )
        if heartbeats < MAX_HEARTBEATS and time.monotonic() - last_heartbeat >= HEARTBEAT_MINUTES * 60:
            last_heartbeat, heartbeats = time.monotonic(), heartbeats + 1
            annotate("notice", "runner resources (live)", {"minute": minutes, **current})
        time.sleep(INTERVAL_SECONDS)


def kernel_oom_lines() -> list[str]:
    """Out-of-memory killer lines from the kernel log (the runner user may read it with sudo)."""
    try:
        completed = subprocess.run(["sudo", "-n", "dmesg"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as error:
        return [f"dmesg not readable: {error}"[:200]]
    if completed.returncode != 0:
        return [f"dmesg not readable: {completed.stderr.strip()[:200]}"]
    keys = ("out of memory", "oom-kill", "killed process")
    lines = [line for line in completed.stdout.splitlines() if any(k in line.lower() for k in keys)]
    return [line[:200] for line in lines[-OOM_LINES:]]


def report(log: Path) -> None:
    """Annotate the lowest free disk and memory seen, the last samples, lines the sampler could
    not write or that were cut (full disk), the sampler's errors and kernel OOM lines."""
    samples, unreadable = [], 0
    lines = log.read_text(errors="replace").splitlines() if log.exists() else []
    for line in lines:
        try:
            samples.append(json.loads(line))
        except json.JSONDecodeError:
            unreadable += 1
    samples.append(snapshot())
    lowest = {key: min(s[key] for s in samples if key in s) for key in samples[-1] if key != "time"}
    errors = log.with_suffix(".err")
    sampler_errors = errors.read_text(errors="replace").splitlines()[-3:] if errors.exists() else []
    annotate(
        "warning",
        "runner resources at failure",
        {
            "kernel_oom": kernel_oom_lines(),
            "lowest": lowest,
            "log": "found" if log.exists() else "missing: the sampler never wrote (failed at start?)",
            "samples": len(samples),
            "unreadable_lines": unreadable,
            "sampler_errors": [e[:200] for e in sampler_errors],
            "last": samples[-REPORT_SAMPLES:],
        },
    )


def main(argv: list[str] | None = None) -> int:
    """Run `sample <log>` or `report <log>`."""
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2 or args[0] not in ("sample", "report"):
        print("usage: python -m ops.runner_resources sample|report <log file>", file=sys.stderr)
        return 2
    (sample if args[0] == "sample" else report)(Path(args[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
