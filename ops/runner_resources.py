#!/usr/bin/env python3
"""Runner disk and memory while a pipeline activity runs, published when the activity fails.

A run that dies from outside (exit code 143 or 137) writes no annotation of its own. The
activity action starts `sample` in the background before the activity, and runs `report`
only when the activity failed or was cancelled: the report shows free disk and memory over
the last minutes and the kernel's out-of-memory lines, so the cause is visible without the
job log.
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


def sample(log: Path) -> None:
    """Append one snapshot per interval until the job ends; a failed write (full disk) is
    reported on stderr, which the action keeps next to the log, and sampling goes on."""
    while True:
        try:
            with log.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(snapshot()) + "\n")
        except OSError as error:
            print(f"{time.strftime('%H:%M:%S', time.gmtime())} {error}", file=sys.stderr, flush=True)
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
