#!/usr/bin/env python3
"""Runner disk and memory while a pipeline activity runs, published when the activity fails.

A run that dies from outside (exit code 143 or 137) writes no annotation of its own. The
activity action starts `sample` in the background before the activity, and runs `report`
only when the activity failed: the report shows free disk and memory over the last minutes
and the kernel's out-of-memory lines, so the cause is visible without the job log.
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
REPORT_SAMPLES = 20  # last ten minutes


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
    """Append one snapshot per interval until the job ends."""
    while True:
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(snapshot()) + "\n")
        time.sleep(INTERVAL_SECONDS)


def kernel_oom_lines() -> list[str]:
    """Out-of-memory killer lines from the kernel log (the runner user may read it with sudo)."""
    completed = subprocess.run(["sudo", "-n", "dmesg"], capture_output=True, text=True, timeout=30)
    if completed.returncode != 0:
        return [f"dmesg not readable: {completed.stderr.strip()[:200]}"]
    keys = ("out of memory", "oom-kill", "killed process")
    return [line for line in completed.stdout.splitlines() if any(k in line.lower() for k in keys)][-10:]


def report(log: Path) -> None:
    """Annotate the lowest free disk and memory seen, the last samples and kernel OOM lines."""
    samples = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
    samples.append(snapshot())
    lowest = {key: min(s[key] for s in samples if key in s) for key in samples[-1] if key != "time"}
    annotate(
        "warning",
        "runner resources at failure",
        {
            "lowest": lowest,
            "samples": len(samples),
            "last": samples[-REPORT_SAMPLES:],
            "kernel_oom": kernel_oom_lines(),
        },
    )


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2 or args[0] not in ("sample", "report"):
        print("usage: python -m ops.runner_resources sample|report <log file>", file=sys.stderr)
        return 2
    (sample if args[0] == "sample" else report)(Path(args[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
