"""Eurostat bronze.load: incremental Landing → bronze.eurostat_full_observations.

Runs bronze_batch.py repeatedly, each batch in its own process so DuckDB memory is
released between batches. Membership of raw_sha256 in the Iceberg table is the
checkpoint, so a run that stops on its time budget resumes on the next run.
Decoder rejections are recorded in bronze.eurostat_rejected_files and skipped, never
retried silently (ops/retry_rejected clears them).
"""
from __future__ import annotations

from collections import deque
import json
import subprocess
import sys
import time

from ingestion.common.report import annotate, step_summary

BATCH_MODULE = "ingestion.to_bronze.eurostat.batch"
BATCH_SIZE = 256
BATCH_SECONDS = 2700         # fewer batches = fewer full receipt listings
MAX_BATCHES = 128
MARGIN_SECONDS = 3600            # one file can take ~1 h (AVIA_TF_ACA: 1.48B cells)


def load(budget_minutes: float) -> int:
    deadline = time.monotonic() + budget_minutes * 60
    totals = {"batches": 0, "appended_files": 0, "rows_added": 0, "bytes_added": 0,
              "rejected": 0, "rejected_datasets": [], "missing_remaining": None}

    def report(result: str, level: str = "notice") -> None:
        annotate(level, "eurostat Bronze from Landing", {"result": result, **totals})
        step_summary(f"## eurostat · bronze load: {result}\n\n"
                     f"Files {totals['appended_files']:,} · rows {totals['rows_added']:,} · "
                     f"rejected {totals['rejected']:,} · remaining {totals['missing_remaining']}\n")

    for batch in range(1, MAX_BATCHES + 1):
        remaining = int(deadline - time.monotonic())
        if remaining < MARGIN_SECONDS:
            report("incomplete_time_budget", "warning")
            return 2
        print(json.dumps({"operation": "eurostat_bronze_batch", "batch": batch}), flush=True)
        command = [sys.executable, "-u", "-m", BATCH_MODULE,
                   "--max-files", str(BATCH_SIZE),
                   "--session-seconds", str(min(BATCH_SECONDS, remaining - 120))]
        receipts, tail = [], deque(maxlen=40)
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
            for line in process.stdout:
                sys.stdout.write(line)
                tail.append(line.rstrip()[:300])
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if (isinstance(item, dict) and item.get("operation") == "eurostat-bronze-batch"
                        and item.get("result") == "pass"):
                    receipts.append(item)
            returncode = process.wait()
        if returncode:
            annotate("error", "eurostat Bronze batch output", {"last_lines": list(tail)})
            report(f"batch_failed_exit_{returncode}", "error")
            return returncode
        if len(receipts) != 1:
            report("batch_without_receipt", "error")
            return 1
        receipt = receipts[0]
        totals["batches"] += 1
        for key in ("appended_files", "rows_added", "bytes_added"):
            totals[key] += int(receipt.get(key) or 0)
        totals["rejected"] += int(receipt.get("failed") or 0)
        totals["rejected_datasets"] = (totals["rejected_datasets"]
                                       + list(receipt.get("failed_datasets") or []))[:50]
        totals["missing_remaining"] = receipt.get("missing_distinct_raw_sha256")
        if receipt.get("missing_distinct_raw_sha256") == 0:
            report("complete")
            return 0
        if receipt.get("processed", 0) == 0:
            report("no_progress", "error")
            return 1
    report("batch_limit_reached", "warning")
    return 2
