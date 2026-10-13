"""Bronze load orchestration shared by sources: run a source's batch module repeatedly.

Each batch runs in its own process, so DuckDB memory is released between batches. The
Bronze table's file list is the checkpoint, so a run that stops on its time budget
resumes on the next run. A batch prints one JSON line with "operation": <operation> and
"result": "pass"; a failed batch publishes its last output lines as an annotation.
Exit codes: 0 complete, 2 time budget / batch limit (rerun continues), other = failure.
"""
from __future__ import annotations

from collections import deque
import json
import subprocess
import sys
import time

from ingestion.common.report import annotate, step_summary


def run_batches(source: str, batch_module: str, operation: str, budget_minutes: float,
                batch_size: int = 256, batch_seconds: int = 2700, margin_seconds: int = 3600,
                max_batches: int = 128) -> int:
    deadline = time.monotonic() + budget_minutes * 60
    totals = {"batches": 0, "appended_files": 0, "rows_added": 0, "bytes_added": 0,
              "rejected": 0, "rejected_datasets": [], "missing_remaining": None}

    def report(result: str, level: str = "notice") -> None:
        annotate(level, f"{source} Bronze from Landing", {"result": result, **totals})
        step_summary(f"## {source} · bronze load: {result}\n\n"
                     f"Files {totals['appended_files']:,} · rows {totals['rows_added']:,} · "
                     f"rejected {totals['rejected']:,} · remaining {totals['missing_remaining']}\n")

    for batch in range(1, max_batches + 1):
        remaining = int(deadline - time.monotonic())
        if remaining < margin_seconds:
            report("incomplete_time_budget", "warning")
            return 2
        print(json.dumps({"operation": f"{source}_bronze_batch", "batch": batch}), flush=True)
        command = [sys.executable, "-u", "-m", batch_module,
                   "--max-files", str(batch_size),
                   "--session-seconds", str(min(batch_seconds, remaining - 120))]
        receipts, tail = [], deque(maxlen=40)
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
            for line in process.stdout:
                sys.stdout.write(line)
                tail.append(line.rstrip()[:300])
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if (isinstance(item, dict) and item.get("operation") == operation
                        and item.get("result") == "pass"):
                    receipts.append(item)
            returncode = process.wait()
        if returncode:
            annotate("error", f"{source} Bronze batch output", {"last_lines": list(tail)})
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
