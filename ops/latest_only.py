#!/usr/bin/env python3
"""Latest version only in Bronze: report or remove superseded versions (policy #822/#845).

Without --apply: per source and Bronze table, the Landing files held, how many are superseded
(not the latest version of their entity), their rows in files named after them, and files the
receipts do not know (kept). With --apply: removes the superseded rows (``ingestion.common.latest``).
The Bronze load of every source runs the same prune after loading, so new versions replace old ones.

Args: --source <id> ... (default: every source that loads Bronze), --apply.
"""

from __future__ import annotations

import argparse

from ingestion.common.bronze_files import SOURCE_TABLES
from ingestion.common.latest import prune
from ingestion.common.report import annotate, step_summary


def main(argv=None) -> int:
    """Report (and with --apply remove) superseded versions per source."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", nargs="*", default=None)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    selected = args.source or sorted(SOURCE_TABLES)
    lines = [
        "## Latest version only (Bronze)",
        "",
        "| table | files | superseded | rows (named files) | unknown kept |",
    ]
    lines.append("|---|---:|---:|---:|---:|")
    for source in selected:
        report = prune(source, args.apply)
        annotate("notice", f"latest only · {source}{' · applied' if args.apply else ''}", report)
        for name, e in report.items():
            lines.append(
                f"| `{name}` | {e['files']:,} | {e['superseded']:,} | "
                f"{e['superseded_rows_in_named_files']:,} | {e['unknown_kept']:,} |"
            )
    step_summary("\n".join(lines) + "\n")
    return 0


def run(argv=None) -> int:
    """main() with failures published as an error annotation (job logs are not readable by AI sessions)."""
    try:
        return main(argv)
    except Exception as exc:
        annotate("error", "latest only failed", {"error": type(exc).__name__, "detail": str(exc)[:3000]})
        raise


if __name__ == "__main__":
    raise SystemExit(run())
