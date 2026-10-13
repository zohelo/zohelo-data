#!/usr/bin/env python3
"""Run one Eurostat Bronze batch of one file and publish any traceback (dev, deleted before merge)."""
from __future__ import annotations

import contextlib
import io
import traceback

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate


def main() -> int:
    cat = catalog()
    annotate("notice", "tables before", sorted(".".join(t) for t in cat.list_tables("bronze") if "eurostat" in t[-1]))
    from ingestion.to_bronze.eurostat import batch
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            rc = batch.main(["--max-files", "1"])
        annotate("notice", "batch result", {"rc": rc, "stdout_tail": out.getvalue()[-2500:]})
    except BaseException:
        annotate("error", "batch traceback", {"traceback": traceback.format_exc()[-3000:],
                                              "stdout_tail": out.getvalue()[-800:]})
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
