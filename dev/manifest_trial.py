"""Dev trial for #2034: dbt parse, then annotate {kpi_key: dimensions} of warehouse.catalog.manifest()."""

from __future__ import annotations

import subprocess

from ingestion.common.report import annotate
from warehouse.catalog import manifest


def main() -> int:
    cmd = ["dbt", "parse", "--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse"]
    code = subprocess.run(cmd, check=False).returncode
    if code != 0:
        annotate("error", "manifest", {"dbt_parse_exit": code})
        return 1
    annotate("notice", "manifest", {m["kpi_key"]: m["dimensions"] for m in manifest()["metrics"]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
