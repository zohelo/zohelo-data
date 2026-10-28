"""Dev probe for #2036: size of latest.json per published KPI and values per source × level; writes nothing."""

from __future__ import annotations

from collections import Counter
import json
import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import publish


def main(argv: list[str]) -> int:
    cat = catalog()
    checked = publish.checked(publish.gold(cat, "gold", "coverage"))
    measures = publish.seed("kpi_measure")
    out = {}
    for kpi in publish.seed("kpi"):
        if kpi["status"] != "published" or kpi["kpi_key"] in publish.SKIP:
            continue
        r = publish.kpi_latest(
            publish.gold(cat, "gold", f"metrics_{kpi['kpi_key']}"),
            measures,
            checked,
            kpi["kpi_key"],
            kpi["unit_key"],
            "2026-01-01T00:00:00Z",
        )
        size = len(json.dumps(r, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        per = Counter(f"{v['source_key']}/{v['geography_level']}" for v in r["values"])
        out[kpi["kpi_key"]] = {"bytes": size, "values": dict(per)}
    annotate("notice", "latest.json sizes", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
