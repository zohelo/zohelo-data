"""Scout for the Gold PoC: which BDL subgroups and DBW files carry the v1 concepts, and how
fast pyiceberg reads one block of each big Silver table (partition pruning). Read-only."""

from __future__ import annotations

import json
import re
import time

from pyiceberg.expressions import In

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

BDL_PATTERNS = [r"^ludność", r"^stan ludności"]  # within LUDNOŚĆ / STAN LUDNOŚCI (the rest found in run 1)
BDL_KNOWN = ["P2392", "P2497", "P3824", "P1410", "P1873", "P2425"]
DBW_PATTERNS = [
    r"^ludność",
    r"^stopa bezrobocia",
    r"^wskaźnik(i)? cen towarów i usług konsumpcyjnych",
    r"^produkt krajowy brutto",
    r"^przeciętne miesięczne wynagrodzenie",
    r"^mieszkania oddane",
]
EUROSTAT = [
    "demo_pjan",
    "nama_10_gdp",
    "une_rt_a",
    "une_rt_m",
    "prc_hicp_manr",
    "demo_gind",
    "nama_10_pc",
    "lfsi_emp_a",
]


def main() -> int:
    client, bucket = s3(), req("R2_LAKEHOUSE_BUCKET")
    tree = json.loads(client.get_object(Bucket=bucket, Key="_control/gus_bdl/subjects.json")["Body"].read())
    by_id = {s["id"]: s for s in tree["subjects"]}

    def path(s):
        out, cur = [], s
        while cur:
            out.append(cur["name"])
            cur = by_id.get(cur.get("parentId"))
        return " / ".join(reversed(out))

    bdl = []
    for s in tree["subjects"]:
        if (
            s.get("hasVariables")
            and any(re.search(p, s["name"].lower()) for p in BDL_PATTERNS)
            and "STAN LUDNO" in path(s)
        ):
            bdl.append(
                {
                    "id": s["id"],
                    "name": s["name"],
                    "path": path(s)[:120],
                    "years": (s.get("years") or [])[-1:],
                    "levels": s.get("levels"),
                }
            )
    for i in range(0, min(len(bdl), 24), 12):
        annotate("notice", f"bdl candidates {i // 12 + 1}", bdl[i : i + 12])

    inv = json.loads(client.get_object(Bucket=bucket, Key="_control/gus_dbw/inventory.json")["Body"].read())
    dbw = []
    for entry in inv.get("indicators", {}).values():
        for f in entry.get("files") or []:
            if any(re.search(p, (f.get("indicator_name") or "").lower()) for p in DBW_PATTERNS):
                dbw.append(
                    {
                        "entity": f["entity"],
                        "ind": f.get("indicator_name"),
                        "sec": f.get("section_name"),
                        "per": f.get("periodicity"),
                    }
                )
    dbw.sort(key=lambda d: (d["ind"], d["sec"] or ""))
    for i in range(0, len(dbw), 12):
        annotate("notice", f"dbw candidates {i // 12 + 1}", dbw[i : i + 12])

    cat = bf.retry(catalog, "catalog")
    timing = {}
    t = time.monotonic()
    table = cat.load_table(("silver", "eurostat_observations"))
    scan = table.scan(
        row_filter=In("dataset_id", EUROSTAT), selected_fields=("dataset_id", "freq_code", "unit_code", "geo_code")
    )
    files = list(scan.plan_files())
    rows = scan.to_arrow()
    timing["eurostat"] = {
        "files": len(files),
        "rows": rows.num_rows,
        "seconds": round(time.monotonic() - t, 1),
        "per_dataset": rows.group_by("dataset_id").aggregate([("freq_code", "count")]).to_pylist(),
    }
    annotate("notice", "pyiceberg block read", timing)
    sg = BDL_KNOWN
    t = time.monotonic()
    table = cat.load_table(("silver", "bdl_observations"))
    scan = table.scan(row_filter=In("subgroup_id", sg), selected_fields=("subgroup_id", "unit_code_scheme"))
    files = list(scan.plan_files())
    rows = scan.to_arrow()
    annotate(
        "notice",
        "pyiceberg bdl read",
        {
            "subgroups": sg,
            "files": len(files),
            "rows": rows.num_rows,
            "seconds": round(time.monotonic() - t, 1),
            "per_subgroup": rows.group_by("subgroup_id").aggregate([("unit_code_scheme", "count")]).to_pylist(),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
