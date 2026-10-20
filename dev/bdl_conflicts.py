"""Read-only: the conflicting BDL keys of P1314 and P1410 with both published values, the part each
comes from and that part's territory selection (#875 owner decision)."""

from __future__ import annotations

from collections import defaultdict
import csv
import io
import json
import zipfile

from ingestion.common.r2 import archive_key, get_with_backoff, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gus_bdl.entities import candidates


def rows_of(client, bucket, part):
    key = part["object"]["key"]
    try:
        body = get_with_backoff(client, bucket, key)
    except Exception:
        body = get_with_backoff(client, bucket, archive_key("gus_bdl", part["sha256"], key))
    z = zipfile.ZipFile(io.BytesIO(body))
    member = next(m for m in z.namelist() if m.lower().endswith(".csv"))
    rows = list(csv.reader(io.TextIOWrapper(z.open(member), encoding="utf-8-sig"), delimiter=";"))
    header = rows[0][:-1] if rows[0][-1] == "" else rows[0]
    n = len(header)
    return [(r[0], r[1], tuple(r[2 : n - 4]), r[n - 4], r[n - 3].strip(), r[n - 2]) for r in rows[1:]]


def main() -> int:
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    parts = [p for p in candidates(client, bucket) if p["subgroup_id"] in ("P1314", "P1410")]
    for sg in ("P1314", "P1410"):
        seen = defaultdict(list)
        for p in [x for x in parts if x["subgroup_id"] == sg]:
            idx = (p.get("part") or {}).get("index")
            terr = (p.get("part") or {}).get("selection", {}).get("territories", [])
            for code, name, dims, year, value, unit in rows_of(client, bucket, p):
                if value:
                    seen[(code, dims, year)].append(
                        {
                            "part": idx,
                            "value": value,
                            "name": name,
                            "territories": len(terr),
                            "first_territories": terr[:3],
                            "ingested": p.get("ingested_at"),
                        }
                    )
        conflicts = {k: v for k, v in seen.items() if len({x["value"] for x in v}) > 1}
        codes = defaultdict(int)
        for (code, _, _), _v in conflicts.items():
            codes[code] += 1
        annotate(
            "notice",
            f"bdl conflicts {sg}",
            {
                "keys": len(conflicts),
                "by_unit": dict(codes),
                "sample": [
                    {"key": list(k[:1]) + [list(k[1]), k[2]], "values": v} for k, v in list(conflicts.items())[:6]
                ],
            },
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
