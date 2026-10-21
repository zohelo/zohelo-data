"""Read-only: how large the BDL subgroups to export are (missing from Landing, or changed since),
as the website counts them ("informacji" = variables × territorial units × years)."""

from __future__ import annotations

from collections import Counter

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.gus_bdl.api import Bdl, token
from ingestion.to_landing.gus_bdl.inspect import landing_versions

UNITS = {0: 1, 1: 7, 2: 17, 3: 73, 4: 380, 5: 2477, 6: 4100}  # BDL units per level (approx.)
MAX_INFORMATION = 3500


def main() -> int:
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    held = landing_versions(client, bucket)
    api = Bdl()
    sizes, kinds = [], Counter()
    for subject in api.subgroups():
        sid = subject["id"]
        detail = api.get(f"/subjects/{sid}", lang="pl")
        version = token(api.last_update(sid, detail))
        kind = "missing" if sid not in held else ("changed" if version > held[sid] else None)
        if not kind:
            continue
        variables = api.get("/variables", **{"subject-id": sid, "page-size": 1}).get("totalRecords") or 0
        levels = detail.get("levels") or subject.get("levels") or []
        years = len(detail.get("years") or []) or 1
        info = variables * sum(UNITS.get(int(level), 0) for level in levels) * years
        sizes.append((info, sid, kind))
        kinds[kind] += 1
    sizes.sort()
    buckets = Counter(
        "≤3,500" if s <= MAX_INFORMATION else "≤50,000" if s <= 50_000 else "≤500,000" if s <= 500_000 else ">500,000"
        for s, _, _ in sizes
    )
    annotate(
        "notice",
        "bdl export sizes",
        {
            "subgroups": len(sizes),
            "by_kind": dict(kinds),
            "by_size": dict(buckets),
            "requests": api.requests,
            "largest": sizes[-10:],
            "smallest": sizes[:5],
            "total_information": sum(s for s, _, _ in sizes),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
