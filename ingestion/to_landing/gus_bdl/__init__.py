"""GUS BDL → Landing (docs/landing.md). Entity = subgroup (P####), version = its lastUpdate.

landing.inspect   inspect.py: subgroups' lastUpdate (BDL API) vs Landing → current / stale / missing
landing.extract   metadata.py: variables of every subgroup with data, as API pages (resumable);
                  the filter selects subgroups (globs); "units" in the filter also lands the unit list.
                  "web" in the filter runs web_export.py instead: subgroups missing from Landing,
                  exported from the website (optionally only the P#### listed; "split" also
                  exports subgroups that need territory parts); "web packages" runs
                  web_packages.py: collect ready whole-subgroup packages, request missing ones;
                  "api P#### …" runs refresh.py --initial for the listed subgroups (#889), then
                  metadata.py for them
"""

from __future__ import annotations

import time


def _inspect(p) -> int:
    from ingestion.to_landing.gus_bdl import inspect

    tokens = [x for x in (p.filter or "").replace(",", " ").split() if x not in ("web", "split", "packages", "api")]
    return inspect.main([x for x in tokens if x])


def _extract(p) -> int:
    from ingestion.to_landing.gus_bdl import metadata

    tokens = [x for x in (p.filter or "").replace(",", " ").split() if x]
    if "api" in tokens:  # owner decision #889: the listed subgroups through the API, then their variables
        from ingestion.to_landing.gus_bdl import metadata, refresh

        only = [t for t in tokens if t.startswith("P")]
        if not only:
            raise SystemExit("'api' needs the subgroups to load (P####)")
        budget = min(p.budget_minutes, 330)
        started = time.monotonic()
        status = refresh.main(["--initial", "--only", *only, "--budget-minutes", str(budget * 0.85)])
        left = max(budget - (time.monotonic() - started) / 60, 10)
        meta = metadata.main(["--filter", ",".join(only), "--budget-minutes", str(left)])
        return status or meta
    if "web" in tokens and "packages" in tokens:
        from ingestion.to_landing.gus_bdl import web_packages

        if p.parallel_jobs > 1:
            raise SystemExit("the BDL website export runs as one job (parallel_jobs 1)")
        return web_packages.main(budget_minutes=min(p.budget_minutes, 330))
    if "web" in tokens:
        from ingestion.to_landing.gus_bdl import web_export

        if p.parallel_jobs > 1:  # one account, one browser: packages of parallel sessions would be ambiguous
            raise SystemExit("the BDL website export runs as one job (parallel_jobs 1)")
        only = [t for t in tokens if t.startswith("P")] or None
        return web_export.main(
            limit=10_000, budget_minutes=min(p.budget_minutes, 330), only=only, split="split" in tokens
        )
    args = [
        "--filter",
        ",".join(t for t in tokens if t != "units"),
        "--budget-minutes",
        str(min(p.budget_minutes, 330)),
    ]
    return metadata.main(args + (["--units"] if "units" in tokens else []))


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract}
