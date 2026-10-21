"""GUS BDL → Landing (docs/landing.md). Entity = subgroup (P####), version = its lastUpdate.

landing.inspect   inspect.py: subgroups' lastUpdate (BDL API) vs Landing → current / stale / missing
landing.extract   metadata.py: variables of every subgroup with data, as API pages (resumable);
                  the filter selects subgroups (globs); "units" in the filter also lands the unit list.
                  "web" in the filter runs web_export.py instead: subgroups missing from Landing,
                  exported from the website (optionally only the P#### listed; "split" also
                  exports subgroups that need territory parts)
"""

from __future__ import annotations


def _inspect(p) -> int:
    from ingestion.to_landing.gus_bdl import inspect

    return inspect.main([x for x in (p.filter or "").replace(",", " ").split() if x])


def _extract(p) -> int:
    from ingestion.to_landing.gus_bdl import metadata

    tokens = [x for x in (p.filter or "").replace(",", " ").split() if x]
    if "web" in tokens:
        from ingestion.to_landing.gus_bdl import web_export

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
