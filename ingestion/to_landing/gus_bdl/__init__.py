"""GUS BDL → Landing (docs/landing.md). Entity = subgroup (P####), version = its lastUpdate.

landing.inspect   inspect.py: subgroups' lastUpdate (BDL API) vs Landing → current / stale / missing
"""
from __future__ import annotations


def _inspect(p) -> int:
    from ingestion.to_landing.gus_bdl import inspect
    return inspect.main([x for x in (p.filter or "").replace(",", " ").split() if x])


ACTIVITIES = {"landing.inspect": _inspect}
