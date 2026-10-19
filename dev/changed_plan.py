"""Trial of warehouse/changed.py (#822): the plan against prod (read-only), fingerprint
stability across two parses, and the build-property write on a zz_dev_ scratch table."""

from __future__ import annotations

import contextlib

import pyarrow as pa

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import changed


def main() -> None:
    cat = catalog()
    meta = changed.manifest()
    plan = changed.plan(cat, meta)
    first = {m["name"]: m["fingerprint"] for m in changed.models(meta).values()}
    second = {m["name"]: m["fingerprint"] for m in changed.models(changed.manifest()).values()}
    kinds = {}
    for reason in plan["reasons"].values():
        kinds[reason.split(":")[0]] = kinds.get(reason.split(":")[0], 0) + 1
    annotate(
        "notice",
        "changed plan",
        {
            "rebuild": len(plan["select"]),
            "unchanged": plan["unchanged"],
            "why": kinds,
            "fingerprints_stable": first == second,
            "models": len(first),
        },
    )

    # As if every model had been built by this rule at its current snapshot: which would a
    # build now select because an input changed since?
    real = changed.table_state

    def as_built(cat_, namespace, table):
        state = real(cat_, namespace, table)
        if state and namespace in ("silver", "gold") and state["built_from_ms"] is None:
            names = {m["table"]: m["fingerprint"] for m in changed.models(meta).values()}
            state = {**state, "built_from_ms": state["snapshot_ms"], "fingerprint": names.get(table)}
        return state

    changed.table_state = as_built
    try:
        simulated = changed.plan(cat, meta)
    finally:
        changed.table_state = real
    annotate("notice", "changed plan, simulated second run", {"rebuild": simulated["reasons"]})

    # Property round-trip on a scratch table (the shape record() writes).
    ns = ("zz_dev_changed",)
    cat.create_namespace_if_not_exists(ns)
    ident = ("zz_dev_changed", "probe")
    with contextlib.suppress(Exception):
        cat.drop_table(ident)
    table = cat.create_table(ident, schema=pa.schema([("x", pa.int64())]))
    table.append(pa.table({"x": [1]}))
    model = {"name": "probe", "namespace": "zz_dev_changed", "table": "probe", "parents": [], "fingerprint": "f" * 32}
    original = changed.models
    changed.models = lambda _meta: {"model.zohelo.probe": model}
    try:
        result = changed.record(cat, {}, ["probe"], 1234)
        state = changed.table_state(cat, "zz_dev_changed", "probe")
    finally:
        changed.models = original
        cat.drop_table(ident)
        cat.drop_namespace(ns)
    annotate("notice", "record round-trip", {"record": result, "state": state})


if __name__ == "__main__":
    main()
