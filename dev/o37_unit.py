"""Pure checks of the O37 removal and licence rules (dev only; the repository has no committed tests)."""

import pyarrow as pa

from ops.repo_check import _schema_problems
from warehouse.publish import licence_blocked, licence_gated
from warehouse.publish_v2 import stale_plan


def run() -> list[str]:
    results = []
    files = {"kpi/a/series.json": {}, "catalog.json": {}}
    existing = {"kpi/a/series.json", "kpi/a/old.json", "kpi/b/series.json", "kpi/eurpln/series.json"}
    stale, lost = stale_plan(existing, {"a", "b"}, files)
    assert lost == ["b"], lost  # candidate b published but not built: fail, keep it
    assert stale == ["kpi/a/old.json", "kpi/eurpln/series.json"], stale  # out of scope / leftover: remove
    results.append("stale_plan: lost candidate kept and fails; out-of-scope removed")
    stale, lost = stale_plan({"kpi/eurpln/series.json"}, {"a"}, files)
    assert (stale, lost) == (["kpi/eurpln/series.json"], []), (stale, lost)
    results.append("stale_plan: licence-gated KPI removed")
    measures = [
        {"kpi_key": "fx_rate", "source_system": "nbp"},
        {"kpi_key": "x", "source_system": "nbp"},
        {"kpi_key": "x", "source_system": "eurostat"},
    ]
    assert licence_blocked("fx_rate", measures, {"eurostat"})
    assert not licence_blocked("x", measures, {"eurostat"})
    assert not licence_blocked("none", measures, {"eurostat"})
    results.append("licence_blocked: seed-decided")
    t = pa.table({"source_system": ["nbp", "eurostat"], "value": [1.0, 2.0]})
    assert licence_gated(t, {"eurostat"}).column("source_system").to_pylist() == ["eurostat"]
    try:
        licence_gated(pa.table({"source_system": pa.array([None, "eurostat"], pa.string())}), {"eurostat"}, "k")
        raise AssertionError("null source_system accepted")
    except ValueError as e:
        assert "without source_system" in str(e)
    results.append("licence_gated: unlicensed rows out; null source_system fails")
    assert _schema_problems([], {"type": "array", "minItems": 1}, "$")
    assert not _schema_problems([1], {"type": "array", "minItems": 1}, "$")
    results.append("minItems enforced")
    return results


if __name__ == "__main__":
    print("\n".join(run()))
