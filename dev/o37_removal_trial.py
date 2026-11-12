"""Dev trial for the O37 review (#2824 B1/S1): removals under the scratch prefix zz_dev_o37r/ seeded with a
licence-gated KPI and a KPI whose Gold table read fails. Only the licence-gated files may be removed; a failed read
of a live v2 KPI must fail the run before any write. Read-only against production; scratch removed at the end."""

from __future__ import annotations

import json
import sys
import traceback

from pyiceberg.exceptions import NoSuchTableError

from dev import o37_unit
from ingestion.common import report
from ingestion.common.r2 import catalog, req, s3

SCRATCH = "zz_dev_o37r/"
V1, V2 = SCRATCH + "v1/", SCRATCH + "v2/"
real_annotate = report.annotate
report.annotate = lambda level, title, data: print(f"[{level}] {title}: {json.dumps(data, default=str)[:1500]}")


def listing(client, prefix: str) -> dict[str, bytes]:
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    found = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            found[key[len(prefix) :]] = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return found


def clean(client) -> int:
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    keys = list(listing(client, SCRATCH))
    for i in range(0, len(keys), 500):
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": SCRATCH + k} for k in keys[i : i + 500]]})
    return len(keys)


def failing_gold(real, failing: set[str]):
    def gold(cat, namespace, name):
        if name in failing:
            raise NoSuchTableError(f"simulated: {namespace}.{name}")
        return real(cat, namespace, name)

    return gold


def main() -> int:
    from warehouse import publish, publish_v2

    cat, client = catalog(), s3()
    sentinel = b'{"sentinel":true}'
    out: dict = {}
    ok = True
    try:
        clean(client)
        out["unit"] = o37_unit.run()
        real_gold = publish.gold
        # v1: fx_rate (licence-blocked) and population (table read fails) seeded
        for key in ("kpi/fx_rate/series.parquet", "kpi/fx_rate/latest.json", "kpi/population/series.parquet"):
            publish.put(client, V1 + key, sentinel)
        publish.gold = failing_gold(real_gold, {"metrics_population"})
        try:
            r1 = publish.main(prefix=V1)
        finally:
            publish.gold = real_gold
        v1 = listing(client, V1)
        out["v1"] = {
            "licence_gate": r1["licence_gate"],
            "missing": r1["missing"],
            "fx_rate_files_left": sorted(k for k in v1 if k.startswith("kpi/fx_rate/")),
            "population_kept_unchanged": v1.get("kpi/population/series.parquet") == sentinel,
            "catalog_has_fx_rate": any(
                k["kpi_key"] == "fx_rate" for k in json.loads(v1["catalog/catalog.json"])["kpis"]
            ),
        }
        ok &= not out["v1"]["fx_rate_files_left"] and out["v1"]["population_kept_unchanged"]
        ok &= not out["v1"]["catalog_has_fx_rate"]
        v1_catalog, status = json.loads(v1["catalog/catalog.json"]), json.loads(v1["status.json"])
        stamp = json.loads(v1["catalog/catalog.json"])["generated_at"]
        checked_at = publish.checked(real_gold(cat, "gold", "coverage"))
        # v2 run A: eurpln (licence-gated) and gini (table read fails) seeded: must fail, nothing written
        for key in ("kpi/eurpln/series.json", "kpi/gini/series.json"):
            publish.put(client, V2 + key, sentinel)
        publish.gold = failing_gold(real_gold, {"metrics_gini"})
        try:
            publish_v2.write(cat, client, {}, checked_at, v1_catalog, status, stamp, prefix=V2)
            out["v2_run_a"] = {"error": None}
            ok = False
        except ValueError as error:
            out["v2_run_a"] = {"error": str(error)[:300]}
        finally:
            publish.gold = real_gold
        after_a = listing(client, V2)
        out["v2_run_a"]["objects_after"] = sorted(after_a)
        out["v2_run_a"]["unchanged"] = after_a == {"kpi/eurpln/series.json": sentinel, "kpi/gini/series.json": sentinel}
        ok &= "['gini']" in (out["v2_run_a"]["error"] or "") and out["v2_run_a"]["unchanged"]
        # v2 run B: reads work: eurpln removed after the read-back, gini rewritten
        r2 = publish_v2.write(cat, client, {}, checked_at, v1_catalog, status, stamp, prefix=V2)
        after_b = listing(client, V2)
        out["v2_run_b"] = {
            "kpis": len(r2["kpis"]),
            "removed": r2["removed"],
            "eurpln_left": "kpi/eurpln/series.json" in after_b,
            "gini_rewritten": after_b.get("kpi/gini/series.json") not in (None, sentinel),
            "missing": r2["missing"],
            "empty": r2["empty"],
        }
        ok &= r2["removed"] == ["kpi/eurpln/series.json"] and not out["v2_run_b"]["eurpln_left"]
        ok &= out["v2_run_b"]["gini_rewritten"]
    except Exception as error:
        out["error"] = f"{type(error).__name__}: {str(error)[:1500]}"
        traceback.print_exc()
        ok = False
    finally:
        out["scratch_removed"] = clean(client)
    out["verdict"] = bool(ok)
    print(json.dumps(out, indent=2, default=str))
    real_annotate("notice" if ok else "error", "dev: O37 removal trial", out)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
