"""GIOŚ surface-water status assessment 2019-2024: Landing activities (register PL-ENV-011, #3312).

GIOŚ publishes the status classification of the surface water bodies (JCWP) of the State Environmental Monitoring
(PMŚ), assessment 2019-2024, as five public ArcGIS MapServer services of its water portal
(https://wody.gios.gov.pl/arcgis/rest/services/PJWP, ``Ocena_stanu_JCWP_<category>_2019_2024``): rivers, dam
reservoirs, lakes, transitional and coastal waters, one layer per assessment (ecological status, ecological potential,
chemical status, overall status, and for rivers and lakes the overall status with statistical methods; LAYERS, Dev
probe 45922872272). The services answer no pagination (resultOffset: HTTP 400 "Pagination is not supported") and
return at most 1,000 features a query (maxRecordCount), so each layer is read as ArcGIS documents for such services:
its object ids (``returnIdsOnly``), then the features by ``objectIds`` in pages of PAGE ids, attributes only
(``returnGeometry=false``, ``f=json``). A where clause with a FID range was answered HTTP 403 (probe 45922872272) and is
not used.

Entity = one page of one layer, ``<category>_<assessment>_p<nn>``; version = download time (the services give none);
a page lands through the shared bulk_file pattern (exact bytes, receipt after the file) only when its SHA-256 changed.
Restart unit: one page. One request at a time, PAUSE apart; about 30 pages and 19 id lists.

landing.inspect  the pages of every layer (from its current object ids) vs Landing; no writes
landing.extract  every page whose bytes changed (bulk_file load)
landing.verify   every receipt has its file (bulk_file verify)
"""

from __future__ import annotations

import json
import time
from typing import Any

SOURCE = "gios_surface_water"
BASE = "https://wody.gios.gov.pl/arcgis/rest/services/PJWP/Ocena_stanu_JCWP_{}_2019_2024/MapServer/{}/query"
PAUSE, PAGE = 3.0, 1000
# entity prefix (water category _ assessment) -> (service suffix, layer id), from the services' layer lists
LAYERS = {
    "rivers_ecological_status": ("rzeczne", 0),
    "rivers_ecological_potential": ("rzeczne", 1),
    "rivers_chemical_status": ("rzeczne", 2),
    "rivers_overall_status": ("rzeczne", 3),
    "rivers_overall_status_statistical": ("rzeczne", 4),
    "reservoirs_ecological_potential": ("rzeczne_zbiorniki", 0),
    "reservoirs_chemical_status": ("rzeczne_zbiorniki", 1),
    "reservoirs_overall_status": ("rzeczne_zbiorniki", 2),
    "lakes_ecological_status": ("jeziorne", 0),
    "lakes_ecological_potential": ("jeziorne", 1),
    "lakes_chemical_status": ("jeziorne", 2),
    "lakes_overall_status": ("jeziorne", 3),
    "lakes_overall_status_statistical": ("jeziorne", 4),
    "transitional_ecological_status": ("przejsciowe", 0),
    "transitional_chemical_status": ("przejsciowe", 1),
    "transitional_overall_status": ("przejsciowe", 2),
    "coastal_ecological_status": ("przybrzezne", 0),
    "coastal_chemical_status": ("przybrzezne", 1),
    "coastal_overall_status": ("przybrzezne", 2),
}


def object_ids(layer: str) -> list[int]:
    """The layer's object ids, sorted; bounded retries with backoff, a 4xx other than 408/429 fails at once."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    url = BASE.format(*LAYERS[layer]) + "?where=1%3D1&returnIdsOnly=true&f=json"
    error = ""
    for attempt in range(4):
        time.sleep(PAUSE)
        try:
            r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 120))
            if r.status_code == 200 and "objectIds" in r.json():
                return sorted(r.json()["objectIds"] or [])
            if 400 <= r.status_code < 500 and r.status_code not in (408, 429):
                raise bulk_file.PermanentError(f"{url}: HTTP {r.status_code}")
            error = f"HTTP {r.status_code}: {r.text[:120]}"
        except (requests.RequestException, ValueError) as exc:
            error = f"{type(exc).__name__}: {str(exc)[:120]}"
        print(json.dumps({"status": "retry", "url": url, "error": error}), flush=True)
        time.sleep(20 * (attempt + 1))
    raise RuntimeError(f"{url}: no id list after 4 attempts ({error})")


def specs(ids_of=object_ids) -> list[dict[str, Any]]:
    """One bulk_file spec per page of PAGE object ids of every layer; a layer without ids is an error."""
    out = []
    for layer, (service, layer_id) in LAYERS.items():
        ids = ids_of(layer)
        if not ids:
            raise ValueError(f"{SOURCE}: layer {layer} lists no features")
        for n, start in enumerate(range(0, len(ids), PAGE), start=1):
            chunk = ",".join(str(i) for i in ids[start : start + PAGE])
            url = f"{BASE.format(service, layer_id)}?objectIds={chunk}&outFields=*&returnGeometry=false&f=json"
            out.append({"entity": f"{layer}_p{n:02d}", "url": url, "format": "json", "version": "ingested"})
    return out


def held(landing_root: str = "01_landing") -> list[str]:
    """Entities with a landed copy."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    return sorted({r["entity"] for r in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/")})


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    """budget_minutes is unused: about 50 small requests, a few minutes."""
    del budget_minutes
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", landing_root=landing_root, specs=specs(), pause_seconds=PAUSE)


def _inspect(p) -> int:
    """Pages of the current id lists vs Landing (the services send no Last-Modified, so load compares SHA-256)."""
    from ingestion.common.report import annotate

    listed, landed = [s["entity"] for s in specs()], set(held())
    missing = [e for e in listed if e not in landed]
    summary = {"pages": len(listed), "missing": missing, "landed_not_listed": sorted(landed - set(listed))}
    annotate("warning" if missing or summary["landed_not_listed"] else "notice", f"{SOURCE} landing inspect", summary)
    return 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in held()])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
