"""Trial (read-only): the migrated sources' landing.inspect and landing.verify run through
run.py's generic bulk_file dispatch; Bronze loaders list Landing through the shared module."""
import traceback

import run
from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

try:
    out = {}
    for source, entity in (("world_bank_wdi", "WDI_CSV"), ("gugik_prg", "jednostki_administracyjne")):
        acts = run.implemented(source)
        out[source] = {"inspect": acts["landing.inspect"](None), "verify": acts["landing.verify"](None),
                       "bronze_view": len(bulk_file.landed(s3(), req("R2_LANDING_BUCKET"), bulk_file.prefix(source, entity)))}
    annotate("notice", "bulk_file dispatch trial", out)
except Exception:
    annotate("error", "bulk_file dispatch trial", {"traceback": traceback.format_exc()[-2000:]})
    raise
