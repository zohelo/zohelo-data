"""Read-only: which BDL subgroups are missing from Landing, and their export URLs."""
import traceback

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.gus_bdl.api import Bdl
from ingestion.to_landing.gus_bdl.inspect import landing_versions
from ingestion.to_landing.gus_bdl.web_export import missing_subgroups

try:
    client = s3()
    held = landing_versions(client, req("R2_LANDING_BUCKET"))
    api = Bdl()
    todo = missing_subgroups(api, held)
    annotate("notice", "bdl missing subgroups", {"landing_subgroups": len(held), "missing": len(todo),
             "api_requests": api.requests, "sample": [[s["id"], s["name"][:60], s["url"]] for s in todo[:12]]})
except Exception:
    annotate("error", "bdl web export trial", {"traceback": traceback.format_exc()[-1500:]})
    raise
