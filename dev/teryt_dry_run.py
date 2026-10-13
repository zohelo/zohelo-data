"""Dry run: download the 8 TERYT full files, read versions, decode to Parquet locally; no writes."""
import re
import tempfile
import time
import traceback
from pathlib import Path

import requests

from ingestion.common.report import annotate
from ingestion.to_bronze.gus_teryt.load import decode
from ingestion.to_landing.gus_teryt.extract import CONTROLS, USER_AGENT, catalogue_date, request

s = requests.Session()
s.headers["User-Agent"] = USER_AGENT
html = request(s, "GET").text
fields = dict(re.findall(r'<input[^>]+name="(__[A-Z]+)"[^>]+value="([^"]*)"', html))
result = {}
with tempfile.TemporaryDirectory() as t:
    for entity, control in CONTROLS.items():
        time.sleep(2)
        try:
            body = request(s, "POST", {**fields, "__EVENTTARGET": control, "__EVENTARGUMENT": ""}).content
            version = catalogue_date(body)
            rows = decode(body, {"entity": entity, "sha256": "0" * 64, "version_token": version,
                                 "ingested_at": "20261013T000000Z"}, Path(t) / f"{entity}.parquet")
            result[entity] = {"version": version, "zip_bytes": len(body), "rows": rows}
        except Exception:
            result[entity] = traceback.format_exc()[-700:]
annotate("notice", "teryt dry run", result)
