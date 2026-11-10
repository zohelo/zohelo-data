"""Read-only: why the Bronze decoder rejects stopy_procentowe.xml; prints the file (3 kB) to the log."""

import requests

from ingestion.to_bronze.nbp_interest_rates.load import decode

URL = "https://static.nbp.pl/dane/stopy/stopy_procentowe.xml"

body = requests.get(URL, headers={"User-Agent": "zohelo-data/1.0"}, timeout=60).content
print(body.decode("utf-8", "replace"))
try:
    print(decode(body, {"entity": "rates_current", "sha256": "x", "version_token": "v", "ingested_at": "20261110T000000Z"}).num_rows)
except Exception as exc:
    print("DECODE ERROR", type(exc).__name__, exc)
