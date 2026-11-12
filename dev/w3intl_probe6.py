"""EEA round 3 (read-only): header and README of the newest GovReg CSV zip through the public share WebDAV."""

import re
import zlib

from dev.w3intl_probe import annotate, log
from dev.w3intl_probe3 import call

DAV = "https://sdi.eea.europa.eu/datashare/public.php/webdav/"


def main():
    out = {}
    r = call("GET", "https://sdi.eea.europa.eu/data/f6a252a6-1bde-43a8-9e6a-f58146fd691e", timeout=60, tries=2)
    token = re.findall(r"datashare/s/([A-Za-z0-9]+)", (r.get("body") or b"").decode("utf-8", "replace"))[0]
    out["token_from_html"] = token
    r = call("GET", DAV + "README.md", timeout=60, tries=2, auth=(token, ""))
    out["readme"] = (r.get("body") or b"").decode("utf-8", "replace")[:1100]
    r = call("GET", DAV + "UNFCCC_33_csv.zip", timeout=60, tries=2, auth=(token, ""), headers={"Range": "bytes=0-65535"})
    body = r.get("body") or b""
    out["zip_status"] = r.get("st")
    i = body.find(b"PK\x03\x04", 4)  # second local header: the CSV (first is the folder entry)
    n, m = int.from_bytes(body[i + 26:i + 28], "little"), int.from_bytes(body[i + 28:i + 30], "little")
    out["member"] = body[i + 30:i + 30 + n].decode()
    text = zlib.decompressobj(-15).decompress(body[i + 30 + n + m:])
    lines = text.decode("utf-8", "replace").splitlines()
    out["head"] = lines[:6]
    log("eea csv head", lines[:40])
    r = call("GET", DAV + "UNFCCC_v33.csv", timeout=60, tries=2, auth=(token, ""), headers={"Range": "bytes=0-3000"})
    out["plain_csv_head"] = (r.get("body") or b"")[:600].decode("utf-8", "replace")
    annotate("notice", "probe3 eea csv", out)


if __name__ == "__main__":
    main()
