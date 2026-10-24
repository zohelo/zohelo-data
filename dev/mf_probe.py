"""Read-only: MF white-list flat file structure. Deleted before merge."""

from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import traceback
from zoneinfo import ZoneInfo

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
day = datetime.now(ZoneInfo("Europe/Warsaw")).strftime("%Y%m%d")
url = f"https://plikplaski.mf.gov.pl/pliki/{day}.7z"
out = {"url": url, "7z": shutil.which("7z"), "7za": shutil.which("7za"), "7zr": shutil.which("7zr")}


def run(out):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "f.7z"
        with requests.get(url, headers={**UA, "Accept-Encoding": "identity"}, stream=True, timeout=(30, 600)) as r:
            out["status"], out["headers"] = (
                r.status_code,
                {k: r.headers.get(k) for k in ("Content-Length", "Last-Modified", "ETag", "Content-Type")},
            )
            with path.open("wb") as h:
                for b in r.iter_content(8 << 20):
                    h.write(b)
        tool = out["7z"] or out["7za"]
        out["list"] = subprocess.run([tool, "l", str(path)], capture_output=True, text=True).stdout[-1500:]
        subprocess.run([tool, "x", "-o" + tmp, str(path)], capture_output=True, check=True)
        files = sorted((p for p in Path(tmp).iterdir() if p.name != "f.7z"), key=lambda p: not p.name.endswith(".json"))
        out["files"] = [(p.name, p.stat().st_size) for p in files]
        with files[0].open("rb") as h:
            head = h.read(1500)
            h.seek(-800, 2)
            tail = h.read(800)
        out["head"], out["tail"] = head.decode("utf-8", "replace"), tail.decode("utf-8", "replace")
        try:
            data = json.loads(files[0].read_text()) if files[0].stat().st_size < 3e9 else {}
            out["keys"] = {k: (len(v) if isinstance(v, list) else v) for k, v in data.items()}
            out["mask_sample"] = data.get("maski", [])[:5]
        except Exception as exc:
            out["json_error"] = repr(exc)[:300]


try:
    run(out)
except Exception:
    out["error"] = traceback.format_exc()[-1500:]
annotate("notice", "mf probe", {k: out[k] for k in out if k not in ("head", "tail", "list", "error")})
annotate("notice", "mf probe head", {"list": out.get("list"), "head": out.get("head")})
annotate("notice", "mf probe tail", {"tail": out.get("tail")})

annotate("notice", "mf probe error", {"error": out.get("error")})
