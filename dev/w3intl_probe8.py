"""GEM series metadata (read-only): name and source note of the eight declared series (third-party terms check)."""

from dev.w3intl_probe import annotate, js
from dev.w3intl_probe3 import call

SERIES = ["CPTOTSAXNZGY", "CPTOTSAXN", "CORESA", "IPTOTSAKD", "UNEMPSA_", "RETSALESSA", "DPANUSSPB", "NYGDPMKTPSAKD"]


def main():
    out = {}
    for s in SERIES:
        r = call("GET", f"https://api.worldbank.org/v2/sources/15/series/{s}/metadata?format=json", timeout=120, tries=3)
        d = js(r) if "body" in r else None
        try:
            fields = {m["id"]: m["value"] for m in d["source"][0]["concept"][0]["variable"][0]["metatype"]}
            out[s] = {k: (v or "")[:300] for k, v in fields.items() if k in ("IndicatorName", "Source", "License_Type", "License_URL", "Unitofmeasure", "Periodicity", "Topic", "Longdefinition")}
        except Exception:  # noqa: BLE001
            out[s] = {"st": r.get("st"), "body": (r.get("body") or b"")[:300].decode("utf-8", "replace")}
    first = dict(list(out.items())[:4]); rest = dict(list(out.items())[4:])
    annotate("notice", "probe gem metadata 1/2", first)
    annotate("notice", "probe gem metadata 2/2", rest)


if __name__ == "__main__":
    main()
