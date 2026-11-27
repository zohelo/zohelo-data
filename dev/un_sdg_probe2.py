"""Probe 4 for GL-018 (#3316): robots.txt of unstats.un.org (Accept */*), and in the current release's CSV ZIP
(2026_Q3.1_AllData_After_20260930_CSV.zip, downloaded to the runner's disk, not R2): the values under the missing-value
observation statuses and nature N, the period range, censored values. GET to unstats.un.org only, one request at a
time, pauses, identifying User-Agent. Deleted before merge (never on the PR branch).
"""

from __future__ import annotations

import json
import time
import urllib.robotparser
import zipfile

import duckdb
import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HOST = "https://unstats.un.org"
S = requests.Session()
S.headers.update({"User-Agent": UA})
EU = ("('40','56','100','191','196','203','208','233','246','250','276','300','348','372','380','428','440','442',"
      "'470','528','616','620','642','703','705','724','752')")
URL = f"{HOST}/sdgs/indicators/database/archive/2026_Q3.1_AllData_After_20260930_CSV.zip"


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, data if len(text) < 3800 else {"truncated": text[:3800]})


def main() -> int:
    r = S.get(f"{HOST}/robots.txt", timeout=(20, 60), headers={"Accept": "*/*"})
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(r.text.splitlines())
    note("sdg probe4 robots", {"st": r.status_code, "ct": r.headers.get("content-type"), "text": r.text[:1500],
                               "archive_allowed": parser.can_fetch(UA, f"{HOST}/sdgs/indicators/database/archive"),
                               "zip_allowed": parser.can_fetch(UA, URL),
                               "api_allowed": parser.can_fetch(UA, f"{HOST}/SDGAPI/v1/sdg/Series/List")})
    time.sleep(3)
    with S.get(URL, timeout=(20, 900), stream=True) as resp, open("/tmp/sdg.zip", "wb") as f:
        for chunk in resp.iter_content(1 << 20):
            f.write(chunk)
    with zipfile.ZipFile("/tmp/sdg.zip") as z:
        member = z.infolist()[0]
        z.extract(member, "/tmp/sdg")
    con = duckdb.connect()
    con.execute(f"""create table e as select * from read_csv('/tmp/sdg/{member.filename}', all_varchar=true,
                    header=true) where "GeoAreaCode" in {EU}""")

    def q(s):
        return con.execute(s).fetchall()

    out = {
        "status_values": q("""select "Observation Status", case when "Value" = 'NaN' then 'NaN' when try_cast("Value" as double) = 0
            then 'zero' when try_cast("Value" as double) is not null then 'number' else 'text' end, count(*) from e group by all order by all"""),
        "nature_values": q("""select "Nature", case when "Value" = 'NaN' then 'NaN' when try_cast("Value" as double) = 0
            then 'zero' when try_cast("Value" as double) is not null then 'number' else 'text' end, count(*) from e group by all order by all"""),
        "nature_N_series": q("""select "SeriesCode", count(*), list(distinct "Value")[:5] from e where "Nature" = 'N' group by 1 order by 2 desc limit 10"""),
        "status_OQM_series": q("""select "Observation Status", "SeriesCode", count(*), list(distinct "Value")[:5] from e
            where "Observation Status" in ('M', 'O', 'Q') group by all order by 3 desc limit 12"""),
        "period_range": q('select min("TimePeriod"), max("TimePeriod") from e'),
        "periods_after_2025": q("""select "TimePeriod", count(*), count(distinct "SeriesCode"), list(distinct "SeriesCode")[:8] from e
            where "TimePeriod" > '2025' group by 1 order by 1"""),
        "nan_series": q("""select count(distinct "SeriesCode") from e where "Value" = 'NaN'"""),
        "censored_series": q("""select "SeriesCode", "Value", count(*) from e where "ValueType" = 'String' group by all order by 3 desc limit 8"""),
        "women_parl": q("""select "SeriesCode", "SeriesDescription", "Units", "GeoAreaCode", "TimePeriod", "Value", "Sex", "Nature", "Source"
            from e where "SeriesCode" = 'SG_GEN_PARL' and "GeoAreaCode" = '616' order by "TimePeriod" desc limit 4"""),
        "women_parl_counts": q("""select count(*), count(distinct "GeoAreaCode"), min("TimePeriod"), max("TimePeriod"), min(try_cast("Value" as double)),
            max(try_cast("Value" as double)) from e where "SeriesCode" = 'SG_GEN_PARL'"""),
    }
    note("sdg probe4 values", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
