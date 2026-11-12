"""Read-only: DBW labels of the missing-value reasons and flags, the BDL attribute of the
P2361 zero, and World Bank LCU values (evidence for metrics_value_range). No writes."""

import json
import urllib.request

from dev.metrics_value_zeros import connect, q
from ingestion.common.report import annotate


def main() -> int:
    con = connect()
    out = {
        "dbw_labels": q(
            con,
            """select lower(nazwa_kolumny), id_elementu, any_value(opis) from r2.gold.src_dbw_dictionary
               where lower(nazwa_kolumny) in ('id_brak_wartosci', 'id_flaga', 'id_tajnosci')
                 and id_elementu in (31, 33, 36, 38, 42, 43, 253, 346, 725, 30, 1, 35)
               group by all order by all""",
        ),
        "bdl_p2361_zero": q(
            con,
            """select unit_code, unit_name, year, value, value_text, attribute, variable_id, channel
               from r2.silver.bdl_observations where subgroup_id = 'P2361' and value = 0""",
        ),
        "bdl_p2361_attr": q(
            con, "select attribute, count(*) from r2.silver.bdl_observations where subgroup_id='P2361' group by 1"
        ),
        "bdl_x_zero_share": q(
            con,
            """select attribute, count(*) filter (where value = 0), count(*) filter (where value <> 0),
                      count(*) filter (where value is null) from r2.silver.bdl_observations
               where attribute is not null group by 1 order by 2 desc limit 12""",
        ),
    }
    for c in ("IR", "RS"):
        url = f"https://api.worldbank.org/v2/country/{c}/indicator/NY.GDP.MKTP.CN?format=json&date=1990:2025&per_page=100"
        try:
            r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "zohelo-dev"}), timeout=170)
            rows = json.loads(r.read())[1] or []
            out[f"wb_{c}"] = {x["date"]: x["value"] for x in rows if x["date"] in ("1990", "1991", "2024", "2025")}
        except Exception as e:
            out[f"wb_{c}"] = f"error: {str(e)[:150]}"
    annotate("notice", "metrics_value_range labels", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
