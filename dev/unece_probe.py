"""Dev probe (#2861/#2885, deleted before merge): read-only shapes of silver.unece_observations for the Gold build
stage: tables, variables and value codes with labels, areas, periods, statuses, Poland's series. No writes.
Report: one summary notice and up to nine notices holding the zlib+base64 JSON detail."""

from __future__ import annotations

import base64
import json
import zlib

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

OUT: dict = {}
SUMMARY: dict = {}


def q(con, key: str, sql: str, limit: int = 2000) -> list:
    try:
        rows = con.sql(sql).limit(limit).fetchall()
        OUT[key] = ["|".join("" if v is None else str(v) for v in r) for r in rows]
        return rows
    except Exception as exc:  # reported, the probe goes on
        OUT[key] = {"error": f"{type(exc).__name__}: {str(exc)[:500]}"}
        return []


def main() -> int:
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    try:
        fields = ("entity", "table_id", "table_title", "series_key", "area_code", "area_name", "dimensions_json",
                  "labels_json", "time_period", "obs_value", "obs_status")
        t = bf.retry(cat.load_table(("silver", "unece_observations")).scan(selected_fields=fields).to_arrow, "silver")
        con.register("s0", t)
        SUMMARY["rows"] = t.num_rows
        con.execute("""create table s as select *, cast(cast(dimensions_json as json) as map(varchar, varchar)) as d,
            cast(cast(labels_json as json) as map(varchar, varchar)) as l,
            case when regexp_matches(time_period, '^[0-9]{4}$') then 'A'
                 when regexp_matches(time_period, '^[0-9]{4}Q[1-4]$') then 'Q'
                 when regexp_matches(time_period, '^[0-9]{4}M[0-9]{2}$') then 'M' else 'other' end as freq
            from s0""")
        rows = q(con, "tables", """select table_id, left(any_value(table_title), 110), count(distinct entity), count(*),
            count(*) filter (where obs_value is null), count(distinct area_code),
            array_to_string(map_keys(any_value(d)), ';'), string_agg(distinct freq, ','),
            min(time_period), max(time_period), coalesce(string_agg(distinct obs_status, ','), ''),
            count(distinct series_key), count(*) filter (where area_code is null),
            count(distinct array_to_string(map_keys(d), ';'))
            from s group by 1 order by 1""")
        SUMMARY["tables"] = len(rows)
        SUMMARY["freqs"] = q(con, "freqs", "select freq, count(*), min(time_period), max(time_period) from s group by 1")
        q(con, "other_periods", "select table_id, time_period, count(*) from s where freq = 'other' group by 1, 2", 60)
        q(con, "variables", """select table_id, k, count(distinct d[k]) from (select table_id, d, unnest(map_keys(d)) as k from s)
            group by 1, 2 order by 1, 2""")
        q(con, "codes", """with x as (select table_id, d, l, unnest(map_keys(d)) as k from s)
            select table_id, k, d[k], left(l[k], 70), count(*) from x where k not in ('Country', 'Country/Region')
            group by 1, 2, 3, 4 order by 1, 2, 3""", 1500)
        q(con, "areas", """select area_code, any_value(area_name), count(*), count(distinct table_id) from s
            group by 1 order by 1""", 200)
        q(con, "status", "select coalesce(obs_status, '-'), count(*), count(distinct table_id), min(obs_value), max(obs_value) from s group by 1 order by 2 desc", 40)
        q(con, "status_examples", """select obs_status, any_value(table_id), any_value(time_period), any_value(obs_value)
            from s where obs_status is not null group by 1""", 40)
        q(con, "pl", """select table_id, series_key, left(labels_json, 150), count(*), max(time_period),
            arg_max(obs_value, time_period) from s where area_code = '616' and freq = 'A' group by 1, 2, 3 order by 1, 2""", 500)
        q(con, "pl_qm", """select table_id, freq, count(distinct series_key), count(*), max(time_period) from s
            where area_code = '616' and freq <> 'A' group by 1, 2 order by 1, 2""", 100)
        q(con, "future", """select table_id, count(*), max(time_period) from s
            where cast(left(time_period, 4) as integer) > year(current_date) group by 1""", 60)
        q(con, "dup_grain", """select table_id, count(*) from (select table_id, series_key, time_period from s
            group by all having count(*) > 1) group by 1""", 60)
        q(con, "zeros", "select table_id, count(*) from s where obs_value = 0 group by 1 order by 2 desc", 60)
        try:
            c = bf.retry(cat.load_table(("bronze", "unece_codes")).scan(
                selected_fields=("entity", "dimension", "dimension_label", "is_time")).to_arrow, "codes")
            con.register("c", c)
            q(con, "dimension_labels", """select dimension, dimension_label, is_time, count(distinct split_part(entity, '__p', 1))
                from c group by 1, 2, 3 order by 1""", 200)
        except Exception as exc:
            OUT["dimension_labels"] = str(exc)[:200]
    except Exception as exc:
        SUMMARY["fatal"] = f"{type(exc).__name__}: {str(exc)[:700]}"
    annotate("notice", "p00 summary", SUMMARY)
    drop_order = ["pl", "zeros", "status_examples"]
    dropped: list[str] = []
    while True:
        OUT["dropped_for_size"] = dropped
        raw = json.dumps(OUT, ensure_ascii=False, separators=(",", ":"), default=str).encode()
        text = base64.b64encode(zlib.compress(raw, 9)).decode()
        if len(text) <= 9 * 3700 or not drop_order:
            break
        key = drop_order.pop(0)
        if key in OUT:
            if key == "pl" and len(OUT[key]) > 150:
                OUT[key] = OUT[key][:150]
                drop_order.insert(0, "pl_all")
                OUT["pl_all"] = OUT.pop("pl")
                continue
            OUT.pop(key)
            dropped.append(key)
    parts = [text[i:i + 3700] for i in range(0, len(text), 3700)][:9]
    for i, part in enumerate(parts, 1):
        annotate("notice", f"p{i:02d} of {len(parts)}", part)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
