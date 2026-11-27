"""Dev probe (read-only): profiles of four production Silver dbt tables before their per-block Gold rule (#3314):
silver.owid_observations, owid_charts, unesco_uis_observations, edstats_pisa, gem_observations. Reads only; writes
nothing. At most 5 annotations. Deleted before merge.

python -m dev.gold3314_probe
"""

from __future__ import annotations

import traceback

import duckdb

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate


def arrow(cat, name):
    return cat.load_table(("silver", name)).scan().to_arrow()


def q(con, sql, limit=40):
    return con.execute(sql).fetchall()[:limit]


def main() -> None:
    cat = catalog()
    con = duckdb.connect()
    for name in ("owid_observations", "owid_charts", "unesco_uis_observations", "edstats_pisa", "gem_observations"):
        con.register(name, arrow(cat, name))
    try:
        annotate(
            "notice",
            "gold3314 probe: owid",
            {
                "rows_value_years": q(
                    con,
                    "select chart, indicator, count(*), count(value), min(year), max(year), "
                    "count(*) filter (where year < 1800), count(*) filter (where year < 1900), "
                    "count(*) filter (where entity_code is null), count(*) filter (where value = 0), "
                    "count(*) filter (where isnan(value) or isinf(value)) from owid_observations group by all order by 1",
                ),
                "versions": q(con, "select chart, min(source_version), max(source_version) from owid_observations group by 1"),
                "code_names": q(
                    con,
                    "select entity_code, count(distinct entity_name), min(entity_name), max(entity_name) "
                    "from owid_observations where entity_code is not null group by 1 having count(distinct entity_name) > 1",
                ),
                "name_codes": q(
                    con,
                    "select entity_name, count(distinct coalesce(entity_code, '-')) from owid_observations "
                    "group by 1 having count(distinct coalesce(entity_code, '-')) > 1",
                ),
                "no_code_names": q(
                    con, "select distinct entity_name from owid_observations where entity_code is null order by 1", 80
                ),
                "owid_codes": q(
                    con, "select distinct entity_code from owid_observations where entity_code like 'OWID%' order by 1", 60
                ),
                "charts": q(con, "select chart, short_name, unit, last_updated::varchar, source_version from owid_charts"),
            },
        )
        annotate(
            "notice",
            "gold3314 probe: unesco_uis",
            {
                "rows": q(
                    con,
                    "select entity, indicator_id, count(*), count(value), min(year), max(year), "
                    "count(*) filter (where value = 0), count(*) filter (where not is_country_code) "
                    "from unesco_uis_observations group by all order by 1, 2",
                ),
                "magnitude_qualifier": q(
                    con,
                    "select magnitude, qualifier, count(*), count(value), count(*) filter (where value = 0) "
                    "from unesco_uis_observations group by all order by 3 desc",
                ),
                "versions": q(con, "select entity, min(source_version), max(source_version) from unesco_uis_observations group by 1"),
                "non_country_units": q(
                    con, "select geo_unit, count(*) from unesco_uis_observations where not is_country_code group by 1 order by 1", 80
                ),
                "short_odd_units": q(
                    con,
                    "select distinct geo_unit from unesco_uis_observations where is_country_code and geo_unit not in "
                    "(select geo_unit from unesco_uis_observations where length(geo_unit) = 3) limit 5",
                ),
            },
        )
        annotate(
            "notice",
            "gold3314 probe: edstats",
            {
                "rows": q(
                    con,
                    "select entity, indicator_id, sex_code, count(*), count(value), min(year), max(year), "
                    "count(*) filter (where value = 0), count(distinct country_iso3), count(*) filter (where country_iso3 is null) "
                    "from edstats_pisa group by all order by 1, 2",
                ),
                "obs_status": q(con, "select obs_status, count(*) from edstats_pisa group by 1"),
                "versions": q(con, "select min(source_version), max(source_version), min(source_last_updated), max(source_last_updated) from edstats_pisa"),
                "economies": q(con, "select country_iso3, country_id, min(country_name) from edstats_pisa group by all order by 1", 120),
            },
        )
        annotate(
            "notice",
            "gold3314 probe: gem",
            {
                "rows": q(
                    con,
                    "select series_code, frequency, count(*), count(value), min(period_start)::varchar, "
                    "max(period_start)::varchar, count(distinct country_code), count(*) filter (where value = 0), "
                    "count(*) filter (where isnan(value) or isinf(value)) from gem_observations group by all order by 1, 2",
                ),
                "versions": q(con, "select entity, min(source_version), max(source_version) from gem_observations group by 1"),
                "countries": q(con, "select distinct country_code from gem_observations order by 1", 60),
                "newest_pl": q(
                    con,
                    "select series_code, frequency, max(time_code), arg_max(value, period_start) from gem_observations "
                    "where country_code = 'POL' group by all order by 1, 2",
                ),
            },
        )
    except Exception:
        annotate("error", "gold3314 probe: failed", {"traceback": traceback.format_exc()[-3000:]})


if __name__ == "__main__":
    main()
