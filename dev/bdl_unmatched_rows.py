"""Dev diagnostic for #2307: the gus_bdl rows that fall to geography _X (prod Gold, read only)."""

import subprocess

from ingestion.common.report import annotate

BASE = ["--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse"]

QUERIES = {
    "bdl _X by block and year": """
        select block_id, year(date_key) as y, count(*) as n, count(distinct geography_source_code) as codes,
               (list(distinct geography_source_code))[1:8] as sample
        from {{ ref('poc_build_bdl') }} where geography_key = '_X' group by all order by n desc""",
    "P2137 _X units": """
        with x as (select distinct geography_source_code as code from {{ ref('poc_build_bdl') }}
                   where block_id = 'P2137' and geography_key = '_X')
        select o.unit_code, o.unit_name, u.name as id_name, u.level, o.channel, min(o.year) as y0,
               max(o.year) as y1, count(*) as n
        from {{ source('silver_entities', 'bdl_observations') }} as o
        inner join x on o.unit_code = x.code
        left join {{ ref('bdl_units') }} as u on u.unit_id = o.unit_code
        where o.subgroup_id = 'P2137' group by all order by n desc""",
    "metrics _X rows": """
        select 'population' as k, source_system, count(*) as n from {{ ref('poc_metrics_population') }}
            where geography_key = '_X' group by all
        union all select 'gdp', source_system, count(*) from {{ ref('poc_metrics_gdp_current_prices') }}
            where geography_key = '_X' group by all
        union all select 'inflation', source_system, count(*) from {{ ref('poc_metrics_inflation') }}
            where geography_key = '_X' group by all
        union all select 'unemployment', source_system, count(*) from {{ ref('poc_metrics_unemployment_rate') }}
            where geography_key = '_X' group by all
        union all select 'fx', source_system, count(*) from {{ ref('poc_metrics_fx_rate') }}
            where geography_key = '_X' group by all""",
}


def main() -> int:
    """Run each query with dbt show and annotate its output."""
    for title, sql in QUERIES.items():
        shown = subprocess.run(["dbt", "show", "--inline", sql, "--limit", "40", *BASE], capture_output=True, text=True)
        out = shown.stdout if shown.returncode == 0 else shown.stdout + shown.stderr
        annotate("notice", title, {"rc": shown.returncode, "out": out[-3500:]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
