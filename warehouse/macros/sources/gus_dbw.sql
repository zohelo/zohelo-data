{# GUS DBW geography codes (D34 registration, docs/source-registration.md): territory names of the
   cross-section positions. #}
{% macro poc_geography__gus_dbw() %}
-- DBW cross-section positions are territory names; matched by name (upper) to Polish areas.
gus_dbw_names as (  -- position labels of the activated DBW blocks (extracted dictionaries)
    select distinct upper(trim(opis)) as source_code
    from {{ ref('poc_src_dbw_dictionary') }}
    where lower(nazwa_kolumny) like 'id_pozycja_%'
),

gus_dbw as (
    select
        'gus_dbw' as source_system,
        d.source_code,
        coalesce(
            case when d.source_code = 'POLSKA' then 'PL' end,
            case when d.source_code like 'MAKROREGION WOJEW%MAZOWIECKIE' then 'TERC:14' end,
            g_woj.geography_key,
            g_nuts.geography_key,
            g_pod.geography_key,
            '_X'
        ) as geography_key
    from gus_dbw_names as d
    left join geo as g_woj
        on g_woj.level_name = 'wojewodztwo' and upper(g_woj.name_pl) = regexp_replace(d.source_code, '^REGION ', '')
    left join geo as g_nuts
        on
            g_nuts.level_name in ('makroregion', 'region')
            and regexp_replace(upper(g_nuts.name_pl), '^(MAKROREGION|REGION) ', '')
            = regexp_replace(d.source_code, '^(MAKROREGION|REGION) ', '')
    left join geo as g_pod
        on
            g_pod.level_name = 'podregion'
            and regexp_replace(regexp_replace(upper(g_pod.name_pl), '^PODREGION ', ''), '^(M\. ST\. |M\. |MIASTO )', '')
            = regexp_replace(regexp_replace(d.source_code, '^PODREGION ', ''), '^(M\. ST\. |M\. |MIASTO )', '')
)
{% endmacro %}
