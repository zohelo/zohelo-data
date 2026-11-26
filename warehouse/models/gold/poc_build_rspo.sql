{{ config(alias='build_rspo', tags=['gold_block']) }}
{# Gold build stage (register PLS-021, #3031): RSPO register counts from the yearly lists of schools and educational
   institutions (state of 30 September, 2020-2025). Block 'institutions', frequency A: date_key = 1 January of the
   reference year, period_label = the reference date (30.09.<year>). One measure, register entries (every school,
   institution, complex and branch the list holds: a complex and its member schools are each an entry; unit kind is in
   Silver), unit 'schools' ("schools and institutions"). The total (is_total, breakdown {}) and three breakdowns, one at
   a time, codes as published: institution_type (idTypPodmiotu), public_status (idPublicznosc), governing_body_class
   (typ_org_prow); labels of the codes in breakdown_label_json (a code with two labels in one list fails the build).
   Areas: Poland and the entry's voivodeship, powiat and gmina (TERYT codes as published; macros/sources/rspo.sql);
   every entry has all three, so each level adds up to Poland. raw_sha256: the list's file (one file per year). #}
with entries as (
    select
        reference_year,
        reference_date,
        voivodeship_teryt,
        powiat_teryt,
        gmina_teryt,
        institution_type_id,
        institution_type_name,
        public_status_id,
        public_status_name,
        governing_body_class,
        source_version,
        raw_sha256
    from {{ ref('rspo_institutions') }}
),

areas as (  -- each entry counted for Poland and for its voivodeship, powiat and gmina
    select
        reference_year,
        reference_date,
        institution_type_id,
        institution_type_name,
        public_status_id,
        public_status_name,
        governing_body_class,
        source_version,
        raw_sha256,
        unnest(['PL', voivodeship_teryt, powiat_teryt, gmina_teryt]) as area_code
    from entries
),

cuts as (  -- the total and one breakdown at a time: (dimension, code, label)
    select
        reference_year,
        reference_date,
        area_code,
        source_version,
        raw_sha256,
        unnest([
            struct_pack(dimension := '_T', code := '_T', label := cast(null as varchar)),
            struct_pack(dimension := 'institution_type', code := institution_type_id, label := institution_type_name),
            struct_pack(dimension := 'public_status', code := public_status_id, label := public_status_name),
            struct_pack(
                dimension := 'governing_body_class', code := governing_body_class, label := governing_body_class
            )
        ]) as cut
    from areas
),

flat as (
    select
        reference_year,
        reference_date,
        area_code,
        source_version,
        raw_sha256,
        struct_extract(cut, 'dimension') as dimension,
        struct_extract(cut, 'code') as code,
        struct_extract(cut, 'label') as label
    from cuts
),

counted as (
    select
        reference_year,
        reference_date,
        area_code,
        dimension,
        code,
        min(label) as label,
        count(distinct label) as labels,
        count(*) as value,
        max(source_version) as source_version,
        min(raw_sha256) as raw_sha256,
        count(distinct raw_sha256) as files
    from flat
    group by reference_year, reference_date, area_code, dimension, code
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'rspo'
)

select
    'rspo' as source_system,
    'institutions' as block_id,
    'RSPO schools and educational institutions (counts)' as block_name,
    'rspo:institutions:schools:entries' as measure_key,
    'Entries in the RSPO register of schools and educational institutions (state of 30 September)' as measure_name,
    'entries' as measure_codes,
    'schools' as unit_code,
    'schools and institutions' as unit_name,
    c.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(c.reference_year, 1, 1) as date_key,
    strftime(c.reference_date, '%d.%m.%Y') as period_label,
    case when c.dimension = '_T' then '{}' else cast(json_object(c.dimension, c.code) as varchar) end as breakdown_json,
    case
        when c.dimension = '_T' then cast(null as varchar)
        when c.labels > 1
            then error('build_rspo: ' || c.dimension || ' ' || c.code || ' has two labels in ' || c.reference_year)
        else cast(json_object(c.dimension, c.label) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.dimension = '_T' as is_total,
    cast(c.value as double) as value,
    'ok' as observation_status_key,
    c.source_version,
    case
        when c.files = 1 then c.raw_sha256
        else error('build_rspo: two files for ' || c.reference_year)
    end as raw_sha256
from counted as c
left join geo as g on c.area_code = g.source_code
