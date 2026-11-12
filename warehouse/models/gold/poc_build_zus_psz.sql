{{ config(alias='build_zus_psz', tags=['gold_block']) }}
{# Gold build stage: ZUS statistical portal (register PLS-009, silver.zus_psz_observations). One block per table
   family, one frequency per measure:
     pensioners_dec         old-age pensioners in December (A), persons, by sex and age group
     pension_amount_dec     average old-age pension in December (A), PLN, by sex
     insured_pension        insured persons in pension insurance at the quarter end (Q), by sex and age group
     insured_sickness_voiv  insured persons in sickness insurance at the quarter end (Q), Poland and voivodeships
     insured_health         insured persons in health insurance (M)
     foreign_insured        foreign citizens in pension insurance at the month end (M), all and Ukrainian
     payers_voiv            active contribution payers at the quarter end (Q), by payer type, Poland and voivodeships
     absence_voiv           sick-leave days and medical certificates in the quarter (Q), Poland and voivodeships
   The area is '00' (Poland) or the TERYT voivodeship code, mapped in poc_map_geography. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'zus_psz'
),

s as (
    select
        *,
        case family
            when 'pensioners_dec' then 'age'
            when 'insured_pension' then 'age'
            when 'payers_voiv' then 'payer_type'
            when 'foreign_insured' then 'citizenship'
        end as breakdown_dim,
        case sex_code when 'T' then 'ogółem' when 'M' then 'mężczyźni' when 'F' then 'kobiety' end as sex_label
    from {{ ref('zus_psz_observations') }}
),

shaped as (
    select
        s.*,
        case
            when s.family in ('pensioners_dec', 'pension_amount_dec', 'insured_pension')
                then
                    case
                        when s.breakdown_dim is null then map(['sex'], [s.sex_code])
                        else map(['sex', s.breakdown_dim], [s.sex_code, s.breakdown_code])
                    end
            when s.breakdown_dim is not null then map([s.breakdown_dim], [s.breakdown_code])
        end as breakdown_map,
        case
            when s.family in ('pensioners_dec', 'pension_amount_dec', 'insured_pension')
                then
                    case
                        when s.breakdown_dim is null then map(['sex'], [s.sex_label])
                        else map(['sex', s.breakdown_dim], [s.sex_label, s.row_label])
                    end
            when s.breakdown_dim is not null
                then map([s.breakdown_dim], [s.column_label])
        end as breakdown_label_map
    from s
)

select
    'zus_psz' as source_system,
    x.family as block_id,
    case x.family
        when 'pensioners_dec' then 'ZUS old-age pensioners in December'
        when 'pension_amount_dec' then 'ZUS average old-age pension in December'
        when 'insured_pension' then 'ZUS insured persons, pension insurance'
        when 'insured_sickness_voiv' then 'ZUS insured persons, sickness insurance, by voivodeship'
        when 'insured_health' then 'ZUS insured persons, health insurance'
        when 'foreign_insured' then 'ZUS foreign citizens in pension insurance'
        when 'payers_voiv' then 'ZUS active contribution payers, by voivodeship'
        when 'absence_voiv' then 'ZUS sickness absence, by voivodeship'
    end as block_name,
    'zus_psz:' || x.family || ':' || x.unit_code || ':' || x.measure as measure_key,
    case x.measure
        when 'pensioners' then 'Persons receiving an old-age pension paid by ZUS, December'
        when 'avg_pension' then 'Average old-age pension paid by ZUS per person, December (PLN)'
        when 'insured_pension' then 'Insured persons in pension insurance, quarter end'
        when 'insured_sickness' then 'Insured persons in sickness insurance, quarter end'
        when 'insured_health' then 'Insured persons in health insurance (ZUS), month'
        when 'foreign_insured' then 'Insured persons with a foreign citizenship, pension insurance, month end'
        when 'payers' then 'Active contribution payers, pension insurance, quarter end'
        when 'sick_leave_days' then 'Sick-leave days, own illness of persons insured in ZUS, quarter'
        when 'sick_leave_certificates' then 'Medical certificates of sick leave, own illness, quarter'
    end as measure_name,
    x.measure as measure_codes,
    x.unit_code,
    case x.unit_code
        when 'PLN' then 'zloty'
        else x.unit_code
    end as unit_name,
    x.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    x.frequency,
    x.period_start as date_key,
    x.period_label,
    coalesce(cast(to_json(x.breakdown_map) as varchar), '{}') as breakdown_json,
    cast(to_json(x.breakdown_label_map) as varchar) as breakdown_label_json,
    case when x.family in ('pensioners_dec', 'pension_amount_dec', 'insured_pension') then x.sex_code end as sex_code,
    x.sex_code = 'T' and x.breakdown_code = '_T' as is_total,
    x.value,
    'ok' as observation_status_key,
    x.source_version,
    x.raw_sha256
from shaped as x
left join geo as g on x.area_code = g.source_code
