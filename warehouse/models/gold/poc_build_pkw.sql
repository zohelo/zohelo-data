{{ config(alias='build_pkw', tags=['gold_block']) }}
{# Gold build stage: PKW election results (register PLG-029), observations of silver.pkw_results. One block per
   election type (sejm, pe, sejmik, prezydent); frequency D, date_key = the election day (President: round 1 on
   18 May 2025, round 2 on 1 June 2025, two dates of the same measures). measure_key = pkw:<type>:<unit>:<measure>
   (one frequency per measure_key).
   Selection (the curated Gold of the register entry; every cell stays in Silver):
   - Poland (sejmiki town/country file) and voivodeships: turnout (%), eligible voters, ballots issued (persons),
     valid votes, votes per list or candidate (breakdown committee or candidate, as published) and the share of
     each list (%) where PKW publishes it (Sejm, EP, sejmiki; the President files publish no percentages);
   - powiats and gminas: turnout (%) where published (Sejm, EP, sejmiki), eligible voters and ballots issued
     (President, both rounds). Votes per list or candidate by powiat and gmina stay in Silver: about 100,000 more
     series than the shared dim_series ceiling leaves room for (per-block full coverage, D35, takes them later).
   No share or turnout is computed here: values as published (D33). Abroad (one row per country) and ships are
   not areas and are left out, as are the cells of two committees of one name (is_ambiguous: never picked, the
   Poland file of the sejmiki gives no column that tells them apart); code-less rows map by name (voivodeships of the files without a code column, the
   EP 2024 gmina rows of cities with powiat rights), macros/sources/pkw.sql. #}
with silver as (
    select
        *,
        case area_kind
            when 'country' then 'PL'
            when 'teryt' then area_code
            when 'voivodeship_name' then 'woj:' || lower(area_name)
            when 'city_powiat_name' then 'city:' || lower(wojewodztwo_name) || ':' || lower(area_name)
        end as geography_source_code
    from {{ ref('pkw_results') }}
    where area_kind not in ('abroad', 'ships') and value is not null and not is_ambiguous
),

picked as (
    select *
    from silver
    where
        (
            area_level in ('kraj', 'wojewodztwo')
            and measure_code in (
                'turnout', 'eligible_voters', 'ballots_issued', 'valid_votes', 'list_votes', 'list_share',
                'candidate_votes'
            )
        )
        or (
            area_level in ('powiat', 'gmina')
            and (
                measure_code = 'turnout'
                or (election_type = 'prezydent' and measure_code in ('eligible_voters', 'ballots_issued'))
            )
        )
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'pkw'
)

select
    'pkw' as source_system,
    p.election_type as block_id,
    case p.election_type
        when 'sejm' then 'PKW: elections to the Sejm'
        when 'pe' then 'PKW: elections to the European Parliament'
        when 'sejmik' then 'PKW: elections to the voivodeship assemblies (sejmiki)'
        when 'prezydent' then 'PKW: presidential elections'
    end as block_name,
    'pkw:' || p.election_type || ':' || p.unit_code || ':' || p.measure_code as measure_key,
    case p.measure_code
        when 'turnout' then 'Turnout (%)'
        when 'eligible_voters' then 'Voters entitled to vote'
        when 'ballots_issued' then 'Voters who were issued ballot papers (in person and by post)'
        when 'valid_votes' then 'Valid votes cast for all lists or candidates'
        when 'list_votes' then 'Valid votes cast for the list of a committee'
        when 'list_share' then 'Share of the valid votes cast for the list of a committee (%)'
        when 'candidate_votes' then 'Valid votes cast for a candidate'
    end || ', ' || case p.election_type
        when 'sejm' then 'Sejm elections'
        when 'pe' then 'European Parliament elections'
        when 'sejmik' then 'voivodeship assembly elections'
        when 'prezydent' then 'presidential elections'
    end as measure_name,
    p.measure_code as measure_codes,
    p.unit_code,
    case p.unit_code when 'pct' then '%' when 'persons' then 'persons' when 'votes' then 'votes' end as unit_name,
    p.geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    p.election_date as date_key,
    strftime(p.election_date, '%Y-%m-%d') as period_label,
    case
        when p.breakdown_code is null then '{}'
        when p.measure_code = 'candidate_votes' then '{"candidate": ' || to_json(p.breakdown_code) || '}'
        when p.settlement_code in ('miasto', 'wies')
            then '{"committee": ' || to_json(p.breakdown_code) || ', "settlement": "' || p.settlement_code || '"}'
        else '{"committee": ' || to_json(p.breakdown_code) || '}'
    end as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    p.breakdown_code is null as is_total,
    p.value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from picked as p
left join geo as g on p.geography_source_code = g.source_code
