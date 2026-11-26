{{ config(alias='pkw_elections') }}
{# Gold entity table: the elections of the PKW results (register PLG-029), one row per election and round, keyed by
   PKW's own site name (with the round for the President). The build stage dates each result by election_date
   (models/gold/poc_build_pkw.sql), so this table names the election behind a date_key of the pkw blocks. #}
select
    election_key,
    election_type,
    election_round,
    election_date,
    case election_type
        when 'sejm' then 'Wybory do Sejmu RP'
        when 'pe' then 'Wybory do Parlamentu Europejskiego'
        when 'sejmik' then 'Wybory do sejmików województw'
        when 'prezydent' then 'Wybory Prezydenta RP'
    end || ' ' || year(election_date)
    || case when election_type = 'prezydent' then ', tura ' || election_round else '' end as name_pl,
    case election_type
        when 'sejm' then 'Elections to the Sejm'
        when 'pe' then 'Elections to the European Parliament'
        when 'sejmik' then 'Elections to the voivodeship assemblies'
        when 'prezydent' then 'Presidential election'
    end || ' ' || year(election_date)
    || case when election_type = 'prezydent' then ', round ' || election_round else '' end as name_en,
    count(distinct entity) as files,
    count(distinct breakdown_code) filter (where measure_code in ('list_votes', 'candidate_votes'))
        as lists_or_candidates,
    max(source_version) as source_version
from {{ ref('pkw_results') }}
group by election_key, election_type, election_round, election_date
