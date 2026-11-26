{# PKW election results (register PLG-029): every value cell of the area-level result files of Sejm 2023, the European
   Parliament 2024, the voivodeship assemblies (sejmiki) 2024 and the President 2025 (both rounds), from the newest
   copy of each declared file (bronze.pkw_cells; ingestion/sources/pkw.yaml).
   Grain: entity x source_row x column_number (one published cell); the semantic key election_id x area_level x
   file_kind x area_key x column_label x breakdown_code is tested unique too.
   Files are wide: row 1 is the header; key columns (TERYT code, gmina, powiat, voivodeship, district number, unit
   type) identify the area, every other column is a value. measure_code classifies the header: turnout, eligible
   voters, ballots issued, valid and invalid votes (counts and %, as published), list_votes / list_share (one column
   per committee: the header is the committee name) and candidate_votes (President: one column per candidate);
   the other protocol columns (commissions, ballots, envelopes) are 'protocol'. A header the model does not know is
   'unknown' and fails accepted_values. The sejmiki file 'kraj' is committees in rows by town and country
   (settlement_code miasto, wies, ogolem), Poland only.
   area_code is the TERYT code as published, padded to 6 digits (the CSV files drop the leading zero) and cut to the
   level: voivodeship WW, powiat WWPP, gmina WWPPGG (PKW gives no gmina type digit). Rows without a code: abroad
   (Powiat 'zagranica', one row per country), ships ('statki'), voivodeships of the files without a code column (by
   name), and in the EP 2024 gmina file the 66 cities with powiat rights (code only at powiat level). Values as
   published (decimal comma read), never rescaled; an empty cell is absent (Bronze keeps non-empty cells only); a
   non-numeric cell keeps value null and fails the completeness test.
   is_ambiguous marks cells whose semantic key repeats: two committees of one name (the sejmiki 2024 town/country
   file lists 'KOMITET WYBORCZY WYBORCÓW RUCH OBRONY POLSKI' twice, registered in two voivodeships, with no column
   that tells them apart). Both are kept here and left out of Gold; neither is picked. #}
with elections (election_key, election_type, election_round, election_date) as (
    values
    ('sejm2023', 'sejm', 1, date '2023-10-15'),
    ('pe2024', 'pe', 1, date '2024-06-09'),
    ('sejmiki2024', 'sejmik', 1, date '2024-04-07'),
    ('prezydent2025_r1', 'prezydent', 1, date '2025-05-18'),
    ('prezydent2025_r2', 'prezydent', 2, date '2025-06-01')
),

cells as (
    select *
    from ({{ xlsx_newest_cells(source('pkw', 'pkw_cells')) }})
),

files as (
    select distinct
        entity,
        source_version,
        raw_sha256,
        regexp_extract(entity, '^([a-z]+[0-9]{4}(_r[12])?)_', 1) as election_key,
        case regexp_extract(entity, '_(kraj|wojewodztwa|powiaty|gminy)(_proc)?$', 1)
            when 'kraj' then 'kraj'
            when 'wojewodztwa' then 'wojewodztwo'
            when 'powiaty' then 'powiat'
            when 'gminy' then 'gmina'
        end as area_level,
        case when entity like '%\_proc' escape '\' then 'pct' else 'count' end as file_kind
    from cells
),

header as (
    select
        entity,
        column_number,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as column_label
    from cells
    where row_number = 1
),

body as (
    select
        c.entity,
        c.row_number as source_row,
        c.column_number,
        h.column_label,
        trim(c.value_text) as value_text,
        h.column_label in (
            'TERYT Gminy', 'TERYT Powiatu', 'Kod TERYT', 'Gmina', 'Powiat', 'Województwo', 'Nr okręgu',
            'Rodzaj jednostki', 'Komitet Wyborczy'
        ) as is_key
    from cells as c
    inner join header as h on c.entity = h.entity and c.column_number = h.column_number
    where c.row_number > 1
),

row_keys as (
    select
        entity,
        source_row,
        max(case when column_label in ('TERYT Gminy', 'TERYT Powiatu', 'Kod TERYT') then value_text end) as teryt_text,
        max(case when column_label = 'Gmina' then value_text end) as gmina_name,
        max(case when column_label = 'Powiat' then value_text end) as powiat_name,
        max(case when column_label = 'Województwo' then value_text end) as wojewodztwo_name,
        max(case when column_label = 'Nr okręgu' then value_text end) as district_number,
        max(case when column_label = 'Rodzaj jednostki' then value_text end) as unit_type,
        max(case when column_label = 'Komitet Wyborczy' then value_text end) as row_committee
    from body
    where is_key
    group by entity, source_row
),

rows_located as (
    select
        k.*,
        f.area_level,
        case
            when k.teryt_text is null then null
            when f.area_level = 'gmina' then lpad(k.teryt_text, 6, '0')
            when f.area_level = 'powiat' then substr(lpad(k.teryt_text, 6, '0'), 1, 4)
            when f.area_level = 'wojewodztwo' then substr(lpad(k.teryt_text, 6, '0'), 1, 2)
        end as area_code,
        case f.area_level
            when 'gmina' then coalesce(k.gmina_name, k.powiat_name)
            when 'powiat' then k.powiat_name
            when 'wojewodztwo' then k.wojewodztwo_name
            when 'kraj' then 'Polska'
        end as area_name,
        case
            when f.area_level = 'kraj' then 'country'
            when lower(k.powiat_name) = 'zagranica' then 'abroad'
            when lower(k.powiat_name) = 'statki' then 'ships'
            when k.teryt_text is not null and length(k.teryt_text) in (5, 6) then 'teryt'
            when k.teryt_text is not null then 'bad_code'
            when f.area_level = 'wojewodztwo' then 'voivodeship_name'
            when f.area_level = 'gmina' and k.gmina_name is null then 'city_powiat_name'
            else 'no_code'
        end as area_kind
    from row_keys as k
    inner join files as f on k.entity = f.entity
),

values_classified as (
    select
        b.entity,
        b.source_row,
        b.column_number,
        b.column_label,
        b.value_text,
        try_cast(replace(b.value_text, ',', '.') as double) as value,
        f.election_key,
        f.area_level,
        f.file_kind,
        f.source_version,
        f.raw_sha256,
        r.area_code,
        r.area_name,
        r.area_kind,
        r.wojewodztwo_name,
        r.powiat_name,
        r.district_number,
        r.unit_type,
        case
            when f.area_level = 'kraj' and b.column_label in ('Miasto', 'Wieś', 'Ogółem') then 'list_votes'
            when f.area_level = 'kraj' and b.column_label like '% \% głosów' escape '\' then 'list_share'
            when b.column_label = 'Frekwencja' then 'turnout'
            when b.column_label = 'Liczba wyborców uprawnionych do głosowania' then 'eligible_voters'
            when b.column_label like 'Liczba wyborców, którym wydano karty do głosowania w lokalu wyborczym oraz %'
                then 'ballots_issued'
            when b.column_label like 'Liczba głosów ważnych oddanych łącznie na wszystk%' then 'valid_votes'
            when
                b.column_label in (
                    '% głosów ważnych', 'Procent głosów ważnych oddanych łącznie na wszystkie listy kandydatów'
                )
                then 'valid_votes_pct'
            when b.column_label = 'Liczba głosów nieważnych' then 'invalid_votes'
            when b.column_label in ('% głosów nieważnych', 'Procent głosów nieważnych') then 'invalid_votes_pct'
            when regexp_matches(b.column_label, '^(KOALICYJNY )?KOMITET WYBORCZY')
                then case when f.file_kind = 'pct' then 'list_share' else 'list_votes' end
            when regexp_matches(b.column_label, '^(Liczba|Komisja|Nie wykorzystano|W tym)') then 'protocol'
            when f.election_key like 'prezydent%' and regexp_matches(b.column_label, '^\p{Lu}[\p{Lu}-]+ \p{Lu}\p{Ll}')
                then 'candidate_votes'
            else 'unknown'
        end as measure_code,
        case
            when f.area_level = 'kraj' then r.row_committee
            when regexp_matches(b.column_label, '^(KOALICYJNY )?KOMITET WYBORCZY') then b.column_label
            when f.election_key like 'prezydent%' and regexp_matches(b.column_label, '^\p{Lu}[\p{Lu}-]+ \p{Lu}\p{Ll}')
                then b.column_label
        end as breakdown_code,
        case
            when f.area_level <> 'kraj' then null
            when b.column_label like 'Miasto%' then 'miasto'
            when b.column_label like 'Wieś%' then 'wies'
            when b.column_label like 'Ogółem%' then 'ogolem'
        end as settlement_code
    from body as b
    inner join files as f on b.entity = f.entity
    inner join rows_located as r on b.entity = r.entity and b.source_row = r.source_row
    where not b.is_key
),

results as (
    select
        v.election_key,
        e.election_type,
        e.election_round,
        e.election_date,
        v.area_level,
        v.file_kind,
        v.area_code,
        v.area_name,
        v.area_kind,
        coalesce(
            v.area_code, v.area_kind || ':' || coalesce(v.wojewodztwo_name, '') || ':' || coalesce(v.area_name, '')
        ) as area_key,
        v.wojewodztwo_name,
        v.powiat_name,
        v.district_number,
        v.unit_type,
        v.column_label,
        v.measure_code,
        v.breakdown_code,
        v.settlement_code,
        case
            when v.measure_code in ('turnout', 'valid_votes_pct', 'invalid_votes_pct', 'list_share') then 'pct'
            when v.measure_code = 'protocol' and v.file_kind = 'pct' then 'pct'
            when v.measure_code in ('eligible_voters', 'ballots_issued') then 'persons'
            when v.measure_code in ('valid_votes', 'invalid_votes', 'list_votes', 'candidate_votes') then 'votes'
            else 'number'
        end as unit_code,
        v.value,
        v.value_text,
        v.entity,
        v.source_row,
        v.column_number,
        v.source_version,
        v.raw_sha256
    from values_classified as v
    left join elections as e on v.election_key = e.election_key
)

select
    *,
    count(*) over (
        partition by election_key, area_level, file_kind, area_key, column_label, breakdown_code, settlement_code
    ) > 1 as is_ambiguous
from results
