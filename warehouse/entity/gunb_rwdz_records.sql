-- GUNB RWDZ (register PL-ENV-033): building-permit applications with their decisions, one voivodeship file.
-- Grain: one row per register record (numer_gunb, `record_id`) of the file; source: the producer's file
-- wynik_<voivodeship>.zip of https://wyszukiwarka.gunb.gov.pl/pobranie.html, read into Bronze without the columns
-- that name persons, addresses or plots (ingestion/to_bronze/gunb_rwdz/entities.py).
-- The file repeats a record on one line per plot (up to 2,009 lines, probe 43798083518), and some lines exactly;
-- the plot columns are not in Bronze, so the lines of one record normally agree in every column read here. Where
-- they do not (a few records per file: dates, numbers, TERC or category differ between lines), the column is null
-- for that record and named in `conflicting_columns`: no line is picked. `source_lines` counts the lines.
-- Applied by the loader to ONE Bronze file; `bronze` is that file (design #875).
with voivodeships (entity, voivodeship_code, voivodeship_name_expected) as (
    values
    ('dolnoslaskie', '02', 'dolnośląskie'),
    ('kujawsko_pomorskie', '04', 'kujawsko-pomorskie'),
    ('lubelskie', '06', 'lubelskie'),
    ('lubuskie', '08', 'lubuskie'),
    ('lodzkie', '10', 'łódzkie'),
    ('malopolskie', '12', 'małopolskie'),
    ('mazowieckie', '14', 'mazowieckie'),
    ('opolskie', '16', 'opolskie'),
    ('podkarpackie', '18', 'podkarpackie'),
    ('podlaskie', '20', 'podlaskie'),
    ('pomorskie', '22', 'pomorskie'),
    ('slaskie', '24', 'śląskie'),
    ('swietokrzyskie', '26', 'świętokrzyskie'),
    ('warminsko_mazurskie', '28', 'warmińsko-mazurskie'),
    ('wielkopolskie', '30', 'wielkopolskie'),
    ('zachodniopomorskie', '32', 'zachodniopomorskie')
),

grouped as (
    select
        entity,
        numer_gunb as record_id,
        count(*) as source_lines,
        count(distinct coalesce(numer_urzad, chr(0))) as n_office,
        min(numer_urzad) as office_case_number,
        count(distinct coalesce(nazwa_organu, chr(0))) as n_authority,
        min(nazwa_organu) as authority_name,
        count(distinct coalesce(data_wplywu_wniosku, chr(0))) as n_application,
        min(data_wplywu_wniosku) as application_text,
        count(distinct coalesce(numer_decyzji_urzedu, chr(0))) as n_decision_number,
        min(numer_decyzji_urzedu) as decision_number,
        count(distinct coalesce(data_wydania_decyzji, chr(0))) as n_decision,
        min(data_wydania_decyzji) as decision_text,
        count(distinct coalesce(wojewodztwo, chr(0))) as n_voivodeship,
        min(wojewodztwo) as voivodeship_name,
        count(distinct coalesce(terc, chr(0))) as n_terc,
        min(terc) as terc_code,
        count(distinct coalesce(rodzaj_inwestycji, chr(0))) as n_kind,
        min(rodzaj_inwestycji) as investment_kind,
        count(distinct coalesce(kategoria, chr(0))) as n_category,
        min(kategoria) as object_category,
        count(distinct coalesce(kubatura, chr(0))) as n_cubature,
        min(kubatura) as cubature,
        min(source_version) as source_version,
        min(raw_sha256) as raw_sha256
    from bronze
    group by entity, numer_gunb
)

select
    g.entity,
    g.record_id,
    case when g.n_office = 1 then g.office_case_number end as office_case_number,
    case when g.n_authority = 1 then g.authority_name end as authority_name,
    case
        when g.n_application = 1 then cast(try_strptime(g.application_text, '%Y-%m-%d %H:%M:%S') as date)
    end as application_date,
    case when g.n_decision_number = 1 then g.decision_number end as decision_number,
    case when g.n_decision = 1 then cast(try_strptime(g.decision_text, '%Y-%m-%d %H:%M:%S') as date) end
        as decision_date,
    v.voivodeship_code,
    case when g.n_voivodeship = 1 then g.voivodeship_name end as voivodeship_name,
    case when g.n_terc = 1 then g.terc_code end as terc_code,
    case when g.n_kind = 1 then g.investment_kind end as investment_kind,
    case when g.n_category = 1 then g.object_category end as object_category,
    case when g.n_cubature = 1 then g.cubature end as cubature_text,
    g.source_lines,
    nullif(concat_ws(
        ',',
        case when g.n_office > 1 then 'office_case_number' end,
        case when g.n_authority > 1 then 'authority_name' end,
        case when g.n_application > 1 then 'application_date' end,
        case when g.n_decision_number > 1 then 'decision_number' end,
        case when g.n_decision > 1 then 'decision_date' end,
        case when g.n_voivodeship > 1 then 'voivodeship_name' end,
        case when g.n_terc > 1 then 'terc_code' end,
        case when g.n_kind > 1 then 'investment_kind' end,
        case when g.n_category > 1 then 'object_category' end,
        case when g.n_cubature > 1 then 'cubature_text' end
    ), '') as conflicting_columns,
    -- 1 when a published value breaks the file's contract (the loader rejects the file, reject_if)
    case
        when v.voivodeship_code is null then 'unknown entity'
        when g.n_voivodeship = 1 and g.voivodeship_name is distinct from v.voivodeship_name_expected
            then 'voivodeship differs from the file'
        when g.n_application = 1 and g.application_text is not null
            and try_strptime(g.application_text, '%Y-%m-%d %H:%M:%S') is null then 'application date unreadable'
        when g.n_decision = 1 and g.decision_text is not null
            and try_strptime(g.decision_text, '%Y-%m-%d %H:%M:%S') is null then 'decision date unreadable'
        when g.n_decision = 1 and g.decision_text is null then 'decision date missing'
        when g.n_terc = 1 and g.terc_code is not null and not regexp_full_match(g.terc_code, '[0-9]{7}')
            then 'TERC code not 7 digits'
        when g.n_kind = 1 and g.investment_kind not in (
            'Budynek mieszkalny jednorodzinny', 'Obiekt budowlany inny niż budynek mieszkalny jednorodzinny'
        ) then 'unknown investment kind'
        when g.n_category = 1 and g.object_category not in (
            'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI',
            'XVII', 'XVIII', 'XIX', 'XX', 'XXI', 'XXII', 'XXIII', 'XXIV', 'XXV', 'XXVI', 'XXVII', 'XXVIII', 'XXIX',
            'XXX'
        ) then 'unknown object category'
    end as contract_problem,
    g.source_version,
    g.raw_sha256
from grouped as g
left join voivodeships as v on g.entity = v.entity
