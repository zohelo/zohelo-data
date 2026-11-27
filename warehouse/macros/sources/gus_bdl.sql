{# GUS BDL geography codes (D34 registration, docs/source-registration.md): the API's unit ids
   (gus_bdl_api) and the web export's TERYT codes (gus_bdl_terc). #}
{% macro poc_geography__gus_bdl() %}
-- BDL: the web export keys rows by a 7-digit zero-padded TERYT code (0000000 Polska, WW00000
-- województwo, WWPP000 powiat, WWPPGGT gmina); the API by a 12-character unit id (bdl_units),
-- mapped here by level and name (v1: the GUS KTS–TERYT correspondence table).
gus_bdl_terc_woj as (
    select
        terc_code,
        upper(name_pl) as name_u
    from {{ ref('teryt_terc') }}
    where unit_level = 'wojewodztwo'
),

gus_bdl_terc_pow as (
    select
        terc_code,
        woj_code,
        lower(name_pl) as name_l,
        unit_type_pl
    from {{ ref('teryt_terc') }}
    where unit_level = 'powiat'
),

gus_bdl_terc_gmi as (
    select
        terc_code,
        woj_code,
        pow_code,
        lower(name_pl) as name_l,
        gmina_type_code
    from {{ ref('teryt_terc') }}
    where unit_level = 'gmina'
),

gus_bdl_units as (select * from {{ ref('bdl_units') }}),

gus_bdl_api as (
    select
        'gus_bdl' as source_system,
        u.unit_id as source_code,
        case
            when u.level = 0 then 'PL'
            when u.level = 1
                then
                    case
                        when upper(u.name) like '%MAZOWIECKIE%' then 'TERC:14'
                        when upper(u.name) like '%POŁUDNIOWO-ZACHODNI%' then 'NUTS:PL5'
                        when upper(u.name) like '%PÓŁNOCNO-ZACHODNI%' then 'NUTS:PL4'
                        when upper(u.name) like '%POŁUDNIOWY%' then 'NUTS:PL2'
                        when upper(u.name) like '%PÓŁNOCNY%' then 'NUTS:PL6'
                        when upper(u.name) like '%CENTRALNY%' then 'NUTS:PL7'
                        when upper(u.name) like '%WSCHODNI%' then 'NUTS:PL8'
                        else '_X'
                    end
            when u.level = 2 then coalesce('TERC:' || w.terc_code, '_X')
            when u.level = 3 and upper(u.name) like '%WARSZAWSKI STO%' then 'NUTS:PL91'
            when u.level = 3 and upper(u.name) like '%MAZOWIECKI REGIONALNY%' then 'NUTS:PL92'
            when u.level = 3 then coalesce('TERC:' || w3.terc_code, '_X')
            when u.level = 4 then 'BDL:' || u.unit_id
            when u.level = 5 then coalesce('TERC:' || p.terc_code, '_X')
            when u.level = 6 then coalesce('TERC:' || g.terc_code, '_X')
            else '_X'
        end as geography_key
    from gus_bdl_units as u
    left join gus_bdl_terc_woj as w on w.name_u = upper(u.name) and u.level = 2
    left join gus_bdl_terc_woj as w3 on w3.name_u = upper(regexp_replace(u.name, '^(?i)region ', '')) and u.level = 3
    left join gus_bdl_terc_pow as p
        on
            u.level = 5
            and p.woj_code = substr(u.unit_id, 3, 2)
            and p.name_l = lower(regexp_replace(u.name, '^(?i)powiat (m\.\s*st\.\s*|m\.\s*)?', ''))
    left join gus_bdl_terc_gmi as g
        on
            u.level = 6
            and g.woj_code = substr(u.unit_id, 3, 2)
            and g.name_l
            = lower(
                regexp_replace(u.name, '\s*\((miasto|obszar wiejski|gmina (miejska|wiejska|miejsko-wiejska))\)$', '')
            )
            and g.gmina_type_code = substr(u.unit_id, 12, 1)
),

gus_bdl_type_digits as (select unnest(['1', '2', '3']) as digit),

gus_bdl_terc as (  -- the 7-digit zero-padded codes of the web export, generated from TERYT
    select
        'gus_bdl' as source_system,
        rpad(terc_code, 7, '0') as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    union all
    select
        'gus_bdl' as source_system,
        '0000000' as source_code,
        'PL' as geography_key
    union all
    -- a gmina whose type digit changed since (2 → 3 when a village got town rights): the current unit
    select
        'gus_bdl' as source_system,
        substr(g.terc_code, 1, 6) || t.digit as source_code,
        'TERC:' || g.terc_code as geography_key
    from {{ ref('teryt_terc') }} as g
    cross join gus_bdl_type_digits as t
    where g.unit_level = 'gmina' and g.gmina_type_code in ('1', '2', '3') and t.digit <> g.gmina_type_code
),

gus_bdl as (
    select * from gus_bdl_api
    union all
    select * from gus_bdl_terc
)
{% endmacro %}

{% macro poc_bdl_blocks() %}
    select * from (
        values
        ('P2137', 'Ludność wg grup wieku i płci', 'LUDNOŚĆ / STAN LUDNOŚCI'),
        ('P1873', 'Ruch naturalny wg płci', 'LUDNOŚĆ / URODZENIA I ZGONY'),
        ('P2425', 'Gęstość zaludnienia oraz wskaźniki', 'LUDNOŚĆ / STAN LUDNOŚCI'),
        ('P2392', 'Stopa bezrobocia rejestrowanego', 'RYNEK PRACY / BEZROBOCIE REJESTROWANE'),
        ('P2497', 'Przeciętne miesięczne wynagrodzenia brutto', 'WYNAGRODZENIA I ŚWIADCZENIA SPOŁECZNE / WYNAGRODZENIA'),
        ('P3824', 'Mieszkania oddane do użytkowania', 'PRZEMYSŁ I BUDOWNICTWO / BUDOWNICTWO MIESZKANIOWE'),
        ('P1410', 'Powierzchnia', 'PODZIAŁ TERYTORIALNY / POWIERZCHNIA GEODEZYJNA KRAJU (DANE GUGIK)'),
        -- zohelo-bi first slice, every source per KPI (O31, #2592): Poland and voivodeships only
        ('P2955', 'Wskaźnik cen towarów i usług konsumpcyjnych (COICOP 1999, do 2025)', 'CENY / WSKAŹNIKI CEN'),
        ('P2496', 'Wskaźnik cen towarów i usług konsumpcyjnych (COICOP 1999, kwartalne, do 2025)', 'CENY / WSKAŹNIKI CEN'),
        ('P4635', 'Wskaźniki cen towarów i usług konsumpcyjnych (COICOP 2018, kwartalne, od 2026)', 'CENY / WSKAŹNIKI CEN'),
        ('P2359', 'Współczynnik aktywności zawodowej (BAEL, średnioroczne)', 'RYNEK PRACY / AKTYWNOŚĆ EKONOMICZNA (BAEL)'),
        ('P2360', 'Wskaźnik zatrudnienia (BAEL, średnioroczne)', 'RYNEK PRACY / AKTYWNOŚĆ EKONOMICZNA (BAEL)'),
        ('P2361', 'Stopa bezrobocia (BAEL, średnioroczne)', 'RYNEK PRACY / AKTYWNOŚĆ EKONOMICZNA (BAEL)'),
        ('P2961', 'Bezrobotni zarejestrowani wg płci (dane miesięczne)', 'RYNEK PRACY / BEZROBOCIE REJESTROWANE'),
        ('P4334', 'Zarejestrowani bezrobotni na 1 ofertę pracy', 'RYNEK PRACY / BEZROBOCIE REJESTROWANE'),
        ('P2730', 'Przeciętne dalsze trwanie życia', 'LUDNOŚĆ / TRWANIE ŻYCIA'),
        ('P2346', 'Współczynnik dzietności i reprodukcji brutto', 'LUDNOŚĆ / URODZENIA I ZGONY'),
        ('P2426', 'Wskaźniki obciążenia demograficznego', 'LUDNOŚĆ / STAN LUDNOŚCI'),
        ('P1869', 'Przeciętny miesięczny dochód rozporządzalny na 1 osobę', 'BUDŻETY GOSPODARSTW DOMOWYCH'),
        ('P2861', 'Emeryci i renciści', 'WYNAGRODZENIA I ŚWIADCZENIA SPOŁECZNE / EMERYTURY I RENTY'),
        ('P2860', 'Przeciętna miesięczna emerytura i renta brutto', 'WYNAGRODZENIA I ŚWIADCZENIA SPOŁECZNE / EMERYTURY I RENTY'),
        ('P2862', 'Nakłady inwestycyjne i wartość brutto środków trwałych', 'INWESTYCJE I ŚRODKI TRWAŁE'),
        ('P3502', 'Produkt krajowy brutto ogółem, dynamika', 'RACHUNKI REGIONALNE / PRODUKT KRAJOWY BRUTTO'),
        -- slice needs-block rows (O36, #2743): Poland and voivodeships only
        ('P2656', 'Osoby dorosłe (25-64) uczestniczące w kształceniu lub szkoleniu (BAEL, średnioroczne)', 'RYNEK PRACY / AKTYWNOŚĆ EKONOMICZNA (BAEL)'),
        ('P2713', 'Wskaźniki zasięgu ubóstwa - % osób w gosp. dom. o wydatkach poniżej granicy ubóstwa', 'BUDŻETY GOSPODARSTW DOMOWYCH / UBÓSTWO'),
        ('P2692', 'Produkcja sprzedana przemysłu wg PKD 2007 (dane kwartalne)', 'PRZEMYSŁ I BUDOWNICTWO / PRODUKCJA PRZEMYSŁOWA'),
        ('P2721', 'Produkcja sprzedana budownictwa wg PKD 2007 (dane kwartalne)', 'PRZEMYSŁ I BUDOWNICTWO / BUDOWNICTWO'),
        ('P2837', 'Pracujący w sektorze przedsiębiorstw (dane krótkookresowe)', 'RYNEK PRACY / PRACUJĄCY'),
        ('P4610', 'Mediana wynagrodzeń miesięcznych brutto (Rozkład wynagrodzeń w gospodarce narodowej)', 'WYNAGRODZENIA I ŚWIADCZENIA SPOŁECZNE / WYNAGRODZENIA'),
        ('P3788', 'Średnia cena za 1 m2 lokali mieszkalnych sprzedanych w ramach transakcji rynkowych', 'GOSPODARKA MIESZKANIOWA / CENY LOKALI MIESZKALNYCH'),
        ('P3794', 'Średnia cena za 1 m2 lokali mieszkalnych sprzedanych w ramach transakcji rynkowych (kwartalne)', 'GOSPODARKA MIESZKANIOWA / CENY LOKALI MIESZKALNYCH')
    ) as t (subgroup_id, block_name, subject_path)
{% endmacro %}

{# BDL label positions (after the period label is taken out) that name the measure although some variables
   say "ogółem" there: the market of the dwelling prices, so the secondary market is a measure of its own. #}
{% macro poc_bdl_measure_positions() %}
    select * from (
        values
        ('P3788', 1),
        ('P3794', 1)
    ) as t (subgroup_id, position)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__gus_bdl() %}
    {{ return({
        'block_model': 'poc_build_bdl',
        'activated': "select 'gus_bdl', subgroup_id, block_name from (" ~ poc_bdl_blocks() ~ ")",
    }) }}
{% endmacro %}
