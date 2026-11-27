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

{# DBW blocks: indicator × section × presentation × periodicity; the files (one per year) share the prefix. #}
{% macro poc_dbw_blocks() %}
    select * from (
        values
        ('581_57_93_4', 'Ludność — makroregiony, regiony, podregiony; charakter miejscowości; płeć; grupy wieku', 'A'),
        ('581_16_2_1', 'Ludność — Polska, dane miesięczne', 'M'),
        -- zohelo-bi first slice, every source per KPI (O31, #2592); file stem without year
        ('305_739_5_1', 'Wskaźniki cen towarów i usług konsumpcyjnych — analogiczny okres = 100, do 2025', 'M'),
        ('305_1722_5_1', 'Wskaźniki cen towarów i usług konsumpcyjnych — analogiczny okres = 100, od 2026', 'M'),
        ('305_1721_5_3', 'Wskaźniki cen towarów i usług konsumpcyjnych — województwa, kwartalne, od 2026', 'Q'),
        ('376_2_243_1', 'Przeciętne miesięczne wynagrodzenie brutto w sektorze przedsiębiorstw [zł]', 'M'),
        ('376_2_184_1', 'Wynagrodzenie w sektorze przedsiębiorstw — analogiczny okres = 100, nominalnie', 'M'),
        ('376_16_182_1', 'Wynagrodzenie w sektorze przedsiębiorstw — analogiczny okres = 100, realnie', 'M'),
        ('398_16_243_3', 'Przeciętne miesięczne wynagrodzenie brutto w gospodarce narodowej [zł]', 'Q'),
        ('398_16_184_3', 'Wynagrodzenie w gospodarce narodowej — analogiczny okres = 100, nominalnie', 'Q'),
        ('398_16_182_3', 'Wynagrodzenie w gospodarce narodowej — analogiczny okres = 100, realnie', 'Q'),
        ('875_143_95_1', 'Stopa bezrobocia rejestrowanego — Polska, województwa, powiaty', 'M'),
        ('1514_1360_95_3', 'Stopa bezrobocia - BAEL — Polska, makroregiony, województwa, regiony', 'Q'),
        ('1513_1360_95_3', 'Wskaźnik zatrudnienia - BAEL — Polska, makroregiony, województwa, regiony', 'Q'),
        ('1512_1360_95_3', 'Współczynnik aktywności zawodowej - BAEL — Polska, makroregiony, województwa, regiony', 'Q'),
        ('1038_16_389_3', 'PKB — analogiczny okres = 100, ceny stałe 2020', 'Q'),
        ('1038_16_390_3', 'PKB — okres poprzedni = 100, wyrównany sezonowo, ceny stałe 2020', 'Q'),
        ('1038_16_92_4', 'PKB — rok poprzedni = 100, ceny stałe', 'A'),
        ('1038_16_105_4', 'PKB — ceny bieżące [mln zł]', 'A'),
        ('1173_793_92_4', 'PKB w ujęciu regionalnym — rok poprzedni = 100, ceny stałe', 'A'),
        ('814_2_7_1', 'Produkcja sprzedana przemysłu — analogiczny okres = 100, ceny stałe', 'M'),
        ('392_16_223_1', 'Produkcja budowlano-montażowa — analogiczny okres = 100, wyrównana sezonowo', 'M'),
        ('109_849_7_1', 'Sprzedaż detaliczna towarów — analogiczny okres = 100, ceny stałe', 'M'),
        ('469_16_117_1', 'Bieżący wskaźnik ufności konsumenckiej (BWUK)', 'M'),
        -- slice needs-block rows (O36, #2743)
        ('1603_1339_116_4', 'Odsetek osób zagrożonych ubóstwem lub wykluczeniem społecznym — makroregiony, regiony', 'A'),
        ('1618_1335_95_4', 'Stopa bezrobocia długotrwałego - BAEL', 'A'),
        ('1714_1575_243_1', 'Mediana wynagrodzeń miesięcznych brutto (Rozkład wynagrodzeń) [zł]', 'M'),
        ('309_485_180_3', 'Średnie ceny za 1 m2 lokali mieszkalnych sprzedanych w ramach transakcji rynkowych [zł]', 'Q'),
        ('507_870_93_1', 'Bezrobotni zarejestrowani — płeć, wiek', 'M')
    ) as t (block_id, block_name, freq_code)
{% endmacro %}

{# DBW block rules. regional: only Poland and voivodeships are read (poc_src_dbw.py REGIONAL, territory in
   position 1). total_labels ('|'-separated, lower case): positions that count as the total where the block has
   no "ogółem" (the age band and duration of the LFS long-term rate, the residence basis of the median wage).
   measure_dimension: a dimension whose position names the measure (the market of the dwelling prices). #}
{% macro poc_dbw_block_rules() %}
    select * from (
        values
        ('1618_1335_95_4', false, 'od 15 do 74 lat|12 miesięcy i więcej', ''),
        ('1714_1575_243_1', true, 'miejsce zamieszkania', ''),
        ('309_485_180_3', true, '', 'rodzaj rynku nieruchomości'),
        ('507_870_93_1', true, '', '')
    ) as t (block_id, regional, total_labels, measure_dimension)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__gus_dbw() %}
    {{ return({
        'block_model': 'poc_build_dbw',
        'activated': "select 'gus_dbw', block_id, block_name from (" ~ poc_dbw_blocks() ~ ")",
    }) }}
{% endmacro %}
