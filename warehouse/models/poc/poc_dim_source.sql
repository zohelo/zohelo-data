{{ config(alias='dim_source') }}
{# Gold PoC: the platform's sources, one row each, from the ingestion registry (ingestion/sources/*.yaml). #}
select * from (
    values
    ('eurostat', 'Eurostat', 'Eurostat', 'European Commission', 'cube', 'dataset'),
    ('gus_bdl', 'GUS Bank Danych Lokalnych', 'GUS Local Data Bank', 'GUS', 'cube', 'subgroup'),
    (
        'gus_dbw',
        'GUS Dziedzinowe Bazy Wiedzy',
        'GUS Knowledge Databases',
        'GUS',
        'cube',
        'file (indicator × section × presentation × periodicity × year)'
    ),
    ('world_bank_wdi', 'World Bank WDI', 'World Bank World Development Indicators', 'World Bank', 'cube', 'indicator'),
    (
        'nbp',
        'NBP kursy walut i cena złota',
        'NBP exchange rates and gold price',
        'Narodowy Bank Polski',
        'cube',
        'table'
    ),
    ('gleif', 'GLEIF', 'GLEIF LEI golden copy', 'GLEIF', 'registry', 'file'),
    (
        'opendata_org',
        'OpenData ODO Senzing',
        'OpenData organisations, people, locations',
        'OpenData',
        'registry',
        'snapshot member'
    ),
    (
        'mf_biala_lista',
        'MF biała lista VAT',
        'MF VAT white list (flat file)',
        'Ministerstwo Finansów',
        'lookup',
        'file'
    ),
    ('gus_teryt', 'GUS TERYT', 'GUS TERYT register', 'GUS', 'reference', 'file'),
    ('gugik_prg', 'GUGiK PRG', 'GUGiK state register of boundaries', 'GUGiK', 'reference', 'file')
) as t (source_system, name_pl, name_en, publisher, family, block_kind)
