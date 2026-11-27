{{ config(alias='open_data_datasets_pl') }}
{# Gold entity table (register source, kpi false; docs/model.md "Fact kinds": catalogue records): one row per dataset
   of the dane.gov.pl open-data catalogue as harvested by data.europa.eu (register PLG-030, #3263), keyed by the
   dane.gov.pl dataset number; date_key = the day of its last modification (UTC). Title, publisher (dane.gov.pl
   institution number and name), periodicity, high-value flag, the number of themes and of distributions, and the
   harvest record's times (harvest_lag_hours = harvest record modified minus dataset modified). It tells builders which
   publishers have datasets on dane.gov.pl; the data of the datasets is not here. #}
select
    dataset_number,
    modified_on as date_key,
    dataset_id,
    identifier_url,
    landing_page,
    title_pl,
    publisher_number,
    publisher_name,
    publisher_type,
    periodicity_code,
    is_high_value,
    category_count,
    distribution_count,
    issued_at,
    modified_at,
    harvest_issued_at,
    harvest_modified_at,
    round(date_diff('second', modified_at, harvest_modified_at) / 3600.0, 1) as harvest_lag_hours,
    harvest_records,
    'dane_gov_pl' as source_system,
    source_version,
    raw_sha256
from {{ ref('dane_gov_pl_datasets') }}
