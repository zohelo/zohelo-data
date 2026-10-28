{{ config(alias='dim_observation_status') }}
{# Gold PoC: observation status flags, conformed over SDMX OBS_STATUS (Eurostat) and the GUS flags. #}
select * from (
    values
    ('ok', 'Normal value', 'wartość normalna', false),
    ('provisional', 'Provisional', 'dane wstępne', false),
    ('estimated', 'Estimated', 'szacunek', false),
    ('forecast', 'Forecast', 'prognoza', false),
    ('confidential', 'Confidential', 'dane poufne', true),
    ('break', 'Break in time series', 'przerwa w szeregu', false),
    ('low_reliability', 'Low reliability', 'niska wiarygodność', false),
    ('definition_differs', 'Definition differs', 'odmienna definicja', false),
    ('not_significant', 'Not significant', 'nieistotne', false),
    ('flagged', 'Flagged by the source', 'oznaczone przez źródło', false),
    ('conflicting_values', 'Several published values, one kept', 'kilka opublikowanych wartości', false)
) as t (observation_status_key, name_en, name_pl, suppressed)
