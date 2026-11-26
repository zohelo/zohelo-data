{# Sejm API (register PLG-024): prints (druki) of terms 9 and 10.
   Grain: term x print_number (the API's number as text, e.g. '3355'). Source: bronze.sejm_prints, with the number of
   prints /sejm/term lists for the term (prints_listed, for the completeness test). The API gives no applicant field:
   who submitted a print is only in its title, which is kept as published and not parsed. #}
with prints as (
    select
        cast(term as integer) as term,
        cast(item_term as integer) as item_term,
        print_number,
        title,
        cast(document_date as date) as document_date,
        cast(delivery_date as date) as delivery_date,
        cast(change_date as timestamp) as changed_at,
        process_prints,
        cast(additional_prints as integer) as additional_prints,
        source_version,
        raw_sha256
    from {{ source('sejm', 'sejm_prints') }}
),

terms as (
    select
        cast(term_num as integer) as term,
        cast(prints_count as integer) as prints_listed
    from {{ source('sejm', 'sejm_terms') }}
)

select
    p.term,
    p.item_term,
    p.print_number,
    p.title,
    p.document_date,
    p.delivery_date,
    p.changed_at,
    p.process_prints,
    p.additional_prints,
    t.prints_listed,
    p.source_version,
    p.raw_sha256
from prints as p
left join terms as t on p.term = t.term
