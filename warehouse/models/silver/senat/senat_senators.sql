{# Senate of the Republic of Poland (register PLG-025, route D36): senators of term XI as the senators list shows
   them. Grain: senator_id (the Senate's id in senator,<id>,11,<slug>.html). Name as printed and the date the
   mandate ended when the list prints one ('mandat wygasł 17.11.2024 r.' and similar annotations); the reason is not
   kept, nor photos or biographies (public office holders: name and role only). Source: bronze.senat_senators. #}
select
    cast(senator_id as integer) as senator_id,
    cast(term as integer) as term,
    name,
    slug,
    annotation is null as in_office,
    try_cast(try_strptime(regexp_extract(annotation, '(\d{2}\.\d{2}\.\d{4})', 1), '%d.%m.%Y') as date)
        as mandate_ended_on,
    source_version,
    raw_sha256
from {{ source('senat', 'senat_senators') }}
