{# The newest Bronze version (raw_sha256) of every Eurostat dataset, by source version then
   ingestion time. Reads only three columns of Bronze. #}
select dataset_id,
       arg_max(raw_sha256, source_version || '|' || cast(ingested_at as varchar)) as raw_sha256,
       max(source_version) as source_version
from (select distinct dataset_id, raw_sha256, source_version, ingested_at
      from {{ source('eurostat', 'eurostat_full_observations') }})
group by dataset_id
