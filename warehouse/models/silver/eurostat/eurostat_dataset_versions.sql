{# Newest Bronze file (Landing version) of every Eurostat dataset, from the file index
   that warehouse/eurostat_batch.py derives from Iceberg metadata before each build. #}
select dataset_id, raw_sha256, ingested_at, record_count, file_size_bytes
from {{ this.database }}.{{ this.schema }}.eurostat_file_index
where is_current
