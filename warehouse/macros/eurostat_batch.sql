{# Batch of the incremental Eurostat Silver build, planned by warehouse/eurostat_batch.py
   and passed as vars (lists of dataset ids and Landing SHA-256). #}
{% macro sql_list(values) -%}
  {%- for v in values -%}'{{ v | replace("'", "''") }}'{{ ", " if not loop.last }}{%- endfor -%}
{%- endmacro %}

{% macro eurostat_batch_filter(prefix='') -%}
  {%- set hashes = var('eurostat_batch_hashes', []) -%}
  {%- if hashes -%}
    {{ prefix }}dataset_id in ({{ sql_list(var('eurostat_batch_datasets', [])) }})
    and {{ prefix }}raw_sha256 in ({{ sql_list(hashes) }})
  {%- else -%}
    false
  {%- endif -%}
{%- endmacro %}

{# Rows to remove before the insert: superseded versions, and the batch itself so that a
   rerun after a failed bookkeeping step never duplicates rows. #}
{% macro eurostat_clear() -%}
  {%- set hashes = var('eurostat_superseded', []) + var('eurostat_batch_hashes', []) -%}
  {%- if hashes -%}
    delete from {{ this }} where raw_sha256 in ({{ sql_list(hashes) }})
  {%- else -%}
    select 1
  {%- endif -%}
{%- endmacro %}
