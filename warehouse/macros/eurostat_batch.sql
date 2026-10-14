{# Batch of the incremental Eurostat Silver build, planned by warehouse/eurostat_batch.py
   and passed as vars: lists of [dataset_id, raw_sha256] pairs. A pair, not the hash alone,
   identifies a dataset version (Eurostat publishes some identical files under two codes).
   The predicates repeat dataset_id and raw_sha256 as literal lists so scans can prune
   partitions and files. #}
{% macro sql_list(values) -%}
  {%- for v in values -%}'{{ v | replace("'", "''") }}'{{ ", " if not loop.last }}{%- endfor -%}
{%- endmacro %}

{% macro eurostat_pairs_filter(pairs) -%}
  {%- if pairs -%}
    (dataset_id in ({{ sql_list(pairs | map('first') | unique | list) }})
     and raw_sha256 in ({{ sql_list(pairs | map('last') | unique | list) }})
     and dataset_id || '|' || raw_sha256 in ({% for p in pairs %}'{{ (p[0] ~ '|' ~ p[1]) | replace("'", "''") }}'{{ ", " if not loop.last }}{% endfor %}))
  {%- else -%}
    false
  {%- endif -%}
{%- endmacro %}

{% macro eurostat_batch_filter() -%}
  {{ eurostat_pairs_filter(var('eurostat_batch', [])) }}
{%- endmacro %}

{# Rows to remove before the insert: superseded versions, and the batch itself so that a
   rerun after a failed bookkeeping step never duplicates rows. #}
{% macro eurostat_clear() -%}
  {%- set pairs = var('eurostat_superseded', []) + var('eurostat_batch', []) -%}
  {%- if pairs -%}
    delete from {{ this }} where {{ eurostat_pairs_filter(pairs) }}
  {%- else -%}
    select 1
  {%- endif -%}
{%- endmacro %}
