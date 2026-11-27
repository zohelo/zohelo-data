{# Per-block incremental Gold models (#3204 PR B, decision D39): the five facts and poc_dim_series. Each row carries
   its block model in `gold_block` (poc_observations). In the changed-only build warehouse/changed.py lists the
   blocks to replace (`changed_blocks`) and warehouse/build.py passes them as a dbt var; the model reads only those
   blocks (poc_observations(is_incremental())) and this strategy replaces them in its table. #}

{# The `full_refresh` config of a per-block incremental model: true when the plan lists the model under
   `full_refresh` (code changed, no build properties, table missing, too many position deletes, file growth) or
   when the build is not the changed-only plan (no var `changed_blocks`: an explicit filter or "fqn:*" rebuilds it
   in full, as a table was); else none, so dbt's own --full-refresh flag still applies. #}
{% macro poc_block_full_refresh(model_name) %}
    {%- if model_name in var('full_refresh_models', []) or var('changed_blocks', none) is none -%}
        {{- return(true) -}}
    {%- endif -%}
    {{- return(none) -}}
{% endmacro %}

{# incremental_strategy='gold_blocks': delete the rows of the changed blocks and of every block that is no longer
   registered (poc_block_models), then insert the new rows (the temp relation holds the changed blocks only).
   One explicit transaction: the profile disables dbt's transactions (Iceberg tables cannot be renamed inside the
   transaction that created them), and without one the delete and the insert are two catalog commits, the first
   leaving the blocks empty (Dev trial 45750296222). adapter.ensure_transaction() opens a transaction dbt tracks:
   the materialization's adapter.commit() commits both statements as one Iceberg commit, and a failure rolls back. #}
{% macro get_incremental_gold_blocks_sql(arg_dict) %}
    {%- set target = arg_dict['target_relation'] -%}
    {%- set columns = arg_dict['dest_columns'] | map(attribute='quoted') | join(', ') -%}
    {%- set changed = var('changed_blocks', []) -%}
    {%- set registered = [] -%}
    {%- for entry in poc_block_models().split() -%}{%- do registered.append(entry.split(':')[0]) -%}{%- endfor -%}
    {%- do adapter.ensure_transaction() -%}
    delete from {{ target }}
    where
        gold_block not in ({{ poc_quoted(registered) }})
        {%- if changed %}
        or gold_block in ({{ poc_quoted(changed) }})
        {%- endif %};
    insert into {{ target }} ({{ columns }})
    select {{ columns }} from {{ arg_dict['temp_relation'] }}
{% endmacro %}

{# 'a', 'b' from a list of names. #}
{% macro poc_quoted(names) %}
    {%- for name in names -%}'{{ name | replace("'", "''") }}'{{ ', ' if not loop.last }}{%- endfor -%}
{% endmacro %}
