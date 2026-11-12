{# Per-source registration of Gold (decision D34, docs/source-registration.md): a source declares its Gold
   block and its geography codes in its own file warehouse/macros/sources/<source_system>.sql, and the macros
   below find those declarations by name, so adding a source adds no line to a shared macro or model.

   A source file may define
     poc_source__<source_system>()     returns {'block_model': 'poc_build_<x>'} plus either
                                       'activated': one select of source_system, block_id, block_name, or
                                       'blocks': [[block_id, block_name], ...]
     poc_geography__<source_system>()  the source's CTEs for poc_map_geography: one named <source_system>
                                       (source_system, source_code, geography_key), helpers <source_system>_<x>.
   Sources registered before D34 stay in the lists of macros/poc_blocks.sql and models/gold/poc_map_geography.sql.
   Registered sources follow them in name order, so the compiled SQL does not depend on merge order.
   sqlfluff lints the models with plain Jinja, where `context` is not defined: there the four macros the
   models call render the lists of poc_blocks.sql only. #}

{# The names (the part after `prefix`) of the project's macros that start with `prefix`, sorted. dbt gives a
   macro every macro of the project by name in `context`; if a dbt version stops doing that, this macro does
   not find itself and fails the parse instead of silently dropping every registered source. #}
{% macro poc_registered(prefix) %}
    {%- set found = namespace(names=[], self=false) -%}
    {%- for name in context -%}
        {%- if name == 'poc_registered' -%}
            {%- set found.self = true -%}
        {%- elif name is string and name.startswith(prefix) -%}
            {%- do found.names.append(name[prefix | length:]) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if not found.self -%}
        {%- do exceptions.raise_compiler_error(
            'poc_registered: this dbt version does not list macros in `context`; per-source registration '
            ~ '(docs/source-registration.md) needs another discovery'
        ) -%}
    {%- endif -%}
    {{- return(found.names | sort) -}}
{% endmacro %}

{# source_system → its registration (poc_source__<source_system>()), in name order. #}
{% macro poc_registrations() %}
    {%- set out = {} -%}
    {%- for source in poc_registered('poc_source__') -%}
        {%- do out.update({source: context['poc_source__' ~ source]()}) -%}
    {%- endfor -%}
    {{- return(out) -}}
{% endmacro %}

{# The Gold blocks (build stage models) as `model:source_system`, one per line: the list of
   poc_legacy_block_models() and then every registered source's block. A block model carries the tag
   `gold_block` (ops/repo_check.py checks the tags, the list and the registrations agree): its children do not
   wait for it in the changed-only build (warehouse/changed.py), they read it only once its table exists. #}
{% macro poc_block_models() %}
    {%- set legacy = poc_legacy_block_models() -%}
    {{- legacy -}}
    {%- set listed = [] -%}
    {%- for entry in legacy.split() -%}{%- do listed.append(entry.split(':')[0]) -%}{%- endfor -%}
    {%- for source, registration in (poc_registrations().items() if context is defined else []) -%}
        {%- if registration.block_model in listed -%}
            {%- do exceptions.raise_compiler_error(
                'Gold block ' ~ registration.block_model ~ ' is declared twice (poc_source__' ~ source
                ~ ' and another registration or poc_legacy_block_models)'
            ) -%}
        {%- endif -%}
        {%- do listed.append(registration.block_model) %}
    {{ registration.block_model }}:{{ source }}
    {%- endfor -%}
{% endmacro %}

{# Every activated block of every source: the list build_check reports against, so a block that
   yields no rows (not in Silver yet, or filtered out) is visible. The list of
   poc_legacy_activated_blocks(), then each registered source's blocks. #}
{% macro poc_activated_blocks() %}
    {{- poc_legacy_activated_blocks() -}}
    {%- for source, registration in (poc_registrations().items() if context is defined else []) -%}
        {%- if registration.activated %}
    union all
    {{ registration.activated | trim }}
        {%- elif registration.blocks %}
    union all
    select * from (
        values
            {%- for block in registration.blocks %}
        ('{{ source }}', '{{ block[0] | replace("'", "''") }}', '{{ block[1] | replace("'", "''") }}')
                {{- ',' if not loop.last }}
            {%- endfor %}
    ) as t (source_system, block_id, block_name)
        {%- else -%}
            {%- do exceptions.raise_compiler_error(
                'poc_source__' ~ source ~ ': a registration needs `activated` (a select) or `blocks` (a list)'
            ) -%}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}

{# poc_map_geography: the CTEs of every registered source (poc_geography__<source_system>()), each followed
   by a comma; nothing when no source is registered. #}
{% macro poc_geography_ctes() %}
    {%- for source in (poc_registered('poc_geography__') if context is defined else []) -%}
        {{- context['poc_geography__' ~ source]() | trim }},

{% endfor -%}
{% endmacro %}

{# poc_map_geography: `select * from <source_system> union all` for every registered source, before the
   branches written in the model; nothing when no source is registered. #}
{% macro poc_geography_union() %}
    {%- for source in (poc_registered('poc_geography__') if context is defined else []) -%}
        select * from {{ source }}
    union all
    {% endfor -%}
{% endmacro %}
