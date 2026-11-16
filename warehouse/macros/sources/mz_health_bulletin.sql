{# mz_health_bulletin (register PLS-017, #2957): Gold registration, decision D34 (docs/source-registration.md). Block
   'health_bulletin' (models/gold/poc_build_mz_health_bulletin.sql); areas by name as printed (lower case), matched to
   the TERYT voivodeship names of dim_geography, 'polska' = Poland. A static list: the map reads no Silver table of
   this source, so it never waits for the source's first load (no poc_optional_ref needed). #}
{% macro poc_source__mz_health_bulletin() %}
    {{ return({'block_model': 'poc_build_mz_health_bulletin', 'blocks': [['health_bulletin', 'MZ statistical bulletin: health workforce and general hospitals (annual)']]}) }}
{% endmacro %}

{% macro poc_geography__mz_health_bulletin() %}
mz_health_bulletin as (
    select
        'mz_health_bulletin' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'mz_health_bulletin' as source_system,
        'polska' as source_code,
        'PL' as geography_key
)
{% endmacro %}

{# The modelled columns of the four bulletin tables, shared by models/silver/mz_health_bulletin/: (table_kind,
   header_regex, profession_code, measure_code, unit_code). A header is the header-row text with '_' read as a space,
   a trailing footnote number dropped, lower case; it must match one regex in full. #}
{% macro poc_mz_health_bulletin_specs() %}
    {% set professions = [
        ('lekarze', 'doctors'), ('lekarze dentyści', 'dentists'), ('farmaceuci', 'pharmacists'),
        ('diagności laboratoryjni', 'lab_diagnosticians'), ('pielęgniarki', 'nurses'), ('położne', 'midwives'),
        ('fizjoterapeuci', 'physiotherapists'), ('ratownicy medyczni', 'paramedics'), ('dietetycy', 'dietitians'),
    ] %}
    {% set specs = [] %}
    {% for kind in ['licensed', 'employed'] %}{% for p in professions %}
        {% do specs.append((kind, '^' ~ p[0] ~ ' liczba bezw[a-ząćęłńóśźż]*$', p[1], 'count', 'persons')) %}
        {% do specs.append((kind, '^' ~ p[0] ~ ' wskaźnik$', p[1], 'per_10k', 'per_10k_population')) %}
    {% endfor %}{% endfor %}
    {% do specs.extend([
        ('hospitals', '^szpitale ogółem$', 'hospitals', 'count', 'hospitals'),
        ('hospitals', '^łóżka ogółem$', 'beds', 'count', 'beds'),
        ('hospitals', '^łóżka wskaźnik$', 'beds', 'per_10k', 'per_10k_population'),
        ('hospital_activity', '^leczeni w ciągu roku ogółem$', 'inpatients', 'count', 'cases'),
        ('hospital_activity', '^leczeni na 1 łóżko wskaźn[a-z]*$', 'inpatients', 'per_bed', 'cases_per_bed'),
        ('hospital_activity', '^osobodni w tysiącach$', 'patient_days', 'count', 'thousand_patient_days'),
        ('hospital_activity', '^przeciętny pobyt chorego wskaźnik w dniach$', 'length_of_stay', 'mean', 'days'),
        ('hospital_activity', '^wykorzystanie łóżka wskaźnik w dniach$', 'bed_occupancy', 'days', 'days'),
        ('hospital_activity', '^wykorzystanie łóżka wskaźnik w %$', 'bed_occupancy', 'pct', 'pct'),
    ]) %}
    select * from (
        values
        {%- for s in specs %}
            ('{{ s[0] }}', '{{ s[1] }}', '{{ s[2] }}', '{{ s[3] }}', '{{ s[4] }}'){{ "," if not loop.last }}
        {%- endfor %}
    ) as t (table_kind, header_regex, profession_code, measure_code, unit_code)
{% endmacro %}
