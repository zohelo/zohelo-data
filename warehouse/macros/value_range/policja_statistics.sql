{# Police statistics portal (#2969): offences; Poland is the largest area, highest 1,466,643 (2003, all offences; Dev
   trial 43806377026); % detected as published, 15.5 to 83.0 there, bound 0-100. Documentation only (no metrics models
   yet): the bounds wait for their models. #}
{% macro poc_value_range__policja_statistics() %}
    {{ return({
        'bounds': [
            ['recorded_offences', '%', '0', '5e6'],
            ['offence_detection_rate', '%', '0', '100'],
        ],
        'models': [],
    }) }}
{% endmacro %}
