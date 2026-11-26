{# MRiRW ZSRIR (#2484), monthly purchase prices as published, Poland: raw milk zł/100 kg 91.28 (2004-04) to 277.93
   (2004-2026); pigs zł/kg live weight 4.25 to 9.16 (2022-2026); consumption wheat zł/t 458.37 to 1,717.14 (2006-2026)
   (Dev trial 45705794335). #}
{% macro poc_value_range__mrirw_zsrir() %}
    {{ return({
        'bounds': [
            ['milk_purchase_price', '%', '0', '1000'],
            ['pig_purchase_price', '%', '0', '30'],
            ['wheat_purchase_price', '%', '0', '5000'],
        ],
        'models': [
            'poc_metrics_milk_purchase_price',
            'poc_metrics_pig_purchase_price',
            'poc_metrics_wheat_purchase_price',
        ],
    }) }}
{% endmacro %}
