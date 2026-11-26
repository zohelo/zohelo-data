{# e-Zamówienia BZP (#2981), Dev trial 45767597789 (complete months 2026-01 to 2026-10). bzp_notices: Poland is the largest area,
   26,213 notices in July 2026; a month above 100,000 is a scale or date-filter error. bzp_contract_value (mln PLN in
   the KPI layer): Poland's highest month is April 2026, 98,414 mln PLN, of which one result notice reports
   90,051 mln PLN (a value as published, not checked); a month above 200,000 mln PLN is a scale error. No metrics
   models yet: the bounds wait until the KPIs get one. #}
{% macro poc_value_range__uzp_bzp() %}
    {{ return({
        'bounds': [
            ['bzp_notices', 'uzp_bzp:notices:notices', '0', '1e5'],
            ['bzp_contract_value', 'uzp_bzp:notices:%:contract_value', '0', '2e5'],
        ],
        'models': [],
    }) }}
{% endmacro %}
