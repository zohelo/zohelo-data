{# ITU DataHub (GL-031, #2418): totals of every economy, the metrics filter applied to build_itu_datahub (Dev trial
   45874151208): internet users 0 % (North Korea 2000) to 100 % (Saudi Arabia 2021; Poland 2024 88.58); fixed broadband
   0 (Haiti 2012) to 75.75 per 100 (Cayman Islands 2019; Poland 2024 25.36); active mobile broadband 0 (Afghanistan
   2007) to 204.68 per 100 (United Arab Emirates 2020; Poland 2024 196.42). #}
{% macro poc_value_range__itu_datahub() %}
    {{ return({
        'bounds': [
            ['internet_users', 'itu_datahub:itu_dh:PT_POP:IT_NET_USER', '0', '100'],
            ['fixed_broadband_subscriptions', 'itu_datahub:itu_dh:SB_10P2_HB:IT_NET_BBND', '0', '100'],
            ['mobile_broadband_subscriptions', 'itu_datahub:itu_dh:SB_10P2_HB:ACT_MOB_SB', '0', '300'],
        ],
        'models': [
            'poc_metrics_internet_users',
            'poc_metrics_fixed_broadband_subscriptions',
            'poc_metrics_mobile_broadband_subscriptions',
        ],
    }) }}
{% endmacro %}
