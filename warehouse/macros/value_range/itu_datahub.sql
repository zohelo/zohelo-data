{# ITU DataHub (GL-031, #2418): totals of every economy, the metrics filter applied to build_itu_datahub (Dev trial
   TRIAL_RUN). BOUNDS_EVIDENCE #}
{% macro poc_value_range__itu_datahub() %}
    {{ return({
        'bounds': [
            ['internet_users', 'itu_datahub:itu_dh:PT_POP:IT_NET_USER', '0', '100'],
            ['fixed_broadband_subscriptions', 'itu_datahub:itu_dh:SB_10P2_HB:IT_NET_BBND', '0', '100'],
            ['mobile_broadband_subscriptions', 'itu_datahub:itu_dh:SB_10P2_HB:ACT_MOB_SB', '0', '500'],
        ],
        'models': [
            'poc_metrics_internet_users',
            'poc_metrics_fixed_broadband_subscriptions',
            'poc_metrics_mobile_broadband_subscriptions',
        ],
    }) }}
{% endmacro %}
