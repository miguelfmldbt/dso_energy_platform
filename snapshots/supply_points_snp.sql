{% snapshot supply_points_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='supplyPoint',
        strategy='check',
        check_cols=[
            'ct_code',
            'ct_name',
            'transformer_code',
            'line_code',
            'measure_tension_level',
            'tension_section',
            'meter_code',
            'phase'
        ]
    )
}}

select
    supplyPoint,
    street_name,
    town_name,
    postal_code,
    type_address,
    phone,
    number,
    portal,
    floor,
    letter,
    province_name,
    province_ine_code,
    municipality_name,
    municipality_ine_code,

    ct_code,
    ct_name,
    transformer_code,
    line_code,

    dso_id,
    dso_name,
    measure_tension_level,
    tension_section,
    meter_code,
    meter_tech_code,
    meter_tech_name,
    phase,
    point_type,
        
    data_ingest
from {{ ref('base_crm_data__supply_points') }}

{% endsnapshot %}
