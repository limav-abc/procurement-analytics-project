with source_data as (
    select * from {{ref('stg_procurement')}}
),
suppliers as (
    select * from {{ ref('dim_suppliers') }}
),
categories as (
    select * from {{ ref('dim_categories') }}
),
dates as (
    select * from {{ ref('dim_dates') }} 
),

joined_data as(
    select 
        s.purchase_order_id,
        sup.supplier_id,
        c.category_id, 
        s.quantity,
        s.unit_price,
        s.negotiated_price,
        d_order.date_id as order_date_id,
        d_delivery.date_id as delivery_date_id,
        s.order_status,
        s.defective_units,
        s.compliance

    from source_data s
    
    left join suppliers sup
        on s.supplier_name = sup.supplier_name
    left join categories c
        on s.item_category = c.category_name
    left join dates d_order
        on s.order_date = d_order.date_day
    left join dates d_delivery
        on s.delivery_date = d_delivery.date_day
),

calculations as(
    select *,
    -- Cálculos
        (quantity * unit_price) as total_order_value,
        (quantity * negotiated_price) as total_negotiated_value,
        (unit_price - negotiated_price)*quantity as total_savings
    from joined_data
)

select * from calculations

