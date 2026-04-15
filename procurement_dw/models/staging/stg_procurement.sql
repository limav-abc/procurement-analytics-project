with source_data as (
    select * from {{source('raw_data', 'raw_procurement')}}
    where po_id is not null
),

renamed as (
    select
        -- Identificação
        po_id as purchase_order_id,
        supplier as supplier_name,
        item_category,
        -- Datas
        order_date,
        delivery_date,
        -- Status
        order_status,
        compliance,
        -- Números
        cast(quantity as integer) as quantity,
        cast(unit_price as decimal(18,2)) as unit_price,
        cast(negotiated_price as decimal(18,2)) as negotiated_price,
        cast(defective_units as integer) as defective_units,
        -- Auditoria
        current_timestamp  as loaded_at

    from source_data
)

select * from renamed

