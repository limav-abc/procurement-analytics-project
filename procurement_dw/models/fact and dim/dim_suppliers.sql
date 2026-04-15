with source_data as(
    select * from {{ref('stg_procurement')}}
),

suppliers as (
    select 
    md5(upper(trim(supplier_name))) as supplier_id,
    supplier_name,
    mode(item_category) as main_category
    from source_data
    group by supplier_name
)

select * from suppliers

