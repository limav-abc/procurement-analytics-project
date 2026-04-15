with source_data as(
    select * from {{ref('stg_procurement')}}
),

categories as (
    select 
    md5(upper(trim(item_category))) as category_id,
    item_category as category_name
    from source_data
    group by item_category
)

select * from categories

