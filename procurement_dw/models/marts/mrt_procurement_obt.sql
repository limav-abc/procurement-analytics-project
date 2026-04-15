with suppliers as (
    select * from {{ ref("mrt_suppliers_360") }}
),

categories as (
    select * from {{ ref("mrt_categories_360") }}
)

select
    c.*,
    s.* exclude (category_id, category_name, supplier_id, supplier_name)

from categories c
left join suppliers s
    on c.category_id = s.category_id
    and c.supplier_id = s.supplier_id