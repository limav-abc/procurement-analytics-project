with fct_data as(
    select * from{{ref("fct_procurement_performance")}}
),

categories_data as(
    select * from {{ref("dim_categories")}}
),

supplier_data as(
    select * from {{ref("dim_suppliers")}}
),

prepared_data as(
    select 
        fd.*,
        cd.category_name,
        sd.supplier_name

    from fct_data fd
    left join categories_data cd
        on fd.category_id = cd.category_id
    left join supplier_data sd
        on fd.supplier_id = sd.supplier_id
    where order_status = 'Delivered'
),

base_metrics as(
    select
        category_id,
        category_name,
        supplier_id,
        supplier_name,

        count(purchase_order_id) as total_orders,

        round(
            avg(unit_price), 2
        ) as supplier_average_price_in_category,

        sum(total_negotiated_value) as supplier_spend_in_category,
        sum(total_order_value) as supplier_orders_value_in_category,
        sum(total_savings) as supplier_savings_in_category

    from prepared_data
    group by 1,2,3,4

),

calculated_metrics as(
    select
        *,

        round(avg(supplier_average_price_in_category) over(partition by category_id), 2) 
        as category_average_price,

        sum(supplier_spend_in_category) over(partition by category_id) as total_category_spend,
        sum(supplier_orders_value_in_category) over(partition by category_id) as total_category_orders_value,
        sum(supplier_savings_in_category) over(partition by category_id) as total_category_savings,

        sum(supplier_spend_in_category) over() as total_company_spend,
        sum(supplier_savings_in_category) over() as total_company_savings

    from base_metrics
),

final_metrics as(
    select
        *,

        round(supplier_average_price_in_category - category_average_price, 2) 
        as absolute_price_variance,
        round(((supplier_average_price_in_category - category_average_price) / nullif(category_average_price, 0)), 2) 
        as percentage_price_variance,

        round((supplier_savings_in_category/nullif(total_category_orders_value, 0)), 2) 
        as supplier_savings_pct_in_category,
        round((supplier_savings_in_category/nullif(total_category_savings, 0)), 2) 
        as supplier_savings_share_in_category,
        round((supplier_spend_in_category/nullif(total_category_spend, 0)), 2) 
        as supplier_spend_share_in_category,

        round((total_category_savings/nullif(total_category_orders_value, 0)), 2) 
        as category_savings_pct,
        round((total_category_savings/nullif(total_company_savings, 0)), 2) 
        as category_savings_share_in_company,
        round((total_category_spend/nullif(total_company_spend, 0)), 2)
        as category_spend_share_in_company

    from calculated_metrics
)

select * from final_metrics
order by total_category_spend desc, supplier_spend_in_category desc