with fct_data as(
    select * from {{ ref('fct_procurement_performance') }}
),

supplier_data as(
    select * from {{ ref('dim_suppliers') }}
),

category_data as(
    select * from {{ ref('dim_categories') }}
),

dates_data as (
    select * from {{ ref('dim_dates') }}
),

prepared_data as (
    select 
        fd.*,
        sd.supplier_name,
        cd.category_name,
        date_diff('day',odd.date_day, ddd.date_day) as lead_time_days
    from fct_data fd
    left join dates_data odd on fd.order_date_id = odd.date_id
    left join dates_data ddd on fd.delivery_date_id = ddd.date_id
    left join supplier_data sd on fd.supplier_id = sd.supplier_id
    left join category_data cd on fd.category_id = cd.category_id
    where order_status = 'Delivered'
),

supplier_quality_stats as (
    select
        supplier_id,
        supplier_name,
        category_id,
        category_name,

        round(
        avg(lead_time_days), 2
        ) as avg_lead_time, 

        round(
        avg(avg(lead_time_days)) over(partition by category_id), 2
        ) as category_avg_lead_time,

        sum(defective_units) as total_defective_units_by_category,
        sum(quantity) as quantity_by_category,

        sum(sum(defective_units)) over(partition by supplier_id) as supplier_total_defective_units,
        sum(sum(quantity)) over(partition by supplier_id) as supplier_total_quantity,

        round(
            sum(defective_units) / nullif(sum(quantity), 0), 2
        ) as defective_rate,

        round(
            avg(sum(defective_units) / nullif(sum(quantity), 0)) over(partition by category_id), 2
        ) as category_avg_defective_rate,

        round(
            sum(sum(defective_units)) over(partition by supplier_id) / 
            nullif(sum(sum(quantity)) over(partition by supplier_id), 0), 2
        ) as supplier_overall_defective_rate,

        round(
            sum(sum(defective_units)) over(partition by category_id) / 
            nullif(sum(sum(quantity)) over(partition by category_id), 0), 2
        ) as category_overall_defective_rate

    from prepared_data
    group by 1,2,3,4 
),


calculated_ind as (
    select *,
        round((supplier_overall_defective_rate * 0.3 + defective_rate * 0.7), 2) as weighted_quality_score,
        round(avg_lead_time / nullif(category_avg_lead_time, 0), 2) as lead_time_index,
        round(defective_rate / nullif(category_avg_defective_rate, 0), 2) as defective_rate_index
    from supplier_quality_stats
),

final_score as (
    select *,
        round(
            weighted_quality_score * nullif(lead_time_index, 0) * nullif(defective_rate_index, 0), 2
        ) as final_risk_score
    from calculated_ind
),

supplier_risk_level as (
    select *,
        case 
            when 
                final_risk_score
                > (4.0 * category_overall_defective_rate)
                then 'Critical Risk'
            when 
                final_risk_score
                > (2.5 * category_overall_defective_rate)
                then 'High Risk'
            when 
                final_risk_score
                < (0.8 * category_overall_defective_rate)
                then 'Low Risk'
            else 'Medium Risk'

        end as risk_level
    from final_score
),

calculated_savings as(
    select
        supplier_id,

        count(purchase_order_id) as total_orders,

        sum(total_order_value) as overall_order_value,
        sum(total_negotiated_value) as overall_supplier_spend,
        sum(total_savings) as overall_supplier_savings,

        round((sum(total_savings)/nullif(sum(total_order_value), 0)), 2) 
        as overall_supplier_savings_pct,

        round((sum(total_savings)/nullif(sum(sum(total_savings)) over(), 0)), 2) 
        as supplier_savings_share_in_company,

        round((sum(total_negotiated_value)/nullif(sum(sum(total_negotiated_value)) over(), 0)), 2) 
        as supplier_spend_share_in_company

    from prepared_data 
    group by 1
),

abc_segmentation as (
    select
        supplier_id,
        overall_supplier_spend,
        sum(overall_supplier_spend) over (order by overall_supplier_spend desc) / 
            nullif(sum(overall_supplier_spend) over (), 0) as cumulative_spend_pct
    from calculated_savings
),

abc_final as (
    select
        supplier_id,
        case 
            when cumulative_spend_pct <= 0.80 then 'A' 
            when cumulative_spend_pct <= 0.95 then 'B' 
            else 'C'                                 
        end as abc_class
    from abc_segmentation
),

final_table as (
    select 
        srl.*,
        cs.total_orders,
        cs.overall_order_value,
        cs.overall_supplier_spend,
        cs.overall_supplier_savings,
        cs.supplier_savings_share_in_company,
        cs.supplier_spend_share_in_company,
        cs.overall_supplier_savings_pct,
        abc.abc_class,

        case 

            when abc.abc_class = 'A' and srl.risk_level in ('Critical Risk', 'High Risk') then '1 - Immediate Intervention'
            when abc.abc_class = 'A' and srl.risk_level in ('Low Risk', 'Medium Risk') then '2 - Strategic Monitoring'
            when abc.abc_class in ('B', 'C') and srl.risk_level in ('Critical Risk', 'High Risk') then '3 - Operational Risk'
            else '4 - Standard Management'

        end as strategic_segment

    from supplier_risk_level srl
    left join calculated_savings cs on srl.supplier_id = cs.supplier_id 
    left join abc_final abc on srl.supplier_id = abc.supplier_id
)

select * from final_table
order by final_risk_score desc, overall_supplier_savings desc