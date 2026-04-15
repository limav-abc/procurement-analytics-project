with date_series as (
    select generate_series as date_day
    from generate_series(date '2020-01-01', date '2030-12-31', interval 1 day)
),

dates as (
    select
        CAST(strftime('%Y%m%d', date_day) AS INTEGER) as date_id,
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(quarter from date_day) as quarter,
        dayname(date_day) as day_name,
        monthname(date_day) as month_name,
        -- Identifica se é final de semana (Sábado = 6, Domingo = 0 no DuckDB)
        extract(dow from date_day) in (0, 6) as is_weekend
    from date_series
)

select * from dates

