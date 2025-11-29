{{
    config(
        materialized='view',
        schema='marts'
    )
}}

SELECT DISTINCT 
    CAST(TO_CHAR(order_date, 'YYYYMMDD') AS INT) AS date_key,
    order_date,
    
    EXTRACT(year FROM order_date) AS year,
    EXTRACT(month FROM order_date) AS month,

    TO_CHAR(order_date, 'Month') AS month_name,
    TO_CHAR(order_date, 'Day') AS weekday_name

FROM {{ ref('stg_orders') }}
ORDER BY order_date
