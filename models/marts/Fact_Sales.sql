{{
    config(
        materialized='incremental',
        unique_key='sales_key',
        schema='marts',
        on_schema_change='fail'
    )
}}
with orders as (
    SELECT * FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
    where order_date >= (select max(order_date) from {{ this }})
    {% endif %}
)
    
,
order_items as (
    select *
    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
        where order_id in (select order_id from orders)
    {% endif %}
)
select 
{{ dbt_utils.generate_surrogate_key(['o.order_id','oi.item_id']) }} as sales_key,
    
    o.order_id,
    o.customer_id,
    o.store_id,
    o.staff_id,
    oi.product_id,
    -- oi.order_item_id,
    CAST(TO_CHAR(order_date, 'YYYYMMDD') AS INT) AS date_key,

--measures
    o.order_status_name as order_status,
    o.delivery_status,
    oi.quantity,
    oi.unit_price,
    oi.discount,
    oi.unit_price * (1- oi.discount) as price_after_discount, 

    (oi.quantity * oi.unit_price) *(1- oi.discount) as total_sale_amount,



from orders o
join order_items oi
    on o.order_id = oi.order_id
