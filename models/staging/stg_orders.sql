with staging_orders as (
    select
        cast(order_id as int) as order_id,
        cast(customer_id as int) as customer_id,
        cast(order_status as int) as order_status,
        case 
            when order_status = 1 then 'Pending'
            when order_status = 2 then 'Processing'
            when order_status = 3 then 'Rejected'
            when order_status = 4 then 'Completed'
            else 'Unknown'
        end as order_status_name,
        order_date,
        required_date,
        shipped_date,
        case 
            when shipped_date is null then 'Not Shipped'
            when shipped_date <= required_date then 'On Time'
            when shipped_date > required_date then 'Late'
        end as delivery_status,
        
        cast(store_id as int) as store_id,
        cast(staff_id as int) as staff_id,
        current_timestamp() as dbt_loaded_at
    from {{ source('raw', 'orders') }}
)
select * from staging_orders 