with src_stocks as (
    select
        store_id,
        product_id,
        case 
            when quantity < 0 then 0  
            when quantity is null then 0
            else quantity
        end as quantity,
        current_timestamp() as dbt_loaded_at
    from {{ source('raw', 'stocks') }}
)

select * from src_stocks
