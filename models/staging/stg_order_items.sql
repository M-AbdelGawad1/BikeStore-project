with stage_order_items as  (


    select
        order_id,
        item_id,
        cast(product_id as int) as product_id,  
        case 
            when quantity <= 0 then 1  -- Handle invalid quantities
            when quantity is null then 1
            else quantity
        end as quantity,   
        case 
            when list_price < 0 then 0  
            when list_price is null then 0
            else list_price
        end as unit_price,           
        case 
            when discount < 0 then 0  
            when discount is null then 0
            else discount
        end as    
        discount,
        current_timestamp() as dbt_loaded_at
        from {{ source('raw', 'order_items') }}
)
select 
    *   
from stage_order_items
