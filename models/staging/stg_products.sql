with raw_products as (

    select
        cast(product_id as int) as product_id,
        initcap(trim(product_name)) as product_name,
        cast(brand_id as int) as brand_id,
        cast(category_id as int) as category_id,
        cast(model_year as int) as model_year,
        case 
            when list_price < 0 then 0  
            when list_price is null then 0
            else list_price
        end as list_price,
        
        current_timestamp() as dbt_loaded_at,
        
    from {{ source('raw', 'products') }}

)
select * from raw_products