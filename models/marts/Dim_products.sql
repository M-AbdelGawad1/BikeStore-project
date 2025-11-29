with products as (
    select * from {{ ref('stg_products') }}
),

brands as (
    select * from {{ ref('stg_brands') }}
),

categories as (
    select * from {{ ref('stg_categories') }}
),

stocks as (
    select 
        product_id,
        sum(quantity) as total_stock_quantity
        
    from {{ ref('stg_stocks') }}
    group by product_id
),
product_dimension as (
    select
        -- Primary Key
        p.product_id,
        p.product_name,
        p.model_year,
        p.list_price,
        
        p.brand_id,
        b.brand_name,
    
        
        p.category_id,
        c.category,
    
        
        -- Stock information
        coalesce(s.total_stock_quantity, 0) as total_stock_quantity,
        
    
     
        current_timestamp() as dbt_loaded_at
        
    from products p
    inner join brands b on p.brand_id = b.brand_id
    inner join categories c on p.category_id = c.category_id
    left join stocks s on p.product_id = s.product_id
)

select * from product_dimension