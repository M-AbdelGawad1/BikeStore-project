with raw_categories as (

    select
        cast(category_id as int) as category_id,
        initcap(trim(category_name)) as category_name
    from {{ source('raw', 'categories') }}
)

select
    category_id,
    category_name as category

from raw_categories 

