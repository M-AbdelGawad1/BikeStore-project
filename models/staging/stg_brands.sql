with staging as (


    select * from {{ source('raw', 'brands') }}

)

select
    cast(brand_id as int) as brand_id,
    initcap(trim(brand_name)) as brand_name,
    current_timestamp() as dbt_loaded_at

from staging


