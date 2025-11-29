with raw_stores as (
    select
        cast(store_id as int) as store_id,
        initcap(trim(store_name)) as store_name,
        case
            when lower(trim(phone)) in ('null', '') then null
            else regexp_replace(trim(phone), '[^0-9]', '')
        end as phone, 
        email,
        street,
        city,
        upper(trim(state)) as state,
        zip_code,
        current_timestamp() as dbt_loaded_at
    from {{ source('raw', 'stores') }}
)
select
    *
from raw_stores