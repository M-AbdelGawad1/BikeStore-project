with raw_staffs as (
    select
        cast(staff_id as int) as staff_id,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        concat(trim(first_name), ' ', trim(last_name)) as full_name,
        email,
        phone,
        regexp_replace(phone, '[^0-9]', '') as phone_clean,
        active,
        cast(store_id as int) as store_id,
        cast(manager_id as int) as manager_id,
        case 
            when manager_id is null then 'Manager'
            else 'Staff'
        end as staff_level,
        current_timestamp() as dbt_loaded_at
    from {{ source('raw', 'staffs') }}
)

select
    *
from raw_staffs
