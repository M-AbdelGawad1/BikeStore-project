with staff as (
    select
        *
    from {{ ref('stg_staffs') }}
)
select 
    staff_id,
    first_name,
    last_name,
    full_name,
    email,
    active,
    store_id,
    manager_id,
    staff_level,
    current_timestamp() as dbt_loaded_at
from staff
