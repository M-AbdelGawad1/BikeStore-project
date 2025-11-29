with stores as (
    select
    *
     from {{ ref('stg_stores') }}
)
select
    store_id,
    store_name,
    phone,  
    city,
    state,
    current_timestamp() as dbt_loaded_at
from stores
