with customer as (
    select * from {{ ref('stg_customers') }}
)
select 
    customer_id,
    first_name,
    last_name,
    city,
    state,
    region,
    current_timestamp() as dbt_loaded_at 
from customer
