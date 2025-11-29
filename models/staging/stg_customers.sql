with raw_customers  as (
    select
        cast(customer_id as int ) as customer_id,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        concat(trim(first_name), ' ', trim(last_name)) as full_name,
         case
            when lower(trim(phone)) in ('null', '') then null
            else regexp_replace(trim(phone), '[^0-9]', '')
        end as phone,

        email,
        street,
        city,
        upper(trim(state)) as state ,
        zip_code,
         concat(
            coalesce(trim(street), 'N/A'), ', ',
            coalesce(trim(city), 'N/A'), ', ',
            coalesce(upper(trim(state)), 'N/A'), ' ',
            coalesce(trim(zip_code), 'N/A')
        ) as full_address,

        case 
            when upper(state) in ('CA', 'OR', 'WA') then 'West'
            when upper(state) in ('NY', 'NJ', 'PA', 'MA') then 'East'
            when upper(state) in ('TX', 'FL', 'GA') then 'South'
            when upper(state) in ('IL', 'OH', 'MI') then 'Midwest'
            else 'Other'
        end as region
    from {{ source('raw', 'customers') }}
)

select  
    *
from raw_customers