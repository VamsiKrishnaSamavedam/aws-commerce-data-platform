select
    trim(customer_id) as customer_id,
    nullif(trim(first_name), '') as first_name,
    nullif(trim(last_name), '') as last_name,
    lower(nullif(trim(email), '')) as email,
    upper(nullif(trim(country), '')) as country_code,
    created_at,
    updated_at
from {{ source('commerce_incremental', 'customers') }}
