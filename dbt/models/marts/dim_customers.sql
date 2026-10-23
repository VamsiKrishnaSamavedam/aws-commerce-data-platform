select
    customer_id,
    first_name,
    last_name,
    email,
    country_code,
    created_at,
    updated_at
from {{ ref('stg_customers') }}
