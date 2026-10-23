select
    trim(order_id) as order_id,
    trim(customer_id) as customer_id,
    try_cast(nullif(trim(order_date), '') as timestamp) as ordered_at,
    lower(nullif(trim(status), '')) as order_status,
    try_cast(nullif(trim(total_amount), '') as decimal(18, 2)) as order_total_amount,
    updated_at
from {{ source('commerce_incremental', 'orders') }}
