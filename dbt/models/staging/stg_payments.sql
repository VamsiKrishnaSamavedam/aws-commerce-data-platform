select
    trim(payment_id) as payment_id,
    trim(order_id) as order_id,
    lower(nullif(trim(payment_method), '')) as payment_method,
    try_cast(nullif(trim(amount), '') as decimal(18, 2)) as payment_amount,
    lower(nullif(trim(payment_status), '')) as payment_status,
    try_cast(nullif(trim(payment_date), '') as timestamp) as paid_at
from {{ source('commerce_incremental', 'payments') }}
