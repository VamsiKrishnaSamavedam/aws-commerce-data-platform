select
    payment_id,
    order_id,
    payment_method,
    payment_amount,
    payment_status,
    paid_at
from {{ ref('stg_payments') }}
