select
    return_id,
    order_id,
    product_id,
    return_reason,
    returned_at
from {{ ref('stg_returns') }}
