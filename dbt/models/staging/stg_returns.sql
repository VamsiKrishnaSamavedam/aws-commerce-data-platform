select
    trim(return_id) as return_id,
    trim(order_id) as order_id,
    trim(product_id) as product_id,
    nullif(trim(return_reason), '') as return_reason,
    try_cast(nullif(trim(return_date), '') as timestamp) as returned_at
from {{ source('commerce_incremental', 'returns') }}
