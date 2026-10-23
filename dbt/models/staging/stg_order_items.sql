select
    trim(order_item_id) as order_item_id,
    trim(order_id) as order_id,
    trim(product_id) as product_id,
    try_cast(nullif(trim(quantity), '') as integer) as quantity,
    try_cast(nullif(trim(unit_price), '') as decimal(18, 2)) as unit_price
from {{ source('commerce_incremental', 'order_items') }}
