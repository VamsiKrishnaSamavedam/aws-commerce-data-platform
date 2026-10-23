select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    cast(quantity as decimal(18, 2)) * unit_price as line_item_amount
from {{ ref('stg_order_items') }}
