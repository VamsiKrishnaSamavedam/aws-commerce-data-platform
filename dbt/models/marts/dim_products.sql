select
    product_id,
    product_name,
    category,
    price,
    updated_at
from {{ ref('stg_products') }}
