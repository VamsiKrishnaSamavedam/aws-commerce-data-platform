select
    trim(product_id) as product_id,
    nullif(trim(product_name), '') as product_name,
    nullif(trim(category), '') as category,
    try_cast(nullif(trim(price), '') as decimal(18, 2)) as price,
    updated_at
from {{ source('commerce_incremental', 'products') }}
