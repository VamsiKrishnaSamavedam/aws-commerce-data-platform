select
    trim(shipment_id) as shipment_id,
    trim(order_id) as order_id,
    nullif(trim(carrier), '') as carrier,
    try_cast(nullif(trim(ship_date), '') as timestamp) as shipped_at,
    try_cast(nullif(trim(delivery_date), '') as timestamp) as delivered_at,
    lower(nullif(trim(status), '')) as shipment_status,
    updated_at
from {{ source('commerce_incremental', 'shipments') }}
