select
    shipment_id,
    order_id,
    carrier,
    shipped_at,
    delivered_at,
    shipment_status,
    updated_at
from {{ ref('stg_shipments') }}
