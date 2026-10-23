select
    shipment_status,
    carrier,
    count(*) as shipment_count,
    avg(date_diff('day', shipped_at, delivered_at)) as average_delivery_days
from {{ ref('fct_shipments') }}
where shipped_at is not null
  and delivered_at is not null
group by 1, 2
order by shipment_count desc
