with order_item_rollup as (
    select
        order_id,
        count(*) as order_item_count,
        sum(quantity) as total_item_quantity,
        sum(line_item_amount) as order_item_amount
    from {{ ref('fct_order_items') }}
    group by order_id
),

payment_rollup as (
    select
        order_id,
        count(*) as payment_attempt_count,
        sum(case when payment_status = 'completed' then payment_amount else cast(0 as decimal(18, 2)) end) as completed_payment_amount
    from {{ ref('fct_payments') }}
    group by order_id
),

shipment_rollup as (
    select
        order_id,
        count(*) as shipment_count,
        max_by(shipment_status, updated_at) as latest_shipment_status,
        max(delivered_at) as latest_delivered_at
    from {{ ref('fct_shipments') }}
    group by order_id
),

return_rollup as (
    select
        order_id,
        count(*) as return_count
    from {{ ref('fct_returns') }}
    group by order_id
)

select
    orders.order_id,
    orders.customer_id,
    orders.ordered_at,
    orders.order_status,
    orders.order_total_amount,
    coalesce(items.order_item_count, 0) as order_item_count,
    coalesce(items.total_item_quantity, 0) as total_item_quantity,
    coalesce(items.order_item_amount, cast(0 as decimal(18, 2))) as order_item_amount,
    coalesce(payments.payment_attempt_count, 0) as payment_attempt_count,
    coalesce(payments.completed_payment_amount, cast(0 as decimal(18, 2))) as completed_payment_amount,
    coalesce(shipments.shipment_count, 0) as shipment_count,
    shipments.latest_shipment_status,
    shipments.latest_delivered_at,
    coalesce(returns.return_count, 0) as return_count,
    orders.updated_at
from {{ ref('stg_orders') }} as orders
left join order_item_rollup as items on orders.order_id = items.order_id
left join payment_rollup as payments on orders.order_id = payments.order_id
left join shipment_rollup as shipments on orders.order_id = shipments.order_id
left join return_rollup as returns on orders.order_id = returns.order_id
