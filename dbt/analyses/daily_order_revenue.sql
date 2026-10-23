select
    cast(ordered_at as date) as order_date,
    count(*) as order_count,
    sum(order_total_amount) as gross_order_amount,
    sum(completed_payment_amount) as completed_payment_amount,
    sum(return_count) as return_count
from {{ ref('fct_orders') }}
where ordered_at is not null
group by 1
order by 1 desc
