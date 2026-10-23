select
    payment_status,
    payment_method,
    count(*) as payment_attempt_count,
    sum(payment_amount) as payment_amount
from {{ ref('fct_payments') }}
group by 1, 2
order by payment_status, payment_amount desc
