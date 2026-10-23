select
    products.category,
    count(*) as order_item_count,
    sum(items.quantity) as units_sold,
    sum(items.line_item_amount) as gross_merchandise_value
from {{ ref('fct_order_items') }} as items
inner join {{ ref('dim_products') }} as products
    on items.product_id = products.product_id
group by 1
order by gross_merchandise_value desc
