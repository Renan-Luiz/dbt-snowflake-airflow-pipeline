select
    "order_date" as order_date,
    "product" as product,
    sum("quantity") as total_quantity,
    sum("margin") as total_margin
from {{ ref('stg_invoices') }}
group by "order_date", "product"
order by order_date, total_quantity desc