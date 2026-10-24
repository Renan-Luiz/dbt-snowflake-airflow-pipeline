select
    *
from {{ ref('product_sold') }}
where order_date between '2026-01-01' and '2026-09-16' 