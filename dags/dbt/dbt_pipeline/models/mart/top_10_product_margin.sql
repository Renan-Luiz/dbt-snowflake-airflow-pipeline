with total as (
    select
    "product",
    sum("revenue") as total_revenue,
    sum("cost") as total_cost
from {{ ref('stg_invoices') }}
group by "product"
)
select 
    "product" as product,
    {{ margin_percentage("total_revenue", "total_cost") }} as total_margin_ratio
from total
order by total_margin_ratio desc
limit 10

