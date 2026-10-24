select
    *
from {{ ref('top_10_product_margin') }}
where total_margin_ratio > 50