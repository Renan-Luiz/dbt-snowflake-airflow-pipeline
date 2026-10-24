select
    *
from {{ source('snowflake', 'b2b_invoices') }}