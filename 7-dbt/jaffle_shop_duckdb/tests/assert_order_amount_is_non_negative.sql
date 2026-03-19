-- Orders should never have a negative total amount.
-- If this query returns any rows, the test fails.

select
    order_id,
    amount
from {{ ref('orders') }}
where amount < 0
