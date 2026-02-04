create or replace view sales.order_debug_v2 as
select
    oas.order_id,
    oas.account_id,
    os.customer_name,
    oas.order_total
from sales.order_account_summary oas
join sales.order_summary os
    on oas.order_id = os.order_id;
