create or replace view sales.order_debug as
select
    order_id,
    customer_id,
    customer_name,
    order_total
from sales.order_summary;
