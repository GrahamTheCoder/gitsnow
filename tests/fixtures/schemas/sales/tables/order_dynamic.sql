create or replace dynamic table sales.order_dynamic
    target_lag = '1 minute'
    warehouse = 'COMPUTE_WH'
as
select
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.order_total
from sales.orders o
join sales.customers c
    on o.customer_id = c.customer_id;
