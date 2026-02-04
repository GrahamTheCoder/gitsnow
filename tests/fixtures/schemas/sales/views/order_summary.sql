create or replace view sales.order_summary as
select
    o.order_id,
    o.customer_id,
    o.order_total,
    c.customer_name
from sales.orders o
join sales.customers c
    on o.customer_id = c.customer_id;
