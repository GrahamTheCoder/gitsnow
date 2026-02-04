create or replace view sales.order_account_summary as
select
    o.order_id,
    ca.account_id,
    o.order_total
from sales.orders o
join sales.customer_accounts ca
    on o.customer_id = ca.customer_id;
