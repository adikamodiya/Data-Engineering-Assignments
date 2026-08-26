--  Total revenue per customer

create or refresh materialized view cyntexa_dev.medallion_day_6.gold_sales_total_revenue_per_customers as
select   
  customer_id,
  sum(total_amount) as total_revenue
from cyntexa_dev.medallion_day_6.silver_sales
group by customer_id;
