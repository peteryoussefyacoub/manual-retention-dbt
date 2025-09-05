



select
    1
from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily` where activity_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery

where not(activity_date <= DATE('2025-09-05 10:16:09.818673+00:00'))

