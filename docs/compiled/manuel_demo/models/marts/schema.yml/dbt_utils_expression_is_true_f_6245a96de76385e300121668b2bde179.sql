



select
    1
from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly` where month_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery

where not(subscription_number >= 1)

