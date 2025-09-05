



select
    1
from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_base_updates_monthly` where month >= DATE_SUB(CURRENT_DATE(), INTERVAL 9000 DAY)) dbt_subquery

where not(month <= DATE_TRUNC(CURRENT_DATE(), MONTH))

