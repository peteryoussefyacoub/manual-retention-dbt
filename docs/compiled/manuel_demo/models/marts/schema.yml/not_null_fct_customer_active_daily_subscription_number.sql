
    
    



select subscription_number
from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily` where activity_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery
where subscription_number is null


