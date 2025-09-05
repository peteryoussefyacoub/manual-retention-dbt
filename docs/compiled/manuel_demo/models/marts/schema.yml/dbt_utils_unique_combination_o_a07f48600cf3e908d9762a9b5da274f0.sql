





with validation_errors as (

    select
        customer_id, subscription_number, month_start
    from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly` where month_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery
    group by customer_id, subscription_number, month_start
    having count(*) > 1

)

select *
from validation_errors


