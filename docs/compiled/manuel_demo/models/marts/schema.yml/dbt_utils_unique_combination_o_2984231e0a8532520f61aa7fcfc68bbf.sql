





with validation_errors as (

    select
        customer_id, subscription_number, week_start
    from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_weekly` where week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery
    group by customer_id, subscription_number, week_start
    having count(*) > 1

)

select *
from validation_errors


