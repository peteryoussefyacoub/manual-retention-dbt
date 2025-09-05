





with validation_errors as (

    select
        customer_id, subscription_number, activity_date
    from `manuel-demo-1392926998`.`analytics`.`fct_customer_daily_active`
    group by customer_id, subscription_number, activity_date
    having count(*) > 1

)

select *
from validation_errors


