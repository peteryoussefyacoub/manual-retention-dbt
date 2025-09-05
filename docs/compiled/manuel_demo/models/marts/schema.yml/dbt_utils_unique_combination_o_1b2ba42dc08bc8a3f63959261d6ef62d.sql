





with validation_errors as (

    select
        customer_id, activity_date
    from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily`
    group by customer_id, activity_date
    having count(*) > 1

)

select *
from validation_errors


