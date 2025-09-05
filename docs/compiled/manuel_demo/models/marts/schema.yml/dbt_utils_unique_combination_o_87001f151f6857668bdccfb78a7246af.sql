





with validation_errors as (

    select
        customer_id, month_start
    from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly`
    group by customer_id, month_start
    having count(*) > 1

)

select *
from validation_errors


