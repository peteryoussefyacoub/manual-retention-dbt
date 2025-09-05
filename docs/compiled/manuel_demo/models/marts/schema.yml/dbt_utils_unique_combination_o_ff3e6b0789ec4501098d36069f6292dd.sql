





with validation_errors as (

    select
        customer_id, subscription_id
    from `manuel-demo-1392926998`.`analytics`.`dim_customer_subscription`
    group by customer_id, subscription_id
    having count(*) > 1

)

select *
from validation_errors


