



select
    1
from `manuel-demo-1392926998`.`analytics`.`dim_customer_subscription`

where not(activity_start_date <= CURRENT_DATE())

