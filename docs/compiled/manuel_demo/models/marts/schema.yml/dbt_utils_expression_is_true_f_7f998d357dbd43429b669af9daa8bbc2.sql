



select
    1
from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily`

where not(activity_date <= DATE(CURRENT_DATE()))

