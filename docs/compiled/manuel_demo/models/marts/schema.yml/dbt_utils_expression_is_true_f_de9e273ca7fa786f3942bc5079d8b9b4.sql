



select
    1
from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly`

where not(active_days between 0 and 31)

