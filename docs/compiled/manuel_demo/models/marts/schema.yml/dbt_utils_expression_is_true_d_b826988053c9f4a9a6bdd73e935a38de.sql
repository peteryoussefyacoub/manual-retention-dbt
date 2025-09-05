



select
    1
from `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort`

where not(cohort_date <=DATE(CURRENT_DATE()))

