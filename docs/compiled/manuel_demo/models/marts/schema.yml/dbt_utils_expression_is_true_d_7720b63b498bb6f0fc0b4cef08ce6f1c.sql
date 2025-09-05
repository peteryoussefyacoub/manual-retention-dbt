



select
    1
from `manuel-demo-1392926998`.`analytics_analytics`.`dim_customer_cohort`

where not(cohort_date <= CURRENT_DATE())

