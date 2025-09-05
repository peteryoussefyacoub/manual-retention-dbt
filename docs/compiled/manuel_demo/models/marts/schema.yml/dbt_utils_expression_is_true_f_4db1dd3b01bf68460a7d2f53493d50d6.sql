



select
    1
from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_monthly`

where not(active_customers between 0 and cohort_size)

