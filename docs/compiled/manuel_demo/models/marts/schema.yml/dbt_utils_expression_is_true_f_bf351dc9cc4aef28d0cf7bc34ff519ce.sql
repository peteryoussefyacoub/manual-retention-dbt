



select
    1
from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_daily`

where not(active_customers between 0 and cohort_size)

