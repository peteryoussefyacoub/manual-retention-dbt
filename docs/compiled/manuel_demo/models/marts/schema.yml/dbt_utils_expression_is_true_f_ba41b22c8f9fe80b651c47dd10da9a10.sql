



select
    1
from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_monthly`

where not(months_since_cohort >= 0)

