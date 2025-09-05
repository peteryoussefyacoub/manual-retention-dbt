



select
    1
from `manuel-demo-1392926998`.`analytics`.`stg_activity`

where not(start_date <= CURRENT_DATE())

