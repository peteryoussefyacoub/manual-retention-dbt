
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_daily`

where not(cohort_size >= 0)


  
  
      
    ) dbt_internal_test