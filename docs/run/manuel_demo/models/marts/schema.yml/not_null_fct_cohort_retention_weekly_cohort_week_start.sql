
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cohort_week_start
from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_weekly`
where cohort_week_start is null



  
  
      
    ) dbt_internal_test