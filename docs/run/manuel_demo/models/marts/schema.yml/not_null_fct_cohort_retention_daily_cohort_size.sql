
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cohort_size
from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_daily`
where cohort_size is null



  
  
      
    ) dbt_internal_test