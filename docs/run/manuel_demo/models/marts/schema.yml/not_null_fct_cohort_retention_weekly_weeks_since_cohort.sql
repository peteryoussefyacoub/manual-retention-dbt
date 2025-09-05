
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select weeks_since_cohort
from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_weekly`
where weeks_since_cohort is null



  
  
      
    ) dbt_internal_test