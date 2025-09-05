
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cohort_month
from `manuel-demo-1392926998`.`analytics_analytics`.`fct_cohort_retention_daily`
where cohort_month is null



  
  
      
    ) dbt_internal_test