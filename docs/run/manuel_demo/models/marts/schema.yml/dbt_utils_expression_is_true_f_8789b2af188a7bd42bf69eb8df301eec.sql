
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `manuel-demo-1392926998`.`analytics_analytics`.`fct_cohort_retention_rates_daily`

where not(retention_rate between 0 and 1)


  
  
      
    ) dbt_internal_test