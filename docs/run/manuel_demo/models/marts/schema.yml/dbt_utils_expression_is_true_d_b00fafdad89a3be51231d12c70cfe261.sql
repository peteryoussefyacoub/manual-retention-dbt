
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort`

where not(cohort_month = DATE_TRUNC(cohort_date, MONTH))


  
  
      
    ) dbt_internal_test