
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cohort_date
from `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort`
where cohort_date is null



  
  
      
    ) dbt_internal_test