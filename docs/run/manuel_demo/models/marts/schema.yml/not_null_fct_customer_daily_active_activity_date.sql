
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select activity_date
from `manuel-demo-1392926998`.`analytics`.`fct_customer_daily_active`
where activity_date is null



  
  
      
    ) dbt_internal_test