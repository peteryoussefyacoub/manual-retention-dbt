
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select activity_start_date
from `manuel-demo-1392926998`.`analytics`.`dim_customer_subscription`
where activity_start_date is null



  
  
      
    ) dbt_internal_test