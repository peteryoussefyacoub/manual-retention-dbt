
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select subscription_number
from `manuel-demo-1392926998`.`analytics`.`dim_customer_subscription`
where subscription_number is null



  
  
      
    ) dbt_internal_test