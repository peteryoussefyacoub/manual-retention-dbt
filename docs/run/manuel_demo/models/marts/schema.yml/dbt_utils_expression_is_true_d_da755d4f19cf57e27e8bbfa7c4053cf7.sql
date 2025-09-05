
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `manuel-demo-1392926998`.`analytics`.`dim_customer_subscription`

where not(subscription_number >= 1)


  
  
      
    ) dbt_internal_test