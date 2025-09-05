
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_country
from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily`
where customer_country is null



  
  
      
    ) dbt_internal_test