
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select last_updated_at
from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_weekly`
where last_updated_at is null



  
  
      
    ) dbt_internal_test