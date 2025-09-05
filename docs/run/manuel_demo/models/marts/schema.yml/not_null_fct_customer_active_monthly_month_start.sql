
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select month_start
from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly`
where month_start is null



  
  
      
    ) dbt_internal_test