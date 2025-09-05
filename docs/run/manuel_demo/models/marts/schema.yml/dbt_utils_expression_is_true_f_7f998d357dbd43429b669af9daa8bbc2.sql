
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily`

where not(activity_date <= DATE(CURRENT_DATE()))


  
  
      
    ) dbt_internal_test