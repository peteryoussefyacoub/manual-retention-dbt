
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `manuel-demo-1392926998`.`analytics`.`fct_customer_daily_active`

where not(activity_date <= DATE('2025-09-05 08:15:27.216476+00:00'))


  
  
      
    ) dbt_internal_test