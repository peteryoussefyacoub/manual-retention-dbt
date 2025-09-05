
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_weekly` where week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery

where not(active_days_week between 0 and 7)


  
  
      
    ) dbt_internal_test