
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select subscription_number
from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily` where activity_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery
where subscription_number is null



  
  
      
    ) dbt_internal_test