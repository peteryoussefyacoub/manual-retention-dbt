
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select retained_customers
from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_base_updates_monthly` where month >= DATE_SUB(CURRENT_DATE(), INTERVAL 9000 DAY)) dbt_subquery
where retained_customers is null



  
  
      
    ) dbt_internal_test