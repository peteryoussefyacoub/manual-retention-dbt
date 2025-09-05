
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from `manuel-demo-1392926998`.`analytics`.`dim_acquisition_taxonomy`
where customer_id is null



  
  
      
    ) dbt_internal_test