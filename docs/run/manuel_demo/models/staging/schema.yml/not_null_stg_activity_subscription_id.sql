
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select subscription_id
from `manuel-demo-1392926998`.`analytics`.`stg_activity`
where subscription_id is null



  
  
      
    ) dbt_internal_test