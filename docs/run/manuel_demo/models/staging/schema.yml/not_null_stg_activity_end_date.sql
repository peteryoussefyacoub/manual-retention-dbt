
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select end_date
from `manuel-demo-1392926998`.`analytics`.`stg_activity`
where end_date is null



  
  
      
    ) dbt_internal_test