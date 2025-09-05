
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `manuel-demo-1392926998`.`analytics`.`stg_activity`

where not(end_date >= start_date)


  
  
      
    ) dbt_internal_test