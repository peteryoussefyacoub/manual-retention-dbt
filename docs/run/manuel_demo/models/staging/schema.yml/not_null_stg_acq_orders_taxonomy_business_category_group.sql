
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select taxonomy_business_category_group
from `manuel-demo-1392926998`.`analytics`.`stg_acq_orders`
where taxonomy_business_category_group is null



  
  
      
    ) dbt_internal_test