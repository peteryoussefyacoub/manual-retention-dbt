
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_active as value_field,
        count(*) as n_records

    from `manuel-demo-1392926998`.`analytics`.`fct_customer_daily_active`
    group by is_active

)

select *
from all_values
where value_field not in (
    '1'
)



  
  
      
    ) dbt_internal_test