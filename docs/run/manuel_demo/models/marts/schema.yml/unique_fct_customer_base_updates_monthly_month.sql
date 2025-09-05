
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select month as unique_field
  from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_base_updates_monthly` where month >= DATE_SUB(CURRENT_DATE(), INTERVAL 9000 DAY)) dbt_subquery
  where month is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test