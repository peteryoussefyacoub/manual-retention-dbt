
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        customer_id, subscription_number, activity_date, customer_country, taxonomy_business_category_group
    from `manuel-demo-1392926998`.`analytics`.`fct_customer_daily_active`
    group by customer_id, subscription_number, activity_date, customer_country, taxonomy_business_category_group
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test