





with validation_errors as (

    select
        customer_id, subscription_number, activity_date, customer_country, taxonomy_business_category_group
    from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily` where activity_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery
    group by customer_id, subscription_number, activity_date, customer_country, taxonomy_business_category_group
    having count(*) > 1

)

select *
from validation_errors


