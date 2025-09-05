
    
    

with all_values as (

    select
        is_active_month as value_field,
        count(*) as n_records

    from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly` where month_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) dbt_subquery
    group by is_active_month

)

select *
from all_values
where value_field not in (
    '0','1'
)


