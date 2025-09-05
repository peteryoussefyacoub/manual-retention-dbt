
    
    

with all_values as (

    select
        is_active_week as value_field,
        count(*) as n_records

    from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_weekly`
    group by is_active_week

)

select *
from all_values
where value_field not in (
    '0','1'
)


