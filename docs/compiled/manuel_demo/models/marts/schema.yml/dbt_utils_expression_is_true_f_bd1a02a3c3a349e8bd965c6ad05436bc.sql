



select
    1
from (select * from `manuel-demo-1392926998`.`analytics`.`fct_customer_base_updates_monthly` where month >= DATE_SUB(CURRENT_DATE(), INTERVAL 9000 DAY)) dbt_subquery

where not(churned_vs_active_guardrail churned_customers <= new_customers + existing_customers)

