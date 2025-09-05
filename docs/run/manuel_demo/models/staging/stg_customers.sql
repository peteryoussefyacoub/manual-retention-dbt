

  create or replace view `manuel-demo-1392926998`.`analytics`.`stg_customers`
  OPTIONS(
      description="""Cleaned customer dimension derived from raw.customers. One row per customer. Column names are standardized and types are enforced for downstream modeling. Grain: customer_id.\n"""
    )
  as 

select
  cast(customer_id as string) as customer_id,
  upper(trim(customer_country)) as customer_country
from `manuel-demo-1392926998`.`raw`.`customers`;

