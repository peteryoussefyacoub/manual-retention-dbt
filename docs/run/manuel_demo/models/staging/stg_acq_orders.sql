

  create or replace view `manuel-demo-1392926998`.`analytics`.`stg_acq_orders`
  OPTIONS(
      description="""Acquisition / taxonomy attributes per customer derived from raw.acq_orders. Intended as a descriptive dimension joined to customers. TBC if a customer can be mapped to multiple acquisition/business taxonomy. \n"""
    )
  as 

select
  cast(customer_id as string) as customer_id,
  nullif(trim(taxonomy_business_category_group), '') as taxonomy_business_category_group
from `manuel-demo-1392926998`.`raw`.`acq_orders`;

