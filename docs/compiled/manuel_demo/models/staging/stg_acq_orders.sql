

select
  cast(customer_id as string) as customer_id,
  nullif(trim(taxonomy_business_category_group), '') as taxonomy_business_category_group
from `manuel-demo-1392926998`.`raw`.`acq_orders`