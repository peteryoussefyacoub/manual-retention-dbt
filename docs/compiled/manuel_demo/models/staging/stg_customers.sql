

select
  cast(customer_id as string) as customer_id,
  upper(trim(customer_country)) as customer_country
from `manuel-demo-1392926998`.`raw`.`customers`