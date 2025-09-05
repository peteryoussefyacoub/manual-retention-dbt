

select
  cast(customer_id as string)          as customer_id,
  cast(subscription_id as string)      as subscription_id,
  safe.parse_date('%Y-%m-%d', trim(from_date)) as start_date,
  safe.parse_date('%Y-%m-%d', nullif(trim(to_date), '')) as end_date
from `manuel-demo-1392926998`.`raw`.`activity`