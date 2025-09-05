

  create or replace view `manuel-demo-1392926998`.`analytics`.`stg_activity`
  OPTIONS(
      description="""Cleaned activity/subscription windows derived from raw.activity. Renames from_date \u2192 start_date and to_date \u2192 end_date, and enforces types. Grain: one row per (customer_id, subscription_id, start_date) window, TBC. I assumed the end_date is related to the activity, not the subscription;hence, it should not include any nulls. This is also consistent with the data, TBC. TBC if a customer can have multiple active subscriptions at the same time.\n"""
    )
  as 

select
  cast(customer_id as string)          as customer_id,
  cast(subscription_id as string)      as subscription_id,
  safe.parse_date('%Y-%m-%d', trim(from_date)) as start_date,
  safe.parse_date('%Y-%m-%d', nullif(trim(to_date), '')) as end_date
from `manuel-demo-1392926998`.`raw`.`activity`;

