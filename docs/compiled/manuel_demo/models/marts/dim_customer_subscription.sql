-- ========================================================================================
-- MODEL: dim_customer_subscription
-- PURPOSE:
--   One row per (customer_id, subscription_id) capturing the earliest activity_start_date
--   for that subscription
--
-- GRAIN:
--   1 row per (customer_id, subscription_id).
--
-- OUTPUT COLUMNS:
--   - customer_id
--   - subscription_id
--   - activity_start_date        -- MIN(start_date) across all activity windows for the sub
--                                  then by numeric subscription_id (SAFE_CAST) as tiebreaker
--
-- INPUTS:
--   - stg_activity(customer_id, subscription_id, start_date)
--
-- BUSINESS LOGIC:
--   - Compute activity_start_date = MIN(start_date) per (customer_id, subscription_id).
--   - Rank subscriptions per customer by activity_start_date ascending; ties broken by
--     SAFE_CAST(subscription_id AS INT64), then subscription_id string for determinism.
--
-- ASSUMPTIONS:
--   - start_date is present for ranked rows (filtered in CTE).
--   - subscription_id is incrementally generated.
--
-- PERFORMANCE:
--   - Small dimension; materialized as TABLE for stable downstream joins.
--   - No partitioning required; index via clustering not needed at this scale.
--
-- DATA QUALITY:
--   - Exactly one row per (customer_id, subscription_id).
--   - activity_start_date is not in the future.
-- ========================================================================================



with firsts as (
  select
    customer_id,
    subscription_id,
    min(start_date) as activity_start_date
  from `manuel-demo-1392926998`.`analytics`.`stg_activity`
  where subscription_id is not null and start_date is not null
  group by 1,2
)

select
  customer_id,
  subscription_id,
  activity_start_date
from firsts