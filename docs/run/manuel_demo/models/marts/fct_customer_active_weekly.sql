
  
    

    create or replace table `manuel-demo-1392926998`.`analytics`.`fct_customer_active_weekly`
      
    partition by week_start
    cluster by customer_id

    
    OPTIONS(
      description="""Weekly rollup of subscription-level activity at grain (customer_id, week_start).\n"""
    )
    as (
      -- ========================================================================================
-- MODEL: fct_customer_active_weekly
-- PURPOSE:
--   Weekly rollup of subscription-level activity. One row per
--   (customer_id, week_start) with:
--     - active_days = SUM(is_active) across the week (count of active days)
--     - is_active_week   = 1 if any day in the week is active
--   Stable slicing dims are carried through for convenience.
--
-- GRAIN:
--   1 row per (customer_id, week_start).
--
-- OUTPUT COLUMNS:
--   - week_start                           -- DATE_TRUNC(activity_date, WEEK(MONDAY))
--   - customer_id
--   - active_days                     -- SUM of daily is_active (0..7)
--   - is_active_week                       -- 1 if active_days > 0 else 0
--   - customer_country
--   - taxonomy_business_category_group
--   - created_at
--   - last_updated_at
--
-- INPUTS:
--   - fct_customer_active_daily(customer_id, activity_date,
--       is_active, customer_country, taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - week_start = DATE_TRUNC(activity_date, WEEK(MONDAY)).
--   - active_days = SUM(is_active) per (customer_id, week_start).
--   - is_active_week   = CASE WHEN active_days > 0 THEN 1 ELSE 0 END.
--
-- PERFORMANCE:
--   - Partition by week_start; cluster by (customer_id).
--   - Incremental INSERT OVERWRITE replaces only recent partitions (via lower bound).
--   - require_partition_filter=true to prevent accidental full scans.
--
-- DATA QUALITY:
--   - Uniqueness at the declared grain.
--   - active_days between 0 and 7; is_active_week ∈ {0,1}.
-- ========================================================================================



with bounds as (
  select
    date(CURRENT_DATE()) as as_of,
    date_sub(date(CURRENT_DATE()),
             interval 31 day) as lower_bound
),
base as (
  select
    DATE_TRUNC(f.activity_date, WEEK(MONDAY)) as week_start,
    f.customer_id,
    f.is_active,
    f.customer_country,
    f.taxonomy_business_category_group
  from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily` f
  where 1=1
  
),
agg as (
  select
    week_start,
    customer_id,
    sum(is_active) as active_days,
    -- carry dims consistently; they’re stable per customer
    any_value(customer_country) as customer_country,
    any_value(taxonomy_business_category_group) as taxonomy_business_category_group
  from base
  group by 1,2
)



select
  a.week_start as week_start,
  a.customer_id,
  a.active_days,
  case when a.active_days > 0 then 1 else 0 end as is_active_week,
  a.customer_country,
  a.taxonomy_business_category_group,
  
    CURRENT_TIMESTAMP() as created_at,
  
  CURRENT_TIMESTAMP() as last_updated_at
from agg a

    );
  