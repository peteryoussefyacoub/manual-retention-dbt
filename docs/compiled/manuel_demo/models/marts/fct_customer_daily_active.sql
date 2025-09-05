-- ========================================================================================
-- MODEL: fct_customer_daily_active
-- PURPOSE:
--   Subscription-level daily activity fact. Emits one row per (customer_id, subscription_number,
--   activity_date) with a binary is_active=1 for each active day, plus stable slicing dims.
--
-- GRAIN:
--   1 row per (customer_id, subscription_number, activity_date,customer_country,
--	 taxonomy_business_category_group).
--
-- OUTPUT COLUMNS:
--   - activity_date
--   - customer_id
--   - subscription_number                 -- 1-based order per customer from dim_customer_subscription
--   - is_active                           -- always 1 when a row exists for that day
--   - customer_country
--   - taxonomy_business_category_group
--   - created_at
--   - last_updated_at
--
-- INPUTS:
--   - stg_activity(customer_id, subscription_id, start_date, end_date)
--   - dim_customer_subscription(customer_id, subscription_id → subscription_number)
--   - dim_customer_country(customer_id → customer_country)
--   - dim_acquisition_taxonomy(customer_id → taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - Expand each window to days with GENERATE_DATE_ARRAY; on incremental runs, bound
--     the generated dates to the last 180 days.
--   - Join 1:1 dims AFTER explosion to avoid carrying dim columns through the UNNEST.
--   - Preserve created_at during MERGE for existing rows in the incremental horizon.
--
-- ASSUMPTIONS:
--   - start_date <= end_date (enforced upstream).
--   - subscription_number exists in dim_customer_subscription for all subscription_ids seen.
--
-- PERFORMANCE:
--   - Partition: activity_date. Cluster: customer_id, subscription_number, customer_country,
--     taxonomy_business_category_group.
--   - Incremental MERGE on (customer_id, subscription_number, activity_date).
--	 - A retention period of 9999 days is added to avoid uncontroled growth; however, number
--     should be configured for reasonable retention in a production environment.
--
-- DATA QUALITY:
--   - Uniqueness at the declared grain.
--   - Non-null keys/dates; is_active ∈ {1}; subscription_number >= 1; timestamps populated.
-- ========================================================================================



with win as (
  -- Cap open-ended intervals at date('2025-09-05 08:06:25.325299+00:00'); restrict to windows that intersect the horizon
  select 
    a.customer_id,
	a.subscription_id,
    a.start_date,
    coalesce(a.end_date, DATE('2025-09-05 08:06:25.325299+00:00')) as end_date
  from `manuel-demo-1392926998`.`analytics`.`stg_activity` a
  where a.start_date is not null
    and a.start_date <= coalesce(a.end_date, DATE('2025-09-05 08:06:25.325299+00:00'))
    
),
days as (
  -- Expand to daily grain and collapse concurrent subscriptions
  select distinct
    w.customer_id,
	w.subscription_id,
    d as activity_date
  from win w,
  unnest(
  generate_date_array(
    
      w.start_date
    ,
    w.end_date
	)
  ) as d
),

-- Join to separate dimensions (1:1 per customer)
-- Inner join used here as the sample data is missing taxonomy/country for some customers

joined as (
  select
    d.activity_date,
    d.customer_id,
    sub.subscription_number,
    cc.customer_country,
    ct.taxonomy_business_category_group
  from days d
  JOIN `manuel-demo-1392926998`.`analytics`.`dim_customer_country`  cc using (customer_id)
  JOIN `manuel-demo-1392926998`.`analytics`.`dim_acquisition_taxonomy` ct using (customer_id)
  JOIN `manuel-demo-1392926998`.`analytics`.`dim_customer_subscription` sub USING (customer_id, subscription_id)
)



select
  j.activity_date,
  j.customer_id,
  1 as is_active,
  j.subscription_number,
  j.customer_country,
  j.taxonomy_business_category_group,
  
    timestamp('2025-09-05 08:06:25.325299+00:00') as created_at,
  
  timestamp('2025-09-05 08:06:25.325299+00:00') as last_updated_at
from joined j
