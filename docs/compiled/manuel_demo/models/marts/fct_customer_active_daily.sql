-- ========================================================================================
-- MODEL: fct_customer_active_daily
-- PURPOSE:
--   Subscription-level daily activity fact. Emits one row per (customer_id, 
--   activity_date) with a binary is_active=1 for each active day, plus stable slicing dims.
--
-- GRAIN:
--   1 row per (customer_id,  activity_date,customer_country,
--	 taxonomy_business_category_group).
--
-- OUTPUT COLUMNS:
--   - activity_date
--   - customer_id
--   - is_active                           -- always 1 when a row exists for that day
--   - customer_country
--   - taxonomy_business_category_group
--   - created_at
--   - last_updated_at
--
-- INPUTS:
--   - stg_activity(customer_id, subscription_id, start_date, end_date)
--   - dim_customer_country(customer_id → customer_country)
--   - dim_acquisition_taxonomy(customer_id → taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - Expand each window to days with GENERATE_DATE_ARRAY; on incremental runs, bound
--     the generated dates to the last 31 days.
--   - Join 1:1 dims AFTER explosion to avoid carrying dim columns through the UNNEST.
--   - Preserve created_at during insert_overwrite for existing rows in the incremental horizon.
--
-- ASSUMPTIONS:
--   - start_date <= end_date (enforced upstream).
--
-- PERFORMANCE:
--   - Partition: activity_date. Cluster: customer_id,  customer_country,
--     taxonomy_business_category_group.
--   - Incremental insert_overwrite on (customer_id,  activity_date).
--	 - A retention period of 9999 days is added to avoid uncontroled growth; however, number
--     should be configured for reasonable retention in a production environment.
--
-- DATA QUALITY:
--   - Uniqueness at the declared grain.
--   - Non-null keys/dates; is_active ∈ {1}; timestamps populated.
-- ========================================================================================



with win as (
  -- Cap open-ended intervals at date(CURRENT_DATE()); restrict to windows that intersect the horizon
  select 
    a.customer_id,
    a.start_date,
    coalesce(a.end_date, DATE(CURRENT_DATE())) as end_date
  from `manuel-demo-1392926998`.`analytics`.`stg_activity` a
  where a.start_date is not null
    and a.start_date <= coalesce(a.end_date, DATE(CURRENT_DATE()))
    
      and coalesce(a.end_date, DATE(CURRENT_DATE())) >= date_sub(date(CURRENT_DATE()), interval 31 day)
    
),
days as (
  -- Expand to daily grain and collapse concurrent subscriptions
  select distinct
    w.customer_id,
    d as activity_date
  from win w,
  unnest(
  generate_date_array(
    
      greatest(
        w.start_date,
        date_sub(date(CURRENT_DATE()),
                 interval 31 day)
      )
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
    cc.customer_country,
    ct.taxonomy_business_category_group
  from days d
  JOIN `manuel-demo-1392926998`.`analytics`.`dim_customer_country`  cc using (customer_id)
  JOIN `manuel-demo-1392926998`.`analytics`.`dim_acquisition_taxonomy` ct using (customer_id)
)


, existing as (
  -- Preserve created_at for already-present rows in the incremental window
  select customer_id,  activity_date, created_at
  from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_daily`
  where activity_date >= date_sub(date(CURRENT_DATE()), interval 31 day)
)


select
  j.activity_date,
  j.customer_id,
  1 as is_active,
  j.customer_country,
  j.taxonomy_business_category_group,
  
    coalesce(e.created_at, CURRENT_TIMESTAMP()) as created_at,
  
  CURRENT_TIMESTAMP() as last_updated_at
from joined j

left join existing e
  on e.customer_id = j.customer_id
 and e.activity_date = j.activity_date
where j.activity_date >= date_sub(date(CURRENT_DATE()), interval 31 day)
