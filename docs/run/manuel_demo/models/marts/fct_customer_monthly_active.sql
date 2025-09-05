
  
    

    create or replace table `manuel-demo-1392926998`.`analytics`.`fct_customer_monthly_active`
      
    partition by activity_month
    cluster by customer_id, customer_country, taxonomy_business_category_group

    
    OPTIONS(
      description="""One row per customer_id \u00d7 activity_month indicating monthly activity. Multiple concurrent subscriptions are collapsed to a single is_active=1 while preserving a count of active_subscriptions for diagnostics. Grain: (customer_id, activity_month).\n"""
    )
    as (
      -- FACT: monthly customer activity; 1 row per (customer_id, activity_month).
-- Multiple parallel subscriptions collapse to is_active = 1; we keep a count.



-- ========================================================================================
-- MODEL: fct_customer_monthly_active
-- PURPOSE:
--   One row per (customer_id, activity_month) indicating whether the customer was
--   active that month (is_active = 1). Multiple concurrent subscriptions are collapsed,
--   but we retain a count in active_subscriptions for diagnostics.
--
-- GRAIN:
--   1 row per customer_id, activity_month.
--
-- INPUTS:
--   - `manuel-demo-1392926998`.`analytics`.`stg_activity`      (customer_id, subscription_id, start_date, end_date)
--   - `manuel-demo-1392926998`.`analytics`.`stg_customers`     (customer_id, customer_country)
--   - `manuel-demo-1392926998`.`analytics`.`stg_acq_orders`    (customer_id, taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - Expand subscription windows to daily dates, bucket to month, then collapse to 1 row
--     per customer-month. (We count distinct subscription_id for reference; "active"
--     is binary regardless of count.)
--
-- ASSUMPTIONS:
--   - start_date is <= end_date (staging/tests enforce).
--   - Open-ended windows use end_date IS NULL; we treat them as active through CURRENT_DATE().
--
-- PERFORMANCE:
--   - Partition by activity_month for typical date filters.
--   - Cluster by customer_id and common slice dims.
--   - Incremental runs restrict to a recent horizon to minimize reprocessing.
-- ========================================================================================

with win as (
  select
    customer_id,
    subscription_id,
    start_date,
    coalesce(end_date, DATE('2025-09-03 09:27:52.648659+00:00')) as end_date
  from `manuel-demo-1392926998`.`analytics`.`stg_activity`
  where start_date is not null
),

-- Expand windows to days, then bucket by month and dedupe to customer-month
months as (
  select
    w.customer_id,
    date_trunc(d, month) as activity_month,
    count(distinct w.subscription_id) as active_subscriptions
  from win w,
  unnest(generate_date_array(w.start_date, w.end_date)) as d
  group by 1, 2
),

joined as (
  select
    m.activity_month,
    m.customer_id,
    1 as is_active,  -- binary flag (active this month)
    m.active_subscriptions,
    c.customer_country,
    a.taxonomy_business_category_group
  from months m
  left join `manuel-demo-1392926998`.`analytics`.`stg_customers`  c using (customer_id)
  left join `manuel-demo-1392926998`.`analytics`.`stg_acq_orders` a using (customer_id)
)

select *
from joined

    );
  