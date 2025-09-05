
  
    

    create or replace table `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_weekly`
      
    partition by cohort_week_start
    cluster by customer_country, taxonomy_business_category_group, weeks_since_cohort

    
    OPTIONS(
      description="""Weekly retention counts by cohort and slices using the weekly rollup. A customer is retained in a given week if any subscription is active that week.\n"""
    )
    as (
      -- ========================================================================================
-- MODEL: fct_cohort_retention_weekly
-- PURPOSE:
--   Weekly retention counts by cohort and slices using the weekly rollup:
--   (cohort_week_start, weeks_since_cohort, customer_country, taxonomy_business_category_group)
--   → active_customers, with per-slice cohort_size. Also exposes an ISO week label.
--
-- GRAIN:
--   1 row per (cohort_week_start, weeks_since_cohort, customer_country, taxonomy_business_category_group).
--
-- OUTPUT COLUMNS:
--   - cohort_week_start                     -- DATE (week start, Monday)
--   - cohort_week                           -- STRING ISO label '%G-%V' for display (e.g., '2025-36')
--   - weeks_since_cohort
--   - customer_country
--   - taxonomy_business_category_group
--   - cohort_size                           -- per-slice cohort size from dim_customer_cohort
--   - active_customers                      -- distinct customers active in that week_since_cohort
--   - total_active_days		   			 -- total active days of customers in this cohort on grain level.

-- INPUTS:
--   - fct_customer_active_weekly(customer_id, week_start, is_active_week,
--       customer_country, taxonomy_business_category_group)
--   - dim_customer_cohort(customer_id, cohort_week_start, customer_country, taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - week_start = week_start from fct_customer_active_weekly (Monday-start).
--   - Aggregate to customer-week with MAX(is_active_week) across subscriptions.
--   - weeks_since_cohort = DATE_DIFF(week_start, cohort_week_start, WEEK(MONDAY)).
--   - Keep rows where week_start >= cohort_week_start; count DISTINCT customer_id per
--     (cohort_week_start, weeks_since_cohort, customer_country, taxonomy_business_category_group).
--   - Join per-slice cohort_size from dim_customer_cohort on (cohort_week_start, country, taxonomy).
--   - cohort_week is a display label only (ISO year-week as STRING).
--
-- ASSUMPTIONS:
--   - Each customer appears once in dim_customer_cohort; slice attributes are stable there.
--   - weeks_since_cohort ≥ 0 (pre-cohort activity is excluded).
--
-- PERFORMANCE:
--   - Materialized as TABLE; partitioned by cohort_week_start; clustered by
--     customer_country, taxonomy_business_category_group, weeks_since_cohort.
--   - Consider require_partition_filter=true to avoid accidental full scans.
--
-- DATA QUALITY:
--   - Uniqueness at declared grain.
--   - 0 ≤ active_customers; weeks_since_cohort ≥ 0; cohort_size ≥ 0.
-- ========================================================================================



-- 1) Bring in weekly rollup and attach cohort week
with base as (
  select
    w.customer_id,
    w.week_start,                     				 		 -- Monday-start week
    d.cohort_week_start,                                     -- Monday-start cohort week
    -- Collapse subscription-level weekly rows to customer-week activity (any sub active => active)
    w.is_active_week,
    w.customer_country,
    w.taxonomy_business_category_group,
	MAX(w.active_days) AS active_days
  from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_weekly` w
  join `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort` d using (customer_id)
  group by 1,2,3,4,5,6
),

-- 2) Per-slice cohort sizes (denominator)
c_size as (
  select
    cohort_week_start,
    customer_country,
    taxonomy_business_category_group,
    count(distinct customer_id) as cohort_size
  from `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort`
  group by 1,2,3
),

-- 3) Retained customers per week-since-cohort and slice
prep as (
  select
    cohort_week_start,
    date_diff(week_start, cohort_week_start, WEEK(MONDAY)) as weeks_since_cohort,
    customer_country,
    taxonomy_business_category_group,
    count(distinct customer_id) as active_customers,
	SUM(active_days) as total_active_days
  from base
  where week_start >= cohort_week_start
    and is_active_week = 1
  group by 1,2,3,4
)

-- 4) Final output with ISO week label
select
  p.cohort_week_start,
  format_date('%G-%V', p.cohort_week_start) as cohort_week,
  p.weeks_since_cohort,
  p.customer_country,
  p.taxonomy_business_category_group,
  c.cohort_size,
  p.active_customers,
  p.total_active_days
from prep p
join c_size c
  using (cohort_week_start, customer_country, taxonomy_business_category_group)
    );
  