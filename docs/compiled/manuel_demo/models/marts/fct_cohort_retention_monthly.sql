-- ========================================================================================
-- MODEL: fct_cohort_retention_monthly
-- PURPOSE:
--   Monthly retention counts by cohort and slices using the monthly rollup:
--   (cohort_month, months_since_cohort, customer_country, taxonomy_business_category_group)
--   → active_customers, with per-slice cohort_size.
--
-- GRAIN:
--   1 row per (cohort_month, months_since_cohort, customer_country, taxonomy_business_category_group).
--
-- OUTPUT COLUMNS:
--   - cohort_month
--   - months_since_cohort
--   - customer_country
--   - taxonomy_business_category_group
--   - cohort_size                 -- per-slice cohort size from dim_customer_cohort
--   - active_customers            -- distinct customers active in that month_since_cohort
--   - total_active_days		   -- total active days of customers in this cohort on grain level.
--
-- INPUTS:
--   - fct_customer_active_monthly(customer_id, month_start, is_active_month,
--       customer_country, taxonomy_business_category_group)
--   - dim_customer_cohort(customer_id, cohort_month, customer_country, taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - month_start = month_start from fct_customer_active_monthly.
--   - Aggregate to customer-month with MAX(is_active_month) across subscriptions.
--   - months_since_cohort = DATE_DIFF(month_start, cohort_month, MONTH).
--   - Keep rows where month_start >= cohort_month; count DISTINCT customer_id per
--     (cohort_month, months_since_cohort, customer_country, taxonomy_business_category_group).
--   - Join per-slice cohort_size from dim_customer_cohort on (cohort_month, country, taxonomy).
--
-- ASSUMPTIONS:
--   - Each customer appears once in dim_customer_cohort; slice attributes are stable there.
--   - months_since_cohort ≥ 0 (pre-cohort activity is excluded).
--
-- PERFORMANCE:
--   - Materialized as TABLE; partitioned by cohort_month; clustered by
--     customer_country, taxonomy_business_category_group, months_since_cohort.
--   - require_partition_filter=true to avoid accidental full scans.
--
-- DATA QUALITY:
--   - Uniqueness at declared grain.
--   - 0 ≤ active_customers; months_since_cohort ≥ 0; cohort_size ≥ 0.
-- ========================================================================================



-- 1) Bring in monthly rollup and attach cohort month
with base as (
  select
    m.customer_id,
    m.month_start,
    d.cohort_month,
    m.is_active_month,
    m.customer_country,
    m.taxonomy_business_category_group,
	MAX(m.active_days) AS active_days
  from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly` m
  join `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort` d using (customer_id)
  group by 1,2,3,4,5,6
),

-- 2) Per-slice cohort sizes (denominator)
c_size as (
  select
    cohort_month,
    customer_country,
    taxonomy_business_category_group,
    count(distinct customer_id) as cohort_size
  from `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort`
  group by 1,2,3
),

-- 3) Retained customers per months-since-cohort and slice
prep as (
  select
    cohort_month,
    date_diff(month_start, cohort_month, month) as months_since_cohort,
    customer_country,
    taxonomy_business_category_group,
    count(distinct customer_id) as active_customers,
	SUM(active_days) as total_active_days
  from base
  where month_start >= cohort_month
    and is_active_month = 1
  group by 1,2,3,4
)

-- 4) Final output
select
  p.cohort_month,
  p.months_since_cohort,
  p.customer_country,
  p.taxonomy_business_category_group,
  c.cohort_size,
  p.active_customers,
  p.total_active_days
from prep p
join c_size c
  using (cohort_month, customer_country, taxonomy_business_category_group)