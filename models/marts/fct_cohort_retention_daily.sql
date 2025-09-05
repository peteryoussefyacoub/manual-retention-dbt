-- ========================================================================================
-- MODEL: fct_cohort_retention_daily
-- PURPOSE:
--   Long-format daily retention counts by cohort and slices:
--   (cohort_date, days_since_cohort, customer_country, taxonomy_business_category_group)
--   → active_customers, with per-slice cohort_size.
--
-- GRAIN:
--   1 row per (cohort_date, days_since_cohort, customer_country, taxonomy_business_category_group).
--
-- OUTPUT COLUMNS:
--   - cohort_date
--   - days_since_cohort
--   - customer_country
--   - taxonomy_business_category_group
--   - cohort_size                 -- per-slice cohort size from dim_customer_cohort
--   - active_customers            -- distinct customers active on cohort_date + days_since_cohort
--
-- INPUTS:
--   - fct_customer_active_daily(customer_id, activity_date, customer_country, taxonomy_business_category_group)
--   - dim_customer_cohort(customer_id, cohort_date, customer_country, taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - Join daily activity to each customer's cohort_date, keep rows where activity_date >= cohort_date.
--   - active_customers: COUNT(DISTINCT customer_id) per (cohort_date, t=days_since_cohort, slices).
--   - cohort_size: COUNT(DISTINCT customer_id) per (cohort_date, slices) from dim_customer_cohort.
--
-- ASSUMPTIONS:
--   - Each customer appears once in dim_customer_cohort; slice attributes are stable there.
--   - We assume that every activity is asssociated with a subscription.
--	 - We count customer as active despite any period of inactivity between two active days.
--
-- PERFORMANCE:
--   - Materialized as TABLE, partitioned by cohort_date and clustered by
--     customer_country, taxonomy_business_category_group, days_since_cohort for pruning.
--   - Consider require_partition_filter=true to prevent accidental full scans.
--
-- DATA QUALITY:
--   - Uniqueness at declared grain.
--   - cohort_size is non-null; 0 ≤ active_customers ≤ cohort_size; days_since_cohort ≥ 0.
-- ========================================================================================

{{ config(
  materialized='table',
  partition_by={'field': 'cohort_date', 'data_type': 'date'},
  cluster_by=['customer_country','taxonomy_business_category_group','days_since_cohort'],
  persist_docs={'relation': true, 'columns': true},
  on_schema_change='sync_all_columns',
  tags=['marts','cohort','retention','daily']
) }}

with base as (
  select
    f.customer_id,
    f.activity_date,
    d.cohort_date,
    f.customer_country,
    f.taxonomy_business_category_group
  from {{ ref('fct_customer_active_daily') }} f
  join {{ ref('dim_customer_cohort') }} d using (customer_id)
),
c_size as (
  select
    cohort_date,
    customer_country,
    taxonomy_business_category_group,
    count(distinct customer_id) as cohort_size
  from {{ ref('dim_customer_cohort') }}
  group by 1,2,3
),
prep as (
  select
    cohort_date,
    date_diff(activity_date, cohort_date, day) as days_since_cohort,
    customer_country,
    taxonomy_business_category_group,
    count(distinct customer_id) as active_customers
  from base
  where activity_date >= cohort_date
  group by 1,2,3,4
)
select
  p.cohort_date,
  p.days_since_cohort,
  p.customer_country,
  p.taxonomy_business_category_group,
  c.cohort_size,
  p.active_customers
from prep p
	JOIN c_size c USING (cohort_date, customer_country, taxonomy_business_category_group)