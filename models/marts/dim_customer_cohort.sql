-- ========================================================================================
-- MODEL: dim_customer_cohort
-- PURPOSE:
--   One row per customer capturing their first-ever active date (cohort_date) and its
--   week bucket (cohort_week), month bucket (cohort_month), plus stable slicing attributes
--	 (country, taxonomy).
--
-- GRAIN:
--   1 row per customer_id.
--
-- OUTPUT COLUMNS:
--   - customer_id
--   - cohort_month                         -- DATE_TRUNC(cohort_date, MONTH)
--   - cohort_date                          -- MIN(start_date) from activity windows
--   - customer_country                     -- deterministic pick from dim_customer_country
--   - taxonomy_business_category_group     -- deterministic pick from dim_acquisition_taxonomy
--
-- INPUTS:
--   - stg_activity(customer_id, start_date)
--   - dim_customer_country(customer_id, customer_country)
--   - dim_acquisition_taxonomy(customer_id, taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - Compute cohort_date as the earliest observed start_date per customer_id.
--   - Derive cohort_month = DATE_TRUNC(cohort_date, MONTH).
--
-- ASSUMPTIONS:
--   - start_date is inclusive and present for first-activity determination.
--   - Customers may have multiple customer_country/taxonomy rows; model resolves to
--     a single, stable value per customer.
--
-- PERFORMANCE:
--   - Small dimensional table; single pass aggregations over staging inputs.
--   - Materialized as TABLE for stable downstream joins; partitioned by cohort_month.
--
-- DATA QUALITY:
--   - Exactly one row per customer_id.
--   - cohort_date <= DATE({{ current_date_sql() }})
--	 - cohort_month = DATE_TRUNC(cohort_date, MONTH).
-- ========================================================================================

{{ config(
  materialized = 'table',
  partition_by = {'field': 'cohort_month', 'data_type': 'date'},
  cluster_by   = ['customer_id', 'customer_country', 'taxonomy_business_category_group'],
  persist_docs = {'relation': true, 'columns': true},
  on_schema_change = 'sync_all_columns',
  tags = ['marts', 'cohort', 'dimensional']
) }}

/* 1) Keep only valid activity rows (defensive filtering).
      If business allows future-dated activations, relax the CURRENT_DATE() predicate. */
with valid_activity as (
  select
    customer_id,
    start_date
  from {{ ref('stg_activity') }}
  where start_date is not null
    and start_date <= DATE({{ current_date_sql() }})
),

/* 2) First time a customer was active = cohort_date. */
first_seen as (
  select
    customer_id,
    min(start_date) as cohort_date
  from valid_activity
  group by customer_id
)

/* 3) Final dimensional row: 1 row per customer_id with cohort & descriptors. */
select
  fs.customer_id,
  date_trunc(fs.cohort_date, month) as cohort_month,
  date_trunc(fs.cohort_date, week) as cohort_week_start,
  fs.cohort_date,
  ca.customer_country,
  tc.taxonomy_business_category_group
from first_seen fs
left join {{ ref('dim_customer_country') }}  ca using (customer_id)
left join {{ ref('dim_acquisition_taxonomy') }} tc using (customer_id)
