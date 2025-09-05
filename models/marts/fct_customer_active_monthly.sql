-- ========================================================================================
-- MODEL: fct_customer_active_monthly
-- PURPOSE:
--   Monthly rollup of subscription-level activity. One row per
--   (customer_id, month_start) with:
--     - active_days = SUM(is_active) across the month (count of active days)
--     - is_active_month   = 1 if any day in the month is active
--   Stable slicing dims are carried through for convenience.
--
-- GRAIN:
--   1 row per (customer_id, month_start).
--
-- OUTPUT COLUMNS:
--   - month_start                       -- DATE_TRUNC(activity_date, MONTH)
--   - customer_id
--   - active_days                    -- SUM of daily is_active (0..31)
--   - is_active_month                      -- 1 if active_days > 0 else 0
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
--   - month_start = DATE_TRUNC(activity_date, MONTH).
--   - active_days = SUM(is_active) per (customer_id, month_start).
--   - is_active_month   = CASE WHEN active_days > 0 THEN 1 ELSE 0 END.
--
-- PERFORMANCE:
--   - Partition by month_start; cluster by (customer_id).
--   - Incremental INSERT OVERWRITE replaces only recent partitions (via lower bound).
--   - require_partition_filter=true to prevent accidental full scans.
--
-- DATA QUALITY:
--   - Uniqueness at the declared grain.
--   - active_days between 0 and 31; is_active_month ∈ {0,1}.
-- ========================================================================================

{{ config(
  materialized='incremental',
  partition_by={'field': 'month_start', 'data_type': 'date'},
  cluster_by=['customer_id'],
  incremental_strategy='insert_overwrite',
  unique_key=['customer_id','month_start'],
  persist_docs={'relation': true, 'columns': true},
  on_schema_change='sync_all_columns',
  tags=['marts','activity','monthly','subscription']
) }}

with bounds as (
  select
    date({{ current_date_sql() }}) as as_of,
    date_sub(date({{ current_date_sql() }}),
             interval {{ var('fct_active_lookback_days') }} day) as lower_bound
),
base as (
  select
    DATE_TRUNC(f.activity_date, MONTH) as month_start,
    f.customer_id,
    f.is_active,
    f.customer_country,
    f.taxonomy_business_category_group
  from {{ ref('fct_customer_active_daily') }} f
  where 1=1
  {% if is_incremental() %}
    and f.activity_date >= (select lower_bound from bounds)
  {% endif %}
),
agg as (
  select
    month_start,
    customer_id,
    sum(is_active) as active_days,
    any_value(customer_country) as customer_country,
    any_value(taxonomy_business_category_group) as taxonomy_business_category_group
  from base
  group by 1,2
)

{% if is_incremental() %}
, existing as (
  select customer_id, month_start, created_at
  from {{ this }}
  where month_start >= DATE_TRUNC((select lower_bound from bounds), MONTH)
)
{% endif %}

select
  a.month_start,
  a.customer_id,
  a.active_days,
  case when a.active_days > 0 then 1 else 0 end as is_active_month,
  a.customer_country,
  a.taxonomy_business_category_group,
  {% if is_incremental() %}
    coalesce(e.created_at, {{ timestamp_sql() }}) as created_at,
  {% else %}
    {{ timestamp_sql() }} as created_at,
  {% endif %}
  {{ timestamp_sql() }} as last_updated_at
from agg a
{% if is_incremental() %}
left join existing e
  on e.customer_id = a.customer_id
 and e.month_start = a.month_start
where a.month_start >= DATE_TRUNC((select lower_bound from bounds), MONTH)
{% endif %}     
