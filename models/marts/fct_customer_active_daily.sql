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
--     the generated dates to the last {{ var('fct_active_lookback_days') }} days.
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

{{ config(
  materialized='incremental',
  partition_by={'field': 'activity_date', 'data_type': 'date'},
  cluster_by=['customer_id'],
  incremental_strategy='insert_overwrite',
  unique_key=['customer_id','activity_date'],
  persist_docs={'relation': true, 'columns': true},
  on_schema_change='sync_all_columns',
  post_hook=[
    "ALTER TABLE {{ this }} SET OPTIONS (partition_expiration_days = {{ var('activity_partition_ttl_days', 9999) }})"
  ],
  tags=['marts','cohort','activity','daily']
) }}

with win as (
  -- Cap open-ended intervals at date({{ current_date_sql() }}); restrict to windows that intersect the horizon
  select 
    a.customer_id,
    a.start_date,
    coalesce(a.end_date, DATE({{ current_date_sql() }})) as end_date
  from {{ ref('stg_activity') }} a
  where a.start_date is not null
    and a.start_date <= coalesce(a.end_date, DATE({{ current_date_sql() }}))
    {% if is_incremental() %}
      and coalesce(a.end_date, DATE({{ current_date_sql() }})) >= date_sub(date({{ current_date_sql() }}), interval {{ var('fct_active_lookback_days') }} day)
    {% endif %}
),
days as (
  -- Expand to daily grain and collapse concurrent subscriptions
  select distinct
    w.customer_id,
    d as activity_date
  from win w,
  unnest(
  generate_date_array(
    {% if is_incremental() %}
      greatest(
        w.start_date,
        date_sub(date({{ current_date_sql() }}),
                 interval {{ var('fct_active_lookback_days') }} day)
      )
    {% else %}
      w.start_date
    {% endif %},
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
  JOIN {{ ref('dim_customer_country') }}  cc using (customer_id)
  JOIN {{ ref('dim_acquisition_taxonomy') }} ct using (customer_id)
)

{% if is_incremental() %}
, existing as (
  -- Preserve created_at for already-present rows in the incremental window
  select customer_id,  activity_date, created_at
  from {{ this }}
  where activity_date >= date_sub(date({{ current_date_sql() }}), interval {{ var('fct_active_lookback_days') }} day)
)
{% endif %}

select
  j.activity_date,
  j.customer_id,
  1 as is_active,
  j.customer_country,
  j.taxonomy_business_category_group,
  {% if is_incremental() %}
    coalesce(e.created_at, {{ timestamp_sql() }}) as created_at,
  {% else %}
    {{ timestamp_sql() }} as created_at,
  {% endif %}
  {{ timestamp_sql() }} as last_updated_at
from joined j
{% if is_incremental() %}
left join existing e
  on e.customer_id = j.customer_id
 and e.activity_date = j.activity_date
where j.activity_date >= date_sub(date({{ current_date_sql() }}), interval {{ var('fct_active_lookback_days') }} day)
{% endif %}