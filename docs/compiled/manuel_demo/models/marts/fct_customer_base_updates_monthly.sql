-- ========================================================================================
-- MODEL: fct_customer_base_updates_monthly
-- PURPOSE:
--   Monthly customer movement metrics:
--     - new_customers:     customers whose cohort_month equals the month
--     - retained_customers:active in the month but did NOT start that month
--     - churned_customers: customers whose last_activity_month equals the month
--
-- GRAIN:
--   1 row per month_start (DATE, first day of month).
--
-- OUTPUT COLUMNS:
--   - month            -- DATE_TRUNC(..., MONTH)
--   - new_customers
--   - retained_customers
--   - churned_customers
--
-- INPUTS:
--   - dim_customer_cohort(customer_id, cohort_month, last_activity_month)
--   - fct_customer_active_monthly(customer_id, month_start, is_active_month)
--
-- NOTES:
--   - Categories are not mutually exclusive: churned_customers were also active
--     in that month (their final month). retained_customers = active - new.
--   - Bounded by a lookback window for performance. Override with vars as needed.
-- ========================================================================================





with
-- New customers = first-time starters in the month
new_by_month as (
  select
    cohort_month as month,
    count(distinct customer_id) as new_customers
  from `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort`
  group by 1
),

-- Churned customers = last activity falls in the month
churned_by_month as (
  select
    last_activity_month as month,
    count(distinct customer_id) as churned_customers
  from
	(SELECT
		customer_id,
		max(month_start) as last_activity_month
	from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly`
	group by 1)
  group by 1
),

-- Active customers in the month (customer-level, across all subscriptions)
active_by_month as (
  select
    month_start as month,
    count(distinct customer_id) as active_customers
    from `manuel-demo-1392926998`.`analytics`.`fct_customer_active_monthly`
  group by 1
),

-- Months present in any component
months as (
  select month from new_by_month
  union distinct
  select month from churned_by_month
  union distinct
  select month from active_by_month
)

select
  m.month,
  coalesce(n.new_customers, 0) as new_customers,
  greatest(coalesce(a.active_customers, 0) - coalesce(n.new_customers, 0), 0) as retained_customers,
  coalesce(c.churned_customers, 0) as churned_customers
from months m
left join new_by_month     n using (month)
left join active_by_month  a using (month)
left join churned_by_month c using (month)