

  create or replace view `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_rates_monthly`
  OPTIONS(
      description="""Retention counts joined with cohort sizes to compute retention_rate (0..1) for each cohort, months_since_cohort, and slice combination.\n"""
    )
  as -- ========================================================================================
-- MODEL: fct_cohort_retention_rates_daily
-- PURPOSE:
--   Daily retention counts joined to cohort sizes with a computed retention_rate (0..1).
--
-- GRAIN:
--   1 row per (cohort_date, days_since_cohort, customer_country, taxonomy_group).
-- ========================================================================================



with cohort_sizes as (
  select
    cohort_date,
    cohort_month,
    customer_country,
    taxonomy_business_category_group,
    count(distinct customer_id) as cohort_size
  from `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort`
  group by 1,2,3,4
)
select
  r.cohort_date,
  r.cohort_month,
  r.days_since_cohort,
  r.customer_country,
  r.taxonomy_business_category_group,
  r.active_customers,
  c.cohort_size,
  safe_divide(r.active_customers, c.cohort_size) as retention_rate
from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_daily` r
join cohort_sizes c
  using (cohort_date, cohort_month, customer_country, taxonomy_business_category_group);

