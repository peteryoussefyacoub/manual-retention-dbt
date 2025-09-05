
  
    

    create or replace table `manuel-demo-1392926998`.`analytics`.`fct_retention_monthly`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      -- ========================================================================================
-- MODEL: fct_retention_daily
-- PURPOSE:
--   Unsliced daily cohort retention. Provides cohort_size, retained_customers,
--   and retention_rate by (cohort_date, days_since_cohort).
--   Sourced from fct_cohort_retention_daily to keep a single counting logic.
--
-- GRAIN:
--   1 row per (cohort_date, days_since_cohort).
--
-- INPUTS:
--   - fct_cohort_retention_daily(cohort_date, days_since_cohort, active_customers)
--   - dim_customer_cohort(cohort_date, customer_id)
--
-- BUSINESS LOGIC:
--   - retained_customers = SUM(active_customers) for each (cohort_date, days_since_cohort)
--   - cohort_size       = COUNT(DISTINCT customer_id) per cohort_date
--   - retention_rate    = SAFE_DIVIDE(retained_customers, cohort_size)
--
-- ASSUMPTIONS:
--   - fct_cohort_retention_daily is distinct by customer_id at a given day_since_cohort.
--   - dim_customer_cohort contains exactly one row per customer_id with its cohort_date.
--
-- PERFORMANCE:
--   - Materialized as a VIEW (compute on read). If heavy in prod, consider a TABLE
--     partitioned by cohort_date and clustered by days_since_cohort.
--
-- DATA QUALITY:
--   - See schema.yml: non-null keys and retention_rate BETWEEN 0 AND 1.
-- ========================================================================================



with r as (
  select
    cohort_date,
    days_since_cohort,
    sum(active_customers) as retained_customers
  from `manuel-demo-1392926998`.`analytics`.`fct_cohort_retention_daily`
  group by 1,2
),
c as (
  select
    cohort_date,
    count(distinct customer_id) as cohort_size
  from `manuel-demo-1392926998`.`analytics`.`dim_customer_cohort`
  group by 1
)
select
  r.cohort_date,
  r.days_since_cohort,
  c.cohort_size,
  r.retained_customers,
  safe_divide(r.retained_customers, c.cohort_size) as retention_rate
from r
join c using (cohort_date)
order by r.cohort_date, r.days_since_cohort
    );
  