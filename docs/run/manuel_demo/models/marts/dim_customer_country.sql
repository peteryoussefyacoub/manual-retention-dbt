
  
    

    create or replace table `manuel-demo-1392926998`.`analytics`.`dim_customer_country`
      
    
    

    
    OPTIONS(
      description="""One row per customer with a stable country value (uppercased/trimmed).\n"""
    )
    as (
      -- ========================================================================================
-- MODEL: dim_customer_country
-- PURPOSE:
--   One row per customer with a stable country value chosen deterministically from
--   stg_customers.
--
-- GRAIN:
--   1 row per customer_id present in stg_customers.
--
-- OUTPUT COLUMNS:
--   - customer_id
--   - customer_country               -- alphabetically-first non-null value observed
--
-- INPUTS:
--   - stg_customers(customer_id, customer_country)
--
-- BUSINESS LOGIC:
--   - For each customer_id, pick the alphabetically-first non-null customer_country using
--     ARRAY_AGG(... ORDER BY customer_country)[OFFSET(0)] IGNORE NULLS.
--
-- ASSUMPTIONS:
--   - A customer may appear multiple times in stg_customers, for example if they change country;
--	   this model selects one stable representative value per customer_id.
--   - customer_country may be NULL if no non-null values exist for that customer.
--
-- PERFORMANCE:
--   - Small dimension; materialized as TABLE for stable downstream joins.
--   - Single GROUP BY over stg_customers; no partitioning required.
--
-- DATA QUALITY:
--   - Exactly one row per customer_id (enforced by GROUP BY).
--   - customer_id is NOT NULL; customer_country may be NULL.
-- ========================================================================================



-- One row per customer with a stable country value
select
  customer_id,
  -- If a customer appears more than once, pick a stable representative
  -- IF a timestamp exist, we can pick-up the latest
  ARRAY_AGG(customer_country IGNORE NULLS
            ORDER BY customer_country)[OFFSET(0)]
  as customer_country
from `manuel-demo-1392926998`.`analytics`.`stg_customers`
group by customer_id
    );
  