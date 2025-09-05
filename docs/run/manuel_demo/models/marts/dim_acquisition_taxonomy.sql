
  
    

    create or replace table `manuel-demo-1392926998`.`analytics`.`dim_acquisition_taxonomy`
      
    
    

    
    OPTIONS(
      description="""One row per customer with a deterministic acquisition taxonomy assignment.\n"""
    )
    as (
      -- ========================================================================================
-- MODEL: dim_acquisition_taxonomy
-- PURPOSE:
--   One row per customer with a deterministic acquisition taxonomy assignment.
--   Picks the alphabetically-first non-null taxonomy value observed in stg_acq_orders.
--
-- GRAIN:
--   1 row per customer_id present in stg_acq_orders.
--
-- OUTPUT COLUMNS:
--   - customer_id
--   - taxonomy_business_category_group
--
-- INPUTS:
--   - stg_acq_orders(customer_id, taxonomy_business_category_group)
--
-- BUSINESS LOGIC:
--   - For each customer_id, select once taxonomy_business_category_group. This is a
--     stable, deterministic choice when there are multiple categories per customer.
--	 - If a timestamp becomes available, it could be used to select a different taxonomy.
--
-- ASSUMPTIONS:
--	 - We only have the aqusition taxonomy, which means it will be fixed throught the custonmer's
--	   lifecycle even if they change to a different toxonomy later.
--   - Only customers that appear in stg_acq_orders are included (no outer join to all customers).
--   - taxonomy_business_category_group may be NULL if all observed values are NULL.
--
-- PERFORMANCE:
--   - Small dimension; materialized as TABLE for stable downstream joins.
--   - No partitioning; GROUP BY on stg_acq_orders only.
--
-- DATA QUALITY:
--   - Exactly one row per customer_id (as produced by GROUP BY).
--   - customer_id is NOT NULL; taxonomy_business_category_group may be NULL.
-- ========================================================================================



-- One row per customer with a taxonomy assignment
-- If later we have order timestamps, switch to “latest taxonomy” logic,
-- or better match activity with active subscription with relevant toxonomy.
select
  customer_id,
  ARRAY_AGG(taxonomy_business_category_group IGNORE NULLS
            ORDER BY taxonomy_business_category_group)[OFFSET(0)]
    as taxonomy_business_category_group
from `manuel-demo-1392926998`.`analytics`.`stg_acq_orders`
group by customer_id
    );
  