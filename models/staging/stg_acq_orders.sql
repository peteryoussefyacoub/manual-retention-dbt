{{ config(materialized='view') }}

select
  cast(customer_id as string) as customer_id,
  nullif(trim(taxonomy_business_category_group), '') as taxonomy_business_category_group
from {{ source('raw','acq_orders') }}
