#########################################################################################
## Snapshot: acq_orders_snapshot
## Purpose: Tracks acquisition taxonomy (business category group) per customer.
## Why: Business taxonomy assignments can evolve over time (reclassification,
## category migration).
## Strategy: Check strategy on taxonomy columns to preserve changes.
#########################################################################################

{% snapshot acq_orders_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['taxonomy_business_category_group']
) }}

select
  customer_id,
  taxonomy_business_category_group
from {{ ref('stg_acq_orders') }}

{% endsnapshot %}
