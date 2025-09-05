#########################################################################################
## Snapshot: customers_snapshot
## Purpose: Maintains a historical view of customer attributes
## (e.g., country).
## Why: Customer details may be corrected or updated (address change,
## country code standardization, GDPR-driven changes).
## Strategy: Check strategy on all columns to capture any correction.
#########################################################################################

{% snapshot customers_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['customer_country']
) }}

select
  customer_id,
  customer_country
from {{ ref('stg_customers') }}

{% endsnapshot %}
