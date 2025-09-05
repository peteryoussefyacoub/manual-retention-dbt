{% docs fct_customer_active_daily__overview %}
**Grain:** 1 row per (customer_id, activity_date).  
`is_active = 1` means the customer was active on that calendar day.  
Partitioned by `activity_date`; used by monthly roll-up and cohort retention.
{% enddocs %}

{% docs fct_customer_active_monthly__overview %}
**Grain:** 1 row per (customer_id, month_start).  
`active_days_month` = sum of daily actives in the month; `is_active_month` = 1 if any day active.  
Partitioned by `month_start`.
{% enddocs %}

{% docs fct_cohort_retention_daily__overview %}
**Grain:** 1 row per (cohort_date, days_since_cohort, customer_country, taxonomy_business_category_group).  
`active_customers` = distinct customers active on `cohort_date + t`; includes per-slice `cohort_size`.
{% enddocs %}

{% docs fct_cohort_retention_monthly__overview %}
**Grain:** 1 row per (cohort_month, months_since_cohort, customer_country, taxonomy_business_category_group).  
Distinct active customers per month-since-cohort + cohort_size; also sums total active days.
{% enddocs %}

{% docs fct_customer_base_updates_monthly__overview %}
**Grain:** 1 row per calendar month.  
- **new_customers**: `cohort_month = month`  
- **retained_customers**: active in month but not new (`active - new`)  
- **churned_customers**: `last_activity_month = month` (final active month).
{% enddocs %}

{% docs dim_customer_cohort__overview %}
One row per customer with `cohort_date`, `cohort_month`, `last_activity_month`, and slice attributes (country, taxonomy).
{% enddocs %}

{% docs dim_customer_country__overview %}
One row per customer with a deterministic `customer_country` (alphabetically-first non-null).
{% enddocs %}

{% docs dim_acquisition_taxonomy__overview %}
One row per customer with deterministic `taxonomy_business_category_group` for acquisition analysis.
{% enddocs %}
