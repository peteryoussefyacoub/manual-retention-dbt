# MANUAL — Retention & Activity (dbt + BigQuery + Looker Studio)

This project models subscription **activity**, **cohorts**, **retention**, and **customer base movements** on BigQuery, and exposes them for easy analysis in Looker Studio.

- 📊 **Dashboard (Looker Studio):** [Open the report](https://lookerstudio.google.com/u/1/reporting/3087797f-c717-4b5d-816b-084e904127fd/page/mR7WF/edit)
- 🗄️ **Warehouse:** BigQuery (`manuel-demo-1392926998`, dataset `analytics`)
- 🧱 **Modeling:** dbt (partitioned tables, incremental where relevant)
- ✅ **Quality:** schema tests with partition-aware filters
- 📚 **Docs:** [dbt Docs (catalog + lineage)](https://peteryoussefyacoub.github.io/manual-retention-dbt/#!/overview/Dashboard)

---

## Contents

- [What’s delivered](#whats-delivered)
- [Project structure](#project-structure)
- [Quick start](#quick-start)
- [Models (business view)](#models-business-view)
- [Run only changed models](#run-only-changed-models)
- [Tests & guardrails](#tests--guardrails)
- [Docs (catalog + lineage)](#docs-catalog--lineage)
- [Looker Studio](#looker-studio)
- [Submission checklist](#submission-checklist)

---

## What’s delivered

**Core questions**
- How many customers are **active** by day/month?
- How do cohorts **retain** over time (monthly)?
- How many customers are **new / retained / churned** each month?
- Drilldowns by `customer_country` and `taxonomy_business_category_group`.

**Key concepts**
- **Cohort**: first-ever active date of a customer.
- **Retention**: customer is active at *t* days/months after cohort.
- **Churned (month)**: customer’s **last active month** equals that month.
- **Retained (base updates)**: active in month but not new in that month.

---

## Project structure

models/marts/
  
    dim_customer_country.sql
    dim_acquisition_taxonomy.sql
    dim_customer_cohort.sql
    fct_customer_active_daily.sql
    fct_customer_active_monthly.sql
    fct_cohort_retention_daily.sql
    fct_cohort_retention_monthly.sql
    fct_customer_base_updates_monthly.sql   # monthly new / existing / churned

  marts/schema.yml
  
  docs.md                                    # long-form docs blocks for dbt Docs


> Large facts are **partitioned** and (where relevant) **incremental**.

---

## Quick start

> Prereqs: dbt (BigQuery adapter) and a valid `profiles.yml` pointing to project `manuel-demo-1392926998` and dataset `analytics`.

### install/refresh packages
dbt deps

### build everything + tests
dbt build

### generate and view docs locally
dbt docs generate

dbt docs serve


## Models (business view)
### Dimensions
dim_customer_country — stable country per customer.

dim_acquisition_taxonomy — deterministic acquisition taxonomy per customer.

dim_customer_cohort — cohort_date, cohort_month, last_activity_month, plus slices.

### Facts
fct_customer_active_daily — customer-day actives (is_active = 1).

fct_customer_active_monthly — month roll-up; active_days_month, is_active_month.

fct_cohort_retention_daily — daily cohort retention with per-slice cohort_size.

fct_cohort_retention_monthly — monthly cohort retention with per-slice cohort_size.

fct_customer_base_updates_monthly — monthly movements:

## Run only changed models

### Create a baseline once:
dbt build

### save baseline manifest
mkdir -p .state/baseline

cp target/manifest.json .state/baseline/manifest.json


### Then during dev:
#### build changed nodes (and their children) vs baseline
dbt build --select state:modified+ --state .state/baseline --fail-fast

#### Update the baseline after a successful run:
cp target/manifest.json .state/baseline/manifest.json

## Tests & guardrails

Grain uniqueness on facts.

Non-null keys and partition fields within a configurable lookback window (var('test_lookback_days')).

Range checks (e.g., is_active = 1, days/months since cohort ≥ 0).

Consistency: in monthly movements, churned_customers ≤ new_customers + existing_customers.

### Run all:
dbt test

## Docs (catalog & lineage)

### Local:

dbt docs generate

dbt docs serve


### Publish via GitHub Pages:

#### copy static site from target/ to docs/
rm -rf docs

cp -R target docs

git add docs

git commit -m "Publish dbt docs site"

git push

#### GitHub → Settings → Pages → Source: main /docs

## Looker Studio

[Dashboard:](https://lookerstudio.google.com/u/1/reporting/3087797f-c717-4b5d-816b-084e904127fd/page/mR7WF/edit)

For each data source, set the Date range dimension to the table’s partition column.

Suggested controls: Metric selector, Country, Taxonomy, Min cohort size.

## Submission checklist

 dbt project builds on BigQuery (analytics dataset)
 
 Facts/dims documented & tested (schema.yml, docs.md)
 
 Looker Studio dashboard linked above
 
 dbt Docs site published via GitHub Pages
