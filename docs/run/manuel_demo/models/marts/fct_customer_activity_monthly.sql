
  
    

    create or replace table `manuel-demo-1392926998`.`analytics`.`fct_customer_activity_monthly`
      
    partition by activity_month
    cluster by customer_id, customer_country, taxonomy_business_category_group

    
    OPTIONS(
      description=""""""
    )
    as (
      

with win as (
  select customer_id,
         coalesce(start_date, date '1970-01-01') as start_date,
         coalesce(end_date, DATE('2025-09-03 09:27:52.648659+00:00'))      as end_date
  from `manuel-demo-1392926998`.`analytics`.`stg_activity`
  where start_date is not null
),
expanded as (
  select
    w.customer_id,
    date_trunc(d, month) as activity_month
  from win w, unnest(generate_date_array(w.start_date, w.end_date)) as d
),
joined as (
  select
    e.customer_id,
    e.activity_month,
    c.customer_country,
    a.taxonomy_business_category_group
  from expanded e
  left join `manuel-demo-1392926998`.`analytics`.`dim_customer_country`            c using (customer_id)
  left join `manuel-demo-1392926998`.`analytics`.`dim_acquisition_taxonomy` a using (customer_id)
),
cohorted as (
  select
    j.*,
    min(j.activity_month) over (partition by j.customer_id) as cohort_month,
    date_diff(j.activity_month,
              min(j.activity_month) over (partition by j.customer_id),
              month) as months_since_cohort
  from joined j
)
select * from cohorted
    );
  