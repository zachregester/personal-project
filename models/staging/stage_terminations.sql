{{ config(materialized='ephemeral')}}

with base as (

    select employee_id

        ,  (case when year(period_date) < 100 then dateadd(year, 2000, period_date)
                 else period_date
                 end) as period_date

        ,  date_trunc('quarter', period_date_clean) as beg_of_quarter

        ,  (case when year(termination_date) < 100
                 then dateadd(year, 2000, termination_date)
                 else termination_date end) as termination_date

        ,  initcap(lower(termination_type)) as termination_type

        ,  (case when termination_reason is null then 'Other'
                 else termination_reason end) as termination_reason

        ,  (case when metric ilike 'Term%' then 'Termination'
                 else initcap(lower(metric)) end)
                 as metric

        ,  initcap(lower(job_name)) as job_name

        ,  job_name_code as job_code

        ,  concat(job_name_clean, ' - ', job_name_code) as job_description

        ,  (case when employee_id like '999%' and length(employee_id) >= 5 then 'true'
                 when employee_id in ('10256','10257', '10258') and period_date = '2025-08-01' then 'true'
                 else upper(try_to_boolean(is_contractor)) end) as is_contractor

        ,  exec_leader
        ,  l1_from_top_leader
        ,  l2_from_top_leader
        ,  l3_from_top_leader

    from {{ source('source','source_terminations')}}

)

select distinct *
from base