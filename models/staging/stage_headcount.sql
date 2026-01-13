{{ config(materialized='ephemeral')}}

select employee_id

       --correcting odd date formatting in source data
    ,  (case when year(period_date) < 100
            then dateadd(year, 2000, period_date)
            else period_date end) as period_date_clean

    ,  date_trunc('quarter', period_date_clean) as beg_of_quarter

    ,  initcap(lower(metric)) as metric

    ,  (case when job_name ilike '%hr generalist%' then 'HR Generalist'
             when job_name ilike '%ml engineer%' then 'ML Engineer'
             when job_name is null and job_name_code like 'MLE%' then 'ML Engineer'
             else initcap(lower(job_name)) end) as job_name_clean

    ,  job_name_code as job_code
    ,  concat(job_name_clean, ' - ', job_name_code) as job_description

       --correcting classification of contractor employee_ids
    ,  (case when employee_id like '%101%' and length(employee_id) >= 5 then TRUE
            when employee_id in ('10256','10257', '10258') and period_date = '2025-08-01' then TRUE
            else try_to_boolean(is_contractor) end) as is_contractor

    ,  exec_leader
    ,  l1_from_top_leader
    ,  l2_from_top_leader
    ,  l3_from_top_leader
    ,  direct_manager
    ,  upper(region) as region
    ,  state

from {{ source('source','source_headcount')}}

where 1=1

and period_date <= date_trunc('month', current_date)