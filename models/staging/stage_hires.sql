{{ config(materialized='ephemeral')}}

select employee_id
    ,  to_varchar(period_date, 'YYYY-MM-DD') as period_date
    ,  date_trunc('quarter', period_date) as beg_of_quarter
    ,  hire_date
    ,  initcap(lower(metric)) as metric
    ,  initcap(lower(job_name)) as job_name_clean
    ,  job_name_code
    ,  concat(job_name_clean, ' - ', job_name_code) as job_description

    ,  (case when employee_id like '%101%' and length(employee_id) >= 5 then 'True'
            when employee_id in ('10256','10257', '10258') and period_date = '2025-08-01' then 'true'
            else initcap(upper(is_contractor)) end) as is_contractor_clean

    ,  exec_leader
    ,  l1_from_top_leader
    ,  l2_from_top_leader
    ,  l3_from_top_leader

from {{ source('source','source_hires')}}

where 1=1
and period_date >= '2024-12-01'
and date_trunc('month', hire_date) =  period_date
and period_date <= date_trunc('month', current_date)