{{ config(materialized='ephemeral')}}

select employee_id

       --correcting odd date formatting in source data
    ,  (case when year(period_date) < 100
            then dateadd(year, 2000, period_date)
            else period_date end) as period_date_clean

    ,  date_trunc('quarter', period_date_clean) as beg_of_quarter

    ,  (case when year(termination_date) < 100
            then dateadd(year, 2000, termination_date)
            else termination_date end) as termination_date_clean

    ,  case when metric ilike 'Term%' then 'Termination'
             else initcap(lower(metric)) end
             as metric

    ,  initcap(lower(job_name)) as job_name_clean
    ,  job_name_code
    ,  concat(job_name_clean, ' - ', job_name_code) as job_description

       --correcting classification of contractor employee_ids
    ,  case when employee_id like '%101%' and length(employee_id) >= 5 then 'True'
            when employee_id in ('10256','10257', '10258') and period_date = '2025-08-01' then 'true'
            else initcap(upper(is_contractor)) end as is_contractor_clean

    ,  exec_leader
    ,  l1_from_top_leader
    ,  l2_from_top_leader
    ,  l3_from_top_leader

from {{ source('source','source_terminations')}}