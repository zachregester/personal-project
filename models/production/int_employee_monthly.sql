{{ config(materialized='table')}}

with hc as (

    select distinct 'Headcount' as metric
        ,  employee_id
        ,  period_date_clean as period_date
        ,  beg_of_quarter
        ,  null as hire_date
        ,  null as termination_date
        ,  null as termination_type
        ,  null as termination_reason
        ,  job_name_clean as job_name
        ,  job_code
        ,  job_description
        ,  is_contractor
        ,  exec_leader
        ,  l1_from_top_leader
        ,  l2_from_top_leader
        ,  l3_from_top_leader
        ,  region
        ,  state

    from {{ ref("stage_headcount") }} hc
)

,    hires as (

        select distinct 'Hires' as metric
        ,  employee_id
        ,  period_date_clean as period_date
        ,  beg_of_quarter
        ,  hire_date
        ,  null as termination_date
        ,  null as termination_type
        ,  null as termination_reason
        ,  job_name_clean as job_name
        ,  job_code
        ,  job_description
        ,  is_contractor
        ,  exec_leader
        ,  l1_from_top_leader
        ,  l2_from_top_leader
        ,  l3_from_top_leader
        ,  null as region
        ,  null as state

        from {{ ref("stage_hires") }}
)

,    terms as (

        select distinct 'Terminations' as metric
        ,  t.employee_id
        ,  t.period_date_clean as period_date
        ,  t.beg_of_quarter
        ,  null as hire_date
        ,  t.termination_date
        ,  t.termination_type
        ,  t.termination_reason
        ,  t.job_name_clean as job_name
        ,  t.job_code
        ,  t.job_description
        ,  t.is_contractor
        ,  t.exec_leader
        ,  t.l1_from_top_leader
        ,  t.l2_from_top_leader
        ,  t.l3_from_top_leader

        ,  (case when t.exec_leader = 'Alex Kim' then 'WEST'
                 when hc.region is null then 'EAST'
                 else hc.region end) as region

        ,  hc.state

        from {{ ref("stage_terminations") }} t

        left join hc
        on hc.employee_id = t.employee_id
        and hc.period_date = t.period_date_clean
)

-- Union Metrics

,    stacked as (

       (select * from hc)

        union all

       (select * from hires)

        union all

       (select * from terms)
)

-- Final CTE

,    final as (

        select s.metric
        ,  s.employee_id
        ,  s.period_date
        ,  s.beg_of_quarter
        ,  s.hire_date
        ,  s.termination_date
        ,  s.termination_type
        ,  s.termination_reason
        ,  s.job_name
        ,  s.job_code
        ,  s.job_description
        ,  s.is_contractor
        ,  s.exec_leader
        ,  s.l1_from_top_leader
        ,  s.l2_from_top_leader
        ,  s.l3_from_top_leader
        ,  s.region
        ,  s.state

    from stacked s
)

select distinct *
from final