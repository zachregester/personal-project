{{ config(materialized='table')}}

-- Example Scenario Below

-- Stakeholder Request: Provide a production-grade source for a Tableau dashboard covering headcount, hires, terminations, surveys, and training.
-- Stakeholder: People Analytics team member.
-- Audience: People Analytics stakeholders.
-- Inputs: source_headcount, source_hires, source_terminations, source_surveys, source_trainings.
-- Outputs: clean, conformed fields, with consistent date keys.

-- Import CTEs

with hc as (

    select 'Headcount' as metric
        ,  employee_id
        ,  period_date_clean as period_date
        ,  beg_of_quarter
        ,  null as hire_date
        ,  null as termination_date
        ,  job_name_clean as job_name
        ,  job_name_code
        ,  job_description
        ,  is_contractor_clean as is_contractor
        ,  exec_leader
        ,  l1_from_top_leader
        ,  l2_from_top_leader
        ,  l3_from_top_leader
        ,  direct_manager
        ,  null as survey_name
        ,  null as survey_section
        ,  null as section_score
        ,  null as section_answered
        ,  null as training_name
        ,  null as training_status
        ,  null as training_start_date
        ,  null as training_end_date

    from {{ ref("stage_headcount") }} hc

)

,    hires as (

        select 'Hires' as metric
        ,  employee_id
        ,  period_date_clean as period_date
        ,  beg_of_quarter
        ,  hire_date_clean as hire_date
        ,  null as termination_date
        ,  job_name_clean as job_name
        ,  job_name_code
        ,  job_description
        ,  is_contractor_clean as is_contractor
        ,  exec_leader
        ,  l1_from_top_leader
        ,  l2_from_top_leader
        ,  l3_from_top_leader
        ,  null as direct_manager
        ,  null as survey_name
        ,  null as survey_section
        ,  null as section_score
        ,  null as section_answered
        ,  null as training_name
        ,  null as training_status
        ,  null as training_start_date
        ,  null as training_end_date

        from {{ ref("stage_hires") }}

)

,    terms as (

        select 'Terminations' as metric
        ,  employee_id
        ,  period_date_clean as period_date
        ,  beg_of_quarter
        ,  null as hire_date
        ,  termination_date_clean as termination_date
        ,  job_name_clean as job_name
        ,  job_name_code
        ,  job_description
        ,  is_contractor_clean as is_contractor
        ,  exec_leader
        ,  l1_from_top_leader
        ,  l2_from_top_leader
        ,  l3_from_top_leader
        ,  null as direct_manager
        ,  null as survey_name
        ,  null as survey_section
        ,  null as section_score
        ,  null as section_answered
        ,  null as training_name
        ,  null as training_status
        ,  null as training_start_date
        ,  null as training_end_date

        from {{ ref("stage_terminations") }}

)

,   surveys as (

        select 'Surveys' as metric
        ,  employee_id
        ,  null as period_date
        ,  null as beg_of_quarter
        ,  null as hire_date
        ,  null as termination_date
        ,  null as job_name
        ,  null as job_name_code
        ,  null as job_description
        ,  null as is_contractor
        ,  exec_leader
        ,  l1_from_top_leader
        ,  l2_from_top_leader
        ,  l3_from_top_leader
        ,  direct_manager
        ,  survey_name
        ,  survey_section
        ,  section_score
        ,  section_answered
        ,  null as training_name
        ,  null as training_status
        ,  null as training_start_date
        ,  null as training_end_date

        from {{ ref("stage_surveys") }}

)

,    trainings as (

        select 'Trainings' as metric
        ,  employee_id
        ,  null as period_date
        ,  null as beg_of_quarter
        ,  null as hire_date
        ,  null as termination_date
        ,  null as job_name
        ,  null as job_name_code
        ,  null as job_description
        ,  null as is_contractor
        ,  null as exec_leader
        ,  null as l1_from_top_leader
        ,  null as l2_from_top_leader
        ,  null as l3_from_top_leader
        ,  null as direct_manager
        ,  null as survey_name
        ,  null as survey_section
        ,  null as section_score
        ,  null as section_answered
        ,  training_name
        ,  training_status
        ,  training_start_date_clean as training_start_date
        ,  training_end_date_final as training_end_date

        from {{ ref("stage_trainings") }}

)

-- Union Metrics

,    stacked as (

       (select * from hc)

        union all

       (select * from hires)

        union all

       (select * from terms)

        union all

       (select * from surveys)

        union all

       (select * from trainings)

)

-- Final CTE

,    final as (

        select s.metric
        ,  s.employee_id
        ,  s.period_date
        ,  s.beg_of_quarter
        ,  s.hire_date
        ,  s.termination_date
        ,  s.job_name
        ,  s.job_name_code
        ,  s.job_description
        ,  s.is_contractor
        ,  s.exec_leader
        ,  s.l1_from_top_leader
        ,  s.l2_from_top_leader
        ,  s.l3_from_top_leader
        ,  s.direct_manager
        ,  s.survey_name
        ,  s.survey_section
        ,  s.section_score
        ,  s.section_answered
        ,  s.training_name
        ,  s.training_status
        ,  s.training_start_date
        ,  s.training_end_date

    from stacked s

)

select *
from final


