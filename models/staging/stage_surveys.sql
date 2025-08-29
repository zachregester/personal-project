{{ config(materialized='ephemeral')}}

select employee_id
    ,  survey_name
    ,  survey_section
    ,  section_score
    ,  (case when section_score is not null then 'Yes'
             when section_score is null then 'No'
             end) as section_answered
    ,  exec_leader
    ,  l1_from_top_leader
    ,  l2_from_top_leader
    ,  l3_from_top_leader
    ,  direct_manager

from {{ source('source','source_surveys')}}