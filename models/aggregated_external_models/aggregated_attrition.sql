{{ config(materialized='table')}}

-- Example Scenario Below

-- Purpose: Provide monthly and quarterly attrition metrics at both the overall workforce level and for the Data Engineer population specifically.
-- Audience: Ops Analytics Team
-- Requirement: Aggregated to protect employee-level data.
-- Grain: One row per month (period_start), with derived quarterly metrics.
-- Inputs: tableau_leader_scorecard (derived from source_headcount, source_hires, source_terminations, source_surveys, source_trainings).
-- Outputs: Clean attrition measures (overall vs. Data Engineer) with consistent date keys and quarterly rollups.

with base as (

    select concat('Q', quarter(beg_of_quarter), ' ', year(beg_of_quarter)) as quarter_start
        ,  period_date as period_start
        ,  sum(case when metric = 'Terminations' then 1 else 0 end) as overall_terminations
        ,  sum(case when metric = 'Terminations' and job_name = 'Data Engineer' then 1 else 0 end) as data_engineer_terminations
        ,  count(distinct case when metric = 'Headcount' then employee_id end) as overall_end_headcount
        ,  count(distinct case when metric = 'Headcount' and job_name = 'Data Engineer' then employee_id end) as data_engineer_end_headcount

    from {{ ref('tableau_leader_scorecard') }} hc

    where 1=1
    and metric not in ('Trainings', 'Surveys')

    group by 1,2 order by 1,2 desc

)

, monthly_overall as (

    select quarter_start
        ,  period_start
        ,  overall_terminations
        ,  overall_end_headcount
        ,  lag(overall_end_headcount) over (order by period_start) as overall_beg_headcount

    from base

    order by period_start

)

, monthly_data_engineer as (

     select quarter_start
         ,  period_start
         ,  data_engineer_terminations
         ,  data_engineer_end_headcount
         ,  lag(data_engineer_end_headcount) over (order by period_start) as data_engineer_beg_headcount

    from base

    order by period_start

)

, quarterly_overall as (

    select distinct quarter_start
                 ,  period_start
                 ,  overall_terminations / nullif(((overall_beg_headcount + overall_end_headcount) / 2),0) as overall_attrition_monthly

                 ,  (sum(overall_terminations) over (partition by quarter_start))

                        /
                        
                        nullif(((first_value(overall_beg_headcount) over (partition by quarter_start order by period_start))

                        +

                        (last_value(overall_end_headcount) over (partition by quarter_start order by period_start)) / 2),0)

                        as overall_attrition_quarterly

    from monthly_overall

)

, quarterly_data_engineer as (

    select distinct quarter_start
                 ,  period_start
                 ,  data_engineer_terminations / nullif(((data_engineer_beg_headcount + data_engineer_end_headcount) / 2),0) as data_engineer_attrition_monthly

                 ,  (sum(data_engineer_terminations) over (partition by quarter_start))

                        /
                        
                        nullif(((first_value(data_engineer_beg_headcount) over (partition by quarter_start order by period_start))

                        +

                        (last_value(data_engineer_end_headcount) over (partition by quarter_start order by period_start)) / 2),0)

                        as data_engineer_attrition_quarterly

    from monthly_data_engineer

)

,  final_attrition as (

    select o.*
        ,  de.data_engineer_attrition_monthly
        ,  de.data_engineer_attrition_quarterly

    from quarterly_overall o

    left join quarterly_data_engineer de
    on o.period_start = de.period_start

    where 1=1
    and year(o.period_start) >= 2025

)

select *
from final_attrition

where 1=1
order by period_start