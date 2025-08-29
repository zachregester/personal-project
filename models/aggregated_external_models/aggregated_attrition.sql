{{ config(materialized='table')}}

-- Stakeholder has requested table reflecting overall monthly attrition, along with isolating the data engineer population

with base as (

    select beg_of_quarter as quarter_start
        ,  period_start
        ,  sum(case when metric = Terminations then 1 else 0 end) as overall_terminations
        ,  sum(case when metric = Terminations and job_name = 'Data Engineer' then 1 else 0 end) as data_engineer_terminations
        ,  count(distinct case when metric = 'Headcount' then employee_id end) as overall_end_headcount
        ,  count(distinct case when metric = 'Headcount' and job_name = 'Data Engineer' then employee_id end) as data_engineer_end_headcount

    from {{ ref('tableau_leader_scorecard') }} hc

    group by 1,2 order by 1,2 desc

)

, monthly_overall as (

    select quarter_start
        ,  period_start
        ,  overall_terminations
        ,  overall_end_headcount
        ,  lag(headcount) over (order by period_date) as overall_beg_headcount

    from base

    group by 1,2 order by 1,2 desc

)

, monthly_data_engineer as (

     select quarter_start
         ,  period_start
         ,  data_engineer_terminations
         ,  data_engineer_end_headcount
         ,  lag(data_engineer_headcount) over (order by period_date) as data_engineer_beg_headcount

    from base

    group by 1,2 order by 1, 2 desc

)

, quarterly_overall as (

    select distinct quarter_start
                 ,  period_start
                 ,  overall_terminations / ((overall_beg_headcount + overall_end_headcount) / 2) as overall_attrition_monthly

                 ,  (sum(overall_terminations) over (partition by quarter_start))

                        /
                        
                        ((first_value(overall_beg_headcount) over (partition by quarter_start order by period_start))

                        +

                        (last_value(overall_end_headcount) over (partition by quarter_start order by period_start)) / 2)

                        as overall_attrition_quarterly

    from monthly_overall

)

, quarterly_data_engineer as (

    select distinct quarter_start
                 ,  period_start
                 ,  data_engineer_terminations / ((data_engineer_beg_headcount + data_engineer_end_headcount) / 2) as data_engineer_attrition_monthly

                 ,  (sum(data_engineer_terminations) over (partition by quarter_start))

                        /
                        
                        ((first_value(data_engineer_beg_headcount) over (partition by quarter_start order by period_start))

                        +

                        (last_value(data_engineer_end_headcount) over (partition by quarter_start order by period_start)) / 2)

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

)

select *
from final_attrition

where 1=1
order by period_date