{{ config(materialized='table')}}

with monthly_hc as (

    select period_date as period_start
        ,  region
        ,  count(distinct employee_id) as ending_headcount

    from {{ ref('int_employee_monthly') }}

    where 1=1
    and metric = 'Headcount'

    group by 1,2

)

, monthly_hc_all as (

    select * from monthly_hc

    union all

    select period_start, 'All' as region, sum(ending_headcount) as ending_headcount

    from monthly_hc

    group by 1
    
)

, monthly_terms as (

    select period_date as period_start
        ,  region
        ,  termination_type
        ,  count(distinct employee_id) as terminations

    from {{ ref('int_employee_monthly') }}

    where 1=1
    and metric = 'Terminations'

    group by 1,2,3

)

,  monthly_terms_all as (

    select * from monthly_terms

    union all

    select period_start, region, 'All' as termination_type, sum(terminations) as terminations
    
    from monthly_terms
    
    group by 1,2

    union all

    select period_start, 'All' as region, 'All' as termination_type, sum(terminations) as terminations
    
    from monthly_terms
    
    group by 1

)

, final as (

    select t.period_start
        ,  t.region
        ,  t.termination_type
        ,  t.terminations as overall_terminations
        ,  hc.ending_headcount as overall_ending_headcount

    from monthly_terms_all t

    left join monthly_hc_all hc
    on t.period_start = hc.period_start
    and t.region = hc.region

    order by 1 desc, 2,3

)

select *
from final
order by period_start desc, region, termination_type