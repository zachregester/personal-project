{{ config(materialized='table')}}

with headcount_base as (

    select period_date as period_start
        ,  region
        ,  count(distinct employee_id) as ending_headcount

    from {{ ref('int_employee_monthly') }}

    where 1=1
    and metric = 'Headcount'
    group by 1,2

)

, headcount_all as (

    select * 
    from headcount_base

    union all

    select period_start
         , 'All' as region
         , sum(ending_headcount) as ending_headcount

    from headcount_base
    group by 1
    
)

, headcount_rolling as (

    select period_start
        ,  region
        ,  ending_headcount

        ,  avg(ending_headcount) over (
            partition by region
            order by period_start
            rows between 11 preceding and current row)
            as avg_headcount_12m
        
    
    from headcount_all
)

, terminations_base as (

    select period_date as period_start
        ,  region
        ,  termination_reason
        ,  count(distinct employee_id) as terminations

    from {{ ref('int_employee_monthly') }}

    where 1=1
    and metric = 'Terminations'

    group by 1,2,3

)

, terminations_all as (

    select *
    from terminations_base

    union all

    select period_start
         , region
         , 'All' as termination_reason
         , sum(terminations) as terminations
    
    from terminations_base
    group by 1,2

    union all

    select period_start
         , 'All' as region
         , termination_reason
         , sum(terminations) as terminations
    from terminations_base
    group by 1,3

    union all

    select period_start
         , 'All' as region
         , 'All' as termination_reason
         , sum(terminations) as terminations
    
    from terminations_base
    group by 1

)

, combined as (

    select t.period_start
        ,  t.region
        ,  t.termination_reason
        ,  t.terminations
        ,  hc.ending_headcount

    from terminations_all t

    left join headcount_all hc
    on t.period_start = hc.period_start
    and t.region = hc.region

)

, final as (

    select c.period_start
        ,  c.region
        ,  c.termination_reason
        ,  c.terminations
        ,  c.ending_headcount

        -- rolling terminations calc
        ,  sum(c.terminations) over (
            partition by c.region, c.termination_reason 
            order by c.period_start
            rows between 11 preceding and current row) as terminations_12m 

        -- rolling avg headcount calc
        ,  roll.avg_headcount_12m

        -- rolling turnover calculation
        , (sum(c.terminations) over (
            partition by c.region, c.termination_reason
            order by c.period_start
            rows between 11 preceding and current row))
            
            /

            nullif(roll.avg_headcount_12m,0) as turnover_rate_12m
        
    from combined c

    left join headcount_rolling roll
    on c.period_start = roll.period_start
    and c.region = roll.region
)

select *
from final
order by period_start desc, region, termination_reason