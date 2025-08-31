with test as (

  select distinct beg_of_quarter
                , period_date

  from {{ ref('tableau_leader_scorecard') }}
)

select beg_of_quarter
     , period_date
     , count(*) as row_count

from test

group by 1,2
having count(*) > 1