{{ config(materialized='table')}}

-- Import CTEs

with hc as (

    select *
    from {{ ref("stage_headcount") }}

)

,    hires as (

    select *
    from {{ ref("stage_hires") }}

)

,    terms as (

    select *
    from {{ ref("stage_terminations") }}

)

-- Logical Layer

, union_data as

