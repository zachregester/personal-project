{{ config(materialized='ephemeral')}}

select *

from {{ source('source','source_trainings')}}