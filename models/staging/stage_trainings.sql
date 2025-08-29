{{ config(materialized='ephemeral')}}

select employee_id
    ,  to_varchar(training_start_date, 'YYYY-MM-DD') as training_start_date
    ,  to_varchar(training_end_date, 'YYYY-MM-DD') as training_end_date

    ,  (case when training_end_date is null then current_date() 
             else training_end_date
             end) as training_end_data_clean

    ,  (case when training_end_data_clean = current_date() then 'Active'
             else 'Inactive'
             end) as training_status

    ,  training_name

from {{ source('source','source_trainings')}}