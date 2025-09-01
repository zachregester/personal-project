{{ config(materialized='ephemeral')}}

select employee_id

       --correcting odd date formatting in source data
    ,  (case when year(training_start_date) < 100
            then dateadd(year, 2000, training_start_date)
            else training_start_date end) as training_start_date_clean

    ,  (case when year(training_end_date) < 100
            then dateadd(year, 2000, training_end_date)
            else training_end_date end) as training_end_date_clean

    ,  (case when training_end_date_clean is null then current_date() 
             else training_end_date_clean
             end) as training_end_date_final

       --flag for active trainings
    ,  (case when training_end_date_final = current_date() then 'Active'
             else 'Inactive'
             end) as training_status

    ,  training_name

from {{ source('source','source_trainings')}}