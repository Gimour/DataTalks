with 

source as (

    select * from {{ source('staging', 'fhv_2019') }}

),

renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed


-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}