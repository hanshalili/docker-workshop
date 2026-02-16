
with green as (
    select
        'green' as service_type,
        *
    from {{ ref('stg_green_tripdata') }}
),

yellow as (
    select
        'yellow' as service_type,
        *
    from {{ ref('stg_yellow_tripdata') }}
),

unioned as (
    select * from green
    union all
    select * from yellow
),

-- Build deterministic natural key for each trip
keyed as (
    select
        *,
        md5(
            coalesce(cast(service_type as varchar), '') || '|' ||
            coalesce(cast(vendor_id as varchar), '') || '|' ||
            coalesce(cast(pickup_datetime as varchar), '') || '|' ||
            coalesce(cast(dropoff_datetime as varchar), '') || '|' ||
            coalesce(cast(pickup_location_id as varchar), '') || '|' ||
            coalesce(cast(dropoff_location_id as varchar), '') || '|' ||
            coalesce(cast(passenger_count as varchar), '') || '|' ||
            coalesce(cast(trip_distance as varchar), '') || '|' ||
            coalesce(cast(total_amount as varchar), '')
        ) as trip_id
    from unioned
),

-- Remove duplicates if they exist
deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by trip_id
                order by pickup_datetime desc
            ) as rn
        from keyed
    )
    where rn = 1
)

select
    trip_id,
    service_type,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    rate_code_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount
from deduplicated
