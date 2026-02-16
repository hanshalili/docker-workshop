{{ config(materialized='table') }}

with trips as (
    select
        service_type,
        pickup_datetime,
        pickup_location_id,
        total_amount
    from {{ ref('int_trips_unioned') }}
    where pickup_datetime is not null
),

trips_with_zones as (
    select
        date_trunc('month', t.pickup_datetime) as revenue_month,
        cast(strftime(t.pickup_datetime, '%Y') as integer) as year,
        cast(strftime(t.pickup_datetime, '%m') as integer) as month,

        t.service_type,

        z.borough as pickup_borough,
        z.zone as pickup_zone,

        t.total_amount
    from trips t
    left join {{ ref('dim_zones') }} z
      on t.pickup_location_id = z.location_id
)

select
    revenue_month,
    year,
    month,
    service_type,
    pickup_borough,
    pickup_zone,
    count(*) as total_monthly_trips,
    sum(coalesce(total_amount, 0)) as revenue_monthly_total_amount
from trips_with_zones
group by 1,2,3,4,5,6


