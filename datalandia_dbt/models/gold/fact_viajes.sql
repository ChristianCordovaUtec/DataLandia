{{ config(materialized='table') }}
with base as (
    select
        tpep_pickup_datetime   as inicio_viaje,
        tpep_dropoff_datetime  as fin_viaje,
        zona_origen,
        zona_destino,
        passenger_count        as pasajeros,
        distancia_km,
        duracion_min,
        fare_amount            as tarifa,
        total_amount           as total_cobrado,
        round(total_amount / distancia_km, 3)        as costo_por_km,
        round(fare_amount / distancia_km, 3)         as tarifa_por_km,
        round(distancia_km / (duracion_min / 60), 2) as velocidad_kmh
    from {{ source('silver', 'viajes_tlc') }}
)
select
    *,
    (velocidad_kmh <= 120 and distancia_km >= 0.1 and costo_por_km <= 100) as es_valido
from base
