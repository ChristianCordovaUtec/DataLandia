{{ config(materialized='table') }}
with viajes as (
    select
        date_trunc('minute', inicio_viaje) as minuto,
        zona_origen                        as zona_id,
        count(*)                           as viajes_iniciados
    from {{ ref('fact_viajes') }}
    where es_valido
      and inicio_viaje >= '2024-01-15 08:00:00'
      and inicio_viaje <  '2024-01-15 08:10:00'
    group by 1, 2
)
select
    f.minuto,
    f.zona_id,
    f.condado,
    coalesce(v.viajes_iniciados, 0) as viajes_iniciados,
    f.vehiculos_distintos,
    f.porcentaje_disponibles
from {{ ref('fact_flota_zona_minuto') }} f
left join viajes v on v.minuto = f.minuto and v.zona_id = f.zona_id
