{{ config(materialized='table') }}
select
    cast(e.ventana_inicio as timestamp)   as minuto,
    e.zona_id,
    d.condado,
    e.eventos,
    e.vehiculos_distintos,
    e.velocidad_media_kmh,
    e.porcentaje_ocupados,
    round(100 - e.porcentaje_ocupados, 1) as porcentaje_disponibles
from {{ source('silver', 'eventos_gps_por_zona_minuto') }} e
left join {{ ref('dim_zona') }} d on d.zona_id = e.zona_id
