{{ config(materialized='table') }}
select distinct
    zona_id,
    split_part(zona_id, '-', 1) as codigo_condado,
    case split_part(zona_id, '-', 1)
        when 'CEN' then 'Central'
        when 'NOR' then 'Norte'
        when 'SUR' then 'Sur'
        when 'EST' then 'Este'
        when 'OES' then 'Oeste'
        when 'NES' then 'Noreste'
        when 'SOE' then 'Suroeste'
        when 'COS' then 'Costa'
        else 'Sin zona'
    end as condado
from {{ source('referencia', 'mapa_zonas') }}
