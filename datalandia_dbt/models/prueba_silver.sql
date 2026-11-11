{{ config(materialized='table') }}
select count(*) as filas from {{ source('silver', 'viajes_tlc') }}
