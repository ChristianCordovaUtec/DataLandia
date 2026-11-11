
### Versiones para correr en local
python version: 3.10.22
java version: 17


### python libraries  
Revisar requirements.tx


## Notas del proyecto DataLandia

- Cambio de Arquitectura:
    - Antes: Spark tomaba los datos de Cloud Storage y los limpiaba y lo dejaba en BigQuery (capa Bronze).
    - Ahora: Spark se usa para limpiar datos de Bronze a Silver (del contexto2)

- DataLandia tiene 48 zonas, TLC tiene 263 zonas (descartando: N/A y Unknown).
- Se agruparan por LocationID (1 a 263 son las zonas reales, 264 es Unknown y 265 es N/A), se reparten 263 zonas en 48 grupos consecutivos: los primeros 23 grupos tienen 6 zonas de TLC y los otros 25 tienen 5. Esto hace que las zonas no necesariamente estan agrupadas geograficamente.
- Del dataset TLC viajes (enero 2024), hay un total de 4.7% de registros que tienen el campo 'passenger_count' como nulo. Se van a mantener pues, los otros campos si serviran para calcular por ejemplo costo por kilometro. 
- 


#### Pasos que se realizaron:
Batch:
- Paso 1.3: crear la carpeta del proyecto
- Paso 1.4: crear el entorno de conda
- Paso 1.5: activar el entorno
- Paso 2.2: prueba mínima de Spark
- Paso 3.1: instalar Apache Beam
- Paso 3.2: prueba mínima de Beam
- Paso 4.1: instalar dbt Core con el adaptador de DuckDB
- Paso 4.2: crear el proyecto de dbt
- Paso 4.3: configurar la conexión a DuckDB
- Paso 4.4: prueba mínima de dbt
- Paso 5.1: descargar un mes de viajes de taxi amarillo
- Paso 5.2: ver columnas y cantidad de filas
- Paso 5.3: buscar datos sucios
- Paso 5.4: descargar la tabla de zonas de TLC
- Paso 5.5: contar zonas por condado
- Paso 5.6: verificar los IDs
- Paso 5.7: generar el mapeo completo
- Paso 5.8: validar el reparto
- Paso 6.1: escribir y correr el pipeline de Spark (Bronze → Silver)
    - El pipeline hace tres cosas:
        - **Limpia**: descarta los viajes con distancia, duración o total inválidos y los que estan fuera de enero de 2024. Conserva los pasajeros nulos.
        - **Enriquece**: cruza con *mapa_zonas.csv* para agregar *zona_origen* y *zona_destino*.
        - **Agrega dos columnas**: *distancia_km* (TLC reporta millas) y *duracion_min*. El costo por kilómetro se calcula después, en Gold.
- Paso 6.2: comprobar el resultado en Silver
- Paso 7: dbt construye Gold desde Silver
- Paso 7.1: conectar dbt con el Silver que genera Spark
    - Se declara el Silver (de Spark) como fuente de dbt
- Paso 7.2: construir Gold con dbt
    - Creamos dos tablas:
        - **dim_zona**: las 48 zonas más SIN-ZONA, con su condado.
        - **fact_viajes**: los viajes de Silver más *costo_por_km* y *velocidad_kmh*. Agrego también *tarifa_por_km*, calculada con la tarifa sin propina (*fare_amount*), porque *total_amount* incluye la propina. 
- Paso 7.3: revisar valores extremos
    - Se encuentran valores outliers **evidencias/TLC_in_gold?outliers.png**
- Paso 7.4: agregar la columna *es_valido* a Gold
- Paso 7.5: reporte por condado

Stream:
- Paso 8.1: generar los eventos GPS simulados
    - se crea *generar_eventos_gps.py* con 20 000 eventos de 50 vehiculos en 10 minutos (el 15 de enero de 2024, de 08:00 a 08:10), distribuidos en las 48 zonas. Las coordenadas son ficticias.
    - Incluye a propósito un 1 % de eventos con velocidad absurda y otro 1 % sin vehiculo_id, para que el pipeline de Beam tenga algo que limpiar.
    - Los límites y las cantidades los elegí yo. Los eventos se guardan como líneas de JSON en datos/bronze.
- Paso 8.2: pipeline de Beam (eventos GPS de Bronze → Silver)
    - El pipeline hace tres cosas:
        - **Limpia**: separa los eventos sin *vehiculo_id* o con velocidad fuera de 0 a 120 km/h. Los válidos siguen y los rechazados se guardan aparte, como evidencia.
        - **Enriquece**: agrega el nombre del condado a partir de *zona_id*.
        - **Resume** en ventanas: agrupa por zona y por minuto, y calcula la cantidad de eventos, los vehículos distintos, la velocidad media y el porcentaje de vehículos ocupados.
    - Para ello se crea el script *codigo/beam_gps_a_silver.py*
- Paso 9.1: Gold con dbt desde el resumen de Beam
- Paso 9.2: cruzar lotes y tiempo real
    - coincidencia útil: los eventos simulados cubren el 15 de enero de 2024, de 08:00 a 08:10, y los viajes de TLC de enero incluyen ese mismo tramo.
    - Eso permite una tabla de Gold que junte las dos fuentes por zona y minuto: cuántos viajes empezaron (lotes) y cuántos vehículos había (tiempo real). Es el cruce entre el procesamiento por lotes y el de tiempo real.
- 
-



### TODO: Avanzar para terminar el entregable Parte 2
En GCP:
    - 1. Batch
        - Cloud Storage
        - Carga a BigQuery Bronze
        - Dataproc Serverless (Spark), de Bronze a Silver
        - dbt con BigQuery para Gold

    - 2. Streaming
        - Pub/Sub
        - Dataflow (Apache Beam)
        - 


### TODO: queda fuera del entregable Parte 2
- La carga de trenes (OART), las solicitudes de usuarios y las tablas de Gold que dependen de ellas.
- Uber Movement, Array of Things y el censo. Sin ellos el cruce de datos heterogéneos es más débil, porque hoy cruzamos TLC con las zonas. Si sobra tiempo, el censo es el más fácil de sumar.
- La comparación contra conducir un vehículo propio.


### Evidencias
https://drive.google.com/drive/folders/1kA4v6zTsXfjgR3GQbITzmcx9Ry0mf0MV

