
### Versiones para correr en local
python version: 3.10.22
java version: 17


### python libraries  
Revisar requirements.tx


## Notas del proyecto DataLandia

- Cambio de Arquitectura:
    - Antes: Spark tomaba los datos de Cloud Storage y los limpiaba y lo dejaba en BigQuery (capa Bronze).
    - Ahora: Spark se usa para limpiar datos de Bronze a Silver (del contexto2)

- DataLandia tiene 48 zonas, TLC tiene 263 zonas (descartando: N/A y Unknown)
- Se agruparan por LocationID (1 a 263 son las zonas reales, 264 es Unknown y 265 es N/A), se reparten 263 zonas en 48 grupos consecutivos: los primeros 23 grupos tienen 6 zonas de TLC y los otros 25 tienen 5
- Del dataset TLC viajes (enero 2024), hay un total de 4.7% de registros que tienen el campo 'passenger_count' como nulo. Se van a mantener pues, los otros campos si serviran para calcular por ejemplo costo por kilometro. 
- 
- 