from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").appName("bronze_a_silver_viajes").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet("datos/bronze/yellow_tripdata_2024-01.parquet")
mapa = spark.read.csv("datos/referencia/mapa_zonas.csv", header=True, inferSchema=True)

# 1. Limpieza
limpio = bronze.filter(
    (F.col("trip_distance") > 0)
    & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    & (F.col("total_amount") > 0)
    & (F.col("tpep_pickup_datetime") >= "2024-01-01")
    & (F.col("tpep_pickup_datetime") < "2024-02-01")
)

# 2. Enriquecimiento: cruce con el mapa de zonas (origen y destino)
mapa_origen = mapa.select(
    F.col("location_id").alias("PULocationID"), F.col("zona_id").alias("zona_origen"))
mapa_destino = mapa.select(
    F.col("location_id").alias("DOLocationID"), F.col("zona_id").alias("zona_destino"))

silver = (
    limpio
    .join(mapa_origen, "PULocationID", "left")
    .join(mapa_destino, "DOLocationID", "left")
    .withColumn("distancia_km", F.round(F.col("trip_distance") * 1.609344, 3))
    .withColumn(
        "duracion_min",
        F.round((F.unix_timestamp("tpep_dropoff_datetime")
                 - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2))
)

silver.write.mode("overwrite").parquet("datos/silver/viajes_tlc")

# Evidencia: conteos antes y después
n_bronze = bronze.count()
n_silver = spark.read.parquet("datos/silver/viajes_tlc").count()
print("filas en Bronze:", n_bronze)
print("filas en Silver:", n_silver)
print("descartadas:", n_bronze - n_silver)
print("con origen SIN-ZONA:", silver.filter(F.col("zona_origen") == "SIN-ZONA").count())
print("con origen sin cruce (nulo):", silver.filter(F.col("zona_origen").isNull()).count())
spark.stop()
