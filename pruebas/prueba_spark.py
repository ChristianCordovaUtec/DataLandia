from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("prueba").getOrCreate()
tabla = spark.createDataFrame([(1, "uno"), (2, "dos")], ["numero", "texto"])
tabla.show()
print("Spark version:", spark.version)
spark.stop()
