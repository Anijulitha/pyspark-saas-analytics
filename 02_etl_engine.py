from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, to_date, explode_outer

# 1. Configuración del Clúster
spark = (
    SparkSession.builder.appName("SaaS-Analytics-ETL").master("local[*]").getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 2. Esquema Estricto (Evita inferSchema para optimizar rendimiento)
esquema_logs = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("timestamp", StringType(), True),
        StructField(
            "user",
            StructType(
                [
                    StructField("user_id", StringType(), True),
                    StructField("plan", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "device",
            StructType(
                [
                    StructField("os", StringType(), True),
                    StructField("browser", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("action", StringType(), True),
        StructField(
            "metadata",
            ArrayType(
                StructType(
                    [
                        StructField("key", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

print("📥 Ingestando logs crudos...")
df_raw = spark.read.json("raw_events.json", schema=esquema_logs)

# 3. Transformación y Limpieza (Flatten & Data Quality)
print("🧹 Aplanando JSONs y limpiando datos...")
df_clean = (
    df_raw.filter(col("user.plan").isNotNull())
    .dropDuplicates(["event_id"])
    .withColumn("fecha", to_date(col("timestamp")))
    .withColumn("meta_exploded", explode_outer(col("metadata")))
    .select(
        col("event_id"),
        col("fecha"),
        col("user.user_id").alias("usuario"),
        col("user.plan").alias("suscripcion"),
        col("device.os").alias("sistema_operativo"),
        col("action").alias("accion"),
        col("meta_exploded.key").alias("meta_clave"),
        col("meta_exploded.value").alias("meta_valor"),
    )
)

# 4. Almacenamiento Optimizado (Partition Pruning)
print("💾 Escribiendo Data Lake en formato Parquet particionado...")
df_clean.write.mode("overwrite").partitionBy("fecha", "suscripcion").parquet(
    "datalake_eventos.parquet"
)

print("🚀 ¡Pipeline completado con éxito!")
spark.stop()
