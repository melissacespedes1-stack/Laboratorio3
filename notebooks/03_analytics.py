# %% [markdown]
# # 3. Analítica (Capa Oro)
# - Lee Silver (Delta)
# - Agrega Top 10 departamentos por total contratado
# - Guarda en Gold (Delta)

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, desc
from delta import *

master_url = "spark://spark-master:7077"

SILVER_PATH = "/app/data/lakehouse/silver/secop"
GOLD_PATH = "/app/data/lakehouse/gold/top_deptos"

builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Gold")
    .master(master_url)
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.executor.memory", "1g")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("Leyendo Silver (Delta)...")
df_silver = spark.read.format("delta").load(SILVER_PATH)

df_gold = (
    df_silver
    .groupBy("departamento")
    .agg(_sum("precio_base").alias("total_contratado"))
    .orderBy(desc("total_contratado"))
    .limit(10)
)

print("Top 10 departamentos (preview):")
df_gold.show(truncate=False)

print("Escribiendo Gold (Delta)...")
df_gold.write.format("delta").mode("overwrite").save(GOLD_PATH)

print(f"✅ Gold guardado en: {GOLD_PATH}")
print(df_gold.toPandas())


