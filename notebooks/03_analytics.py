# %% [markdown]
# # 3. Analítica (Capa Oro)
# Agregaciones de negocio.

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc, col
from delta import *

builder = SparkSession.builder \
    .appName("Lab_SECOP_Gold") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
# Leer Plata
df_silver = spark.read.format("delta").load("data/lakehouse/silver/secop")

# %%
# Agregación (Shuffle)
df_gold = df_silver \
    .groupBy("departamento") \
    .agg(sum("precio_base").alias("total_contratado")) \
    .orderBy(desc("total_contratado")) \
    .limit(10)

# %%
# Persistir Oro
df_gold.write.format("delta").mode("overwrite").save("data/lakehouse/gold/top_deptos")

# %%
# Visualizar
print("Top 10 Departamentos por contratación:")
df_pandas = df_gold.toPandas()
print(df_pandas)
