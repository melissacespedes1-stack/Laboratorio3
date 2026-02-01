# %% [markdown]
# # 1. Ingesta (Capa Bronce)
# Convertimos CSV crudo a formato Delta Lake (Bronze).
# - Lee el CSV desde /app/data (volumen compartido con el cluster)
# - Limpia nombres de columnas para cumplir reglas de Delta
# - Escribe en /app/data/lakehouse/bronze/secop

# %%
from pyspark.sql import SparkSession
from delta import *
import re

# URL del Master (definida en docker-compose)
master_url = "spark://spark-master:7077"

# Rutas absolutas (evita problemas de working directory)
CSV_PATH = "/app/data/SECOP_II_Contratos_Electronicos.csv"
BRONZE_PATH = "/app/data/lakehouse/bronze/secop"

# Configuración Spark + Delta
builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Bronze")
    .master(master_url)
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.executor.memory", "1g")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# %%
def clean_col(c: str) -> str:
    """
    Delta Lake no permite ciertos caracteres en nombres de columnas.
    Normalizamos a snake_case:
    - trim
    - espacios/tabs/newlines -> _
    - elimina caracteres inválidos: , ; { } ( ) = y similares
    - colapsa __
    - lowercase
    """
    c = c.strip()
    c = re.sub(r"[\s\t\n]+", "_", c)        # whitespace -> _
    c = re.sub(r"[,\;{}\(\)=]+", "", c)     # remove invalid chars
    c = re.sub(r"__+", "_", c)              # collapse multiple underscores
    return c.lower()


# %%
print("Leyendo CSV crudo...")
df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .load(CSV_PATH)
)

print("Columnas originales (primeras 20):")
print(df_raw.columns[:20])


# %%
print("Limpiando nombres de columnas para Delta...")
df_raw = df_raw.toDF(*[clean_col(c) for c in df_raw.columns])

print("Columnas limpias (primeras 20):")
print(df_raw.columns[:20])


# %%
print("Escribiendo en capa Bronce (Delta)...")
df_raw.write.format("delta").mode("overwrite").save(BRONZE_PATH)

rows = df_raw.count()
print(f"✅ Ingesta completada. Registros procesados: {rows}")
print(f"✅ Bronze guardado en: {BRONZE_PATH}")

