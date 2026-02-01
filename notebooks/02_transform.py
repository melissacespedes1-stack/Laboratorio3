# %% [markdown]
# # 2. Transformación (Capa Plata) + Quality Gate
# - Lee Bronze (Delta)
# - Reglas de calidad:
#   1) precio_base > 0
#   2) fecha_de_firma no nula
# - Split:
#   ✅ Silver -> /app/data/lakehouse/silver/secop
#   ❌ Quarantine -> /app/data/lakehouse/quarantine/secop_errors + motivo_rechazo

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws, to_date
from delta import *

master_url = "spark://spark-master:7077"

BRONZE_PATH = "/app/data/lakehouse/bronze/secop"
SILVER_PATH = "/app/data/lakehouse/silver/secop"
QUARANTINE_PATH = "/app/data/lakehouse/quarantine/secop_errors"

builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Silver_QualityGate")
    .master(master_url)
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.executor.memory", "1g")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
print("Leyendo Bronze (Delta)...")
df_bronze = spark.read.format("delta").load(BRONZE_PATH)

print("Columnas Bronze (primeras 30):")
print(df_bronze.columns[:30])

# Asegurar tipo fecha (si viene string)
df = df_bronze.withColumn("fecha_de_firma", to_date(col("fecha_de_firma")))

# -----------------------------
# QUALITY GATE: motivos rechazo
# -----------------------------
motivo_precio = when(
    col("precio_base").isNull() | (col("precio_base") <= 0),
    lit("precio_base<=0_o_nulo")
)

motivo_fecha = when(
    col("fecha_de_firma").isNull(),
    lit("fecha_de_firma_nula")
)

df_qc = df.withColumn("motivo_rechazo", concat_ws(" | ", motivo_precio, motivo_fecha))

# Split: inválidos vs válidos
df_invalid = df_qc.filter(col("motivo_rechazo") != "")
df_valid = df_qc.filter(col("motivo_rechazo") == "")

# %%
# Silver (válidos)
df_silver = df_valid.select(
    "entidad",
    "nit_entidad",
    "departamento",
    "ciudad",
    "estado",
    "descripcion_del_proceso",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "justificacion_modalidad_de_contratacion",
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "precio_base",
    "valor_total",
    "valor_pagado"
)

# Quarantine (inválidos) + motivo
df_quarantine = df_invalid.select(
    "entidad",
    "nit_entidad",
    "departamento",
    "ciudad",
    "estado",
    "descripcion_del_proceso",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "justificacion_modalidad_de_contratacion",
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "precio_base",
    "valor_total",
    "valor_pagado",
    "motivo_rechazo"
)

# %%
print("Escribiendo Silver (Delta)...")
df_silver.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("Escribiendo Quarantine (Delta)...")
df_quarantine.write.format("delta").mode("overwrite").save(QUARANTINE_PATH)

silver_count = df_silver.count()
quarantine_count = df_quarantine.count()

print(f"✅ Silver rows: {silver_count}")
print(f"❌ Quarantine rows: {quarantine_count}")

print("Ejemplo quarantine:")
df_quarantine.show(10, truncate=False)
