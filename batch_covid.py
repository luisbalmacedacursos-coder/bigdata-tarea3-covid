import re, unicodedata
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("BatchCovidColombia").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. CARGA
df = spark.read.csv("covid_colombia.csv", header=True, inferSchema=False)
print("Filas cargadas:", df.count())

# Normalizar nombres de columnas: minúsculas, sin tildes ni espacios
def limpiar(n):
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", n.lower()).strip("_")
df = df.toDF(*[limpiar(c) for c in df.columns])
df.printSchema()

# 2. LIMPIEZA Y TRANSFORMACIÓN
df = df.withColumn("edad", F.col("edad").cast("int")) \
       .withColumn("fecha_notif", F.to_date(F.substring("fecha_de_notificacion", 1, 10), "yyyy-MM-dd")) \
       .withColumn("sexo", F.upper(F.trim("sexo"))) \
       .withColumn("estado", F.initcap(F.trim("estado")))

df = df.dropDuplicates(["id_de_caso"]) \
       .filter(F.col("unidad_de_medida_de_edad") == "1") \
       .filter(F.col("edad").between(0, 110)) \
       .filter(F.col("nombre_departamento").isNotNull()) \
       .filter(F.col("fecha_notif").isNotNull())
df.cache()
print("Filas después de limpiar:", df.count())

# 3. EDA
print("== Casos por departamento (top 10) ==")
por_depto = df.groupBy("nombre_departamento").count().orderBy(F.desc("count")).limit(10)
por_depto.show(truncate=False)

print("== Casos por sexo ==")
por_sexo = df.groupBy("sexo").count()
por_sexo.show()

print("== Estado del caso ==")
por_estado = df.groupBy("estado").count().orderBy(F.desc("count"))
por_estado.show()

print("== Estadísticas de edad ==")
df.select(F.mean("edad").alias("promedio"), F.stddev("edad").alias("desv"),
          F.min("edad").alias("min"), F.max("edad").alias("max")).show()

por_mes = df.groupBy(F.date_format("fecha_notif", "yyyy-MM").alias("mes")) \
            .count().orderBy("mes")

df = df.withColumn("grupo_edad", F.when(F.col("edad") < 18, "0-17")
      .when(F.col("edad") < 40, "18-39").when(F.col("edad") < 60, "40-59").otherwise("60+"))
por_edad = df.groupBy("grupo_edad").count().orderBy("grupo_edad")

# 4. VISUALIZACIÓN (se guardan como PNG)
d = por_depto.toPandas()
plt.figure(figsize=(10, 5))
plt.barh(d["nombre_departamento"], d["count"]); plt.gca().invert_yaxis()
plt.title("Top 10 departamentos por casos"); plt.tight_layout()
plt.savefig("grafica_departamentos.png"); plt.close()

m = por_mes.toPandas()
plt.figure(figsize=(12, 5))
plt.plot(m["mes"], m["count"]); plt.xticks(rotation=90, fontsize=6)
plt.title("Casos por mes"); plt.tight_layout()
plt.savefig("grafica_casos_mes.png"); plt.close()

e = por_edad.toPandas()
plt.figure(figsize=(6, 4))
plt.bar(e["grupo_edad"], e["count"]); plt.title("Casos por grupo de edad")
plt.tight_layout(); plt.savefig("grafica_edad.png"); plt.close()

print("Gráficas guardadas: grafica_departamentos.png, grafica_casos_mes.png, grafica_edad.png")
spark.stop()
