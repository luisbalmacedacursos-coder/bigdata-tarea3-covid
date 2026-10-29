# Procesamiento batch COVID-19 Colombia con Apache Spark

Análisis en batch con PySpark de los casos positivos de COVID-19 en Colombia (~6.4 millones de registros).

## Dataset
Casos positivos de COVID-19 en Colombia, datos.gov.co (ID gt2j-8ykr).
Descarga:
wget -O covid_colombia.csv "https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD"

## Qué hace el script
1. Carga el CSV y normaliza los nombres de columnas.
2. Limpieza: elimina duplicados por ID de caso, filtra edades en años (0-110) y nulos.
3. EDA: casos por departamento, sexo, estado y estadísticas de edad.
4. Visualización: genera 3 gráficas PNG.

## Ejecución
pip install matplotlib pandas
spark-submit --driver-memory 2g batch_covid.py
