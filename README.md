# ST0263 Tópicos Especiales en Telemática

# Estudiante(s): 
- Pablo Moreno Quintero - pmorenoq@eafit.edu.co
- Juan Sebastían Camacho Palacio - jscamachop@eafit.edu.co  
- Samuel Salazar Salazar - ssalazar1@eafit.edu.co

# Profesor: Edwin Montoya - emontoya@eafit.edu.co

## Link video sustentación:

# Proyecto 3 - Big Data

# 1. breve descripción de la actividad
Implementar la aqruitectura Batch para Big Data, mediante procesos de automatización para el proceso de captura, procesamiento y salida de datos, con el fin de realizar una gestión de los datos de Covid en Colombia.

## 1.1. Que aspectos cumplió o desarrolló de la actividad propuesta por el profesor (requerimientos funcionales y no funcionales)
- Creación de clúster EMR
- Ingesta de datos desde una [API](https://www.datos.gov.co/resource/gt2j-8ykr.json) del gobierno
- Procesamiento de datos mediante PySpark y SparkSQL
- Salida de datos mediante SparkSQL
- Automatización de procesos mediante "Stpes"

## 1.2. Que aspectos NO cumplió o desarrolló de la actividad propuesta por el profesor (requerimientos funcionales y no funcionales)
- Algunos procesos si requieren de la intervención humana para dar inicio.
- A nivel de ingesta de datos no se hace desde una base de datos relacional hacia S3
- A nivel de capa de aplicación no se ejecuta el programa correctamente y se limita su uso únicamente a descargar contenido.

# 2. información general de diseño de alto nivel, arquitectura, patrones, mejores prácticas utilizadas.
![image](https://github.com/user-attachments/assets/1e17c673-43ec-42b6-ac09-2588a0e5cf0d)

# 3. Descripción del ambiente de desarrollo y técnico: lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.
- Librerias utilizadas
  - json / Python
  - requests 2.20 / Python
  - boto3 1.16.25 / Python
  - pyspark.slq / Pyspark
 
 - Lenguajes de programación
   - Python 3.6
   - pyspark 3.0.1
   

*Ver guía adjunta en el repositorio*

# 4. Descripción del ambiente de EJECUCIÓN.

Para llevar los datos desde la API hasta S3
``` Python
import requests
import boto3

# Descargar archivo desde la API y guardarlo en S3
def download_covid_data_to_s3():
    url = 'https://www.datos.gov.co/resource/gt2j-8ykr.json'
    response = requests.get(url)
    s3 = boto3.client('s3')
    s3.put_object(Bucket='bucket-rawdata-p3-telematica', Key='covid_data.json', Body=response.content)

download_covid_data_to_s3()
```

ETL para mejorar la calidad de los datos
``` pyspark
from pyspark.sql import SparkSession

# Crear una SparkSession
spark = SparkSession.builder.appName("CovidDataETL").getOrCreate()

# Leer datos desde S3 utilizando el esquema inferido automáticamente
data_raw = spark.read.option("multiline", "true").json("s3a://bucket-rawdata-p3-telematica/covid_data.json")

# Mostrar los primeros registros para confirmar la estructura inferida
data_raw.show(10)

# Eliminar filas que estén completamente vacías
data_cleaned = data_raw.na.drop(how='all')

# Aplicar otros filtros necesarios, por ejemplo, queremos casos confirmados si la columna 'estado' existe
if 'estado' in data_cleaned.columns:
    data_filtered = data_cleaned.filter(data_cleaned['estado'].isin(['Leve', 'Moderado', 'Grave']))
else:
    data_filtered = data_cleaned

# Cachear los datos limpios antes de escribir
data_filtered.cache()

# Guardar los datos procesados en S3 zona Trusted
data_filtered.write.mode('overwrite').parquet("s3a://bucket-trusteddata-p3-telematica/cleaned_covid_data")

# Leer los datos procesados desde el bucket Trusted
data_trusted = spark.read.parquet("s3a://bucket-trusteddata-p3-telematica/cleaned_covid_data")

# Guardar los datos procesados en S3 zona Refined en formato CSV
data_trusted.write.mode('overwrite').option("header", "true").csv("s3a://bucket-refineddata-p3-telematica/refined_covid_data_csv")

# Leer los datos procesados desde el bucket Refined para confirmar
data_refined = spark.read.option("header", "true").csv("s3a://bucket-refineddata-p3-telematica/refined_covid_data_csv")

# Mostrar una muestra de los datos procesados para confirmar que están bien
data_refined.show(10)

# Contar la cantidad de registros en cada etapa
print(f"Cantidad de registros sin filtrar: {data_raw.count()}")
print(f"Cantidad de registros después de eliminar filas completamente nulas: {data_cleaned.count()}")
print(f"Cantidad de registros en el CSV refinado: {data_refined.count()}")
```

Para la realización de consultas
``` Hive
-- Crear la tabla en Hive
CREATE EXTERNAL TABLE IF NOT EXISTS covid_data_refined (
    ciudad_municipio STRING,
    ciudad_municipio_nom STRING,
    departamento STRING,
    departamento_nom STRING,
    edad INT,
    estado STRING,
    fecha_de_notificaci_n STRING,
    fecha_diagnostico STRING,
    fecha_inicio_sintomas STRING,
    fecha_muerte STRING,
    fecha_recuperado STRING,
    fecha_reporte_web STRING,
    fuente_tipo_contagio STRING,
    id_de_caso STRING,
    nom_grupo_ STRING,
    per_etn_ STRING,
    recuperado STRING,
    sexo STRING,
    tipo_recuperacion STRING,
    ubicacion STRING,
    unidad_medida INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://bucket-refineddata-p3-telematica/refined_covid_data_csv/';

-- Consulta 1: Conteo de casos por departamento
SELECT departamento_nom, COUNT(*) AS total_casos
FROM covid_data_refined
GROUP BY departamento_nom
ORDER BY total_casos DESC;

-- Consulta 2: Número de casos activos, recuperados y fallecidos
SELECT estado, COUNT(*) AS total_casos
FROM covid_data_refined
GROUP BY estado;

-- Consulta 3: Número de casos por género
SELECT sexo, COUNT(*) AS total_casos
FROM covid_data_refined
GROUP BY sexo;

-- Consulta 4: Promedio de edad de los casos fallecidos
SELECT AVG(edad) AS promedio_edad
FROM covid_data_refined
WHERE estado = 'Fallecido';

-- Consulta 5: Número de casos por tipo de contagio
SELECT fuente_tipo_contagio, COUNT(*) AS total_casos
FROM covid_data_refined
GROUP BY fuente_tipo_contagio
ORDER BY total_casos DESC;

-- Consulta 6: Conteo de casos por rango de edad
SELECT 
  CASE 
    WHEN edad < 20 THEN 'Menor de 20'
    WHEN edad BETWEEN 20 AND 39 THEN '20-39'
    WHEN edad BETWEEN 40 AND 59 THEN '40-59'
    ELSE 'Mayor de 60'
  END AS rango_edad,
  COUNT(*) AS total_casos
FROM covid_data_refined
GROUP BY 
  CASE 
    WHEN edad < 20 THEN 'Menor de 20'
    WHEN edad BETWEEN 20 AND 39 THEN '20-39'
    WHEN edad BETWEEN 40 AND 59 THEN '40-59'
    ELSE 'Mayor de 60'
  END
ORDER BY total_casos DESC;

-- Consulta 7: Tiempo promedio de recuperación (en días)
SELECT AVG(DATEDIFF(TO_DATE(fecha_recuperado), TO_DATE(fecha_diagnostico))) AS tiempo_promedio_recuperacion
FROM covid_data_refined
WHERE estado = 'Recuperado' AND fecha_recuperado IS NOT NULL;

-- Consulta 8: Número de casos en el último mes
SELECT COUNT(*) AS total_casos_ultimo_mes
FROM covid_data_refined
WHERE TO_DATE(fecha_diagnostico) >= ADD_MONTHS(CURRENT_DATE(), -1);

-- Consulta 9: Distribución de casos por ubicación del paciente
SELECT ubicacion, COUNT(*) AS total_casos
FROM covid_data_refined
GROUP BY ubicacion
ORDER BY total_casos DESC;

-- Consulta 10: Comparación de casos leves vs graves por departamento
SELECT departamento_nom,
       COUNT(CASE WHEN estado = 'Leve' THEN 1 END) AS casos_leves,
       COUNT(CASE WHEN estado = 'Grave' OR estado = 'Fallecido' THEN 1 END) AS casos_graves
FROM covid_data_refined
GROUP BY departamento_nom
ORDER BY casos_graves DESC;

-- Consulta 11: Casos por fecha de notificación (tendencia temporal)
SELECT fecha_de_notificaci_n, COUNT(*) AS total_casos
FROM covid_data_refined
GROUP BY fecha_de_notificaci_n
ORDER BY TO_DATE(fecha_de_notificaci_n);

```




# referencias:

