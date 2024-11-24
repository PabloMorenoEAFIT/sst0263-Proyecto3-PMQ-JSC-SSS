# ST0263 Tópicos Especiales en Telemática

# Estudiante(s): 
- Pablo Moreno Quintero - pmorenoq@eafit.edu.co
- Juan Sebastían Camacho Palacio - jscamachop@eafit.edu.co  
- Samuel Salazar Salazar - 

# Profesor: Edwin Montoya - emontoya@eafit.edu.co


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

```




# referencias:

