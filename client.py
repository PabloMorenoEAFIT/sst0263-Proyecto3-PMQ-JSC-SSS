import boto3
import pandas as pd

# Configuración del cliente S3
s3 = boto3.client('s3')

# Información del bucket y archivo
bucket_name = "bucket-refineddata-p3-telematica"
object_key = "/refined_covid_data_csv"  # Cambia el nombre del archivo según sea necesario
local_file = "refined_covid_data.csv"

# Descargar el archivo desde S3
s3.download_file(bucket_name, object_key, local_file)
print(f"Archivo descargado: {local_file}")

# Leer y procesar el archivo CSV
df = pd.read_csv(local_file)

# Mostrar las primeras filas
print(df.head())

# Generar un resumen estadístico
summary = df.describe()

# Guardar el resumen en un archivo
summary.to_csv("summary_covid_data.csv", index=False)
print("Resumen guardado en 'summary_covid_data.csv'")
