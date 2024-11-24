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
