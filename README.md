# SaaS Analytics ETL Engine (PySpark)

Un pipeline de ingeniería de datos construido con **Apache Spark (PySpark)** diseñado para procesar eventos telemétricos anidados en JSON, aplicar reglas de calidad de datos y generar un Data Lake optimizado.

## Stack Tecnológico
* **Lenguaje:** Python 3.12
* **Procesamiento Distribuido:** PySpark 4.2
* **Almacenamiento:** Formato Parquet columnar

## Arquitectura del Pipeline
1. **Data Ingestion:** Lectura de logs JSON masivos aplicando un `StructType` estricto para evitar penalizaciones de rendimiento por inferencia.
2. **Flattening & Transformation:** Aplanamiento de estructuras jerárquicas usando navegación por puntos (`col("padre.hijo")`) y expansión de arrays con `explode_outer()`.
3. **Data Quality:** Deduplicación por `event_id` y filtrado matemático de nulos críticos.
4. **Data Lake Storage:** Escritura en formato Parquet aplicando **Partition Pruning** (particionado físico por `fecha` y `suscripcion`) para reducir el coste de escaneo en futuras consultas analíticas.

## Ejecución Local
\`\`\`bash
# 1. Generar los datos crudos de prueba
python 01_generar_logs.py

# 2. Ejecutar el motor de procesamiento distribuido
python 02_etl_engine.py
\`\`\`