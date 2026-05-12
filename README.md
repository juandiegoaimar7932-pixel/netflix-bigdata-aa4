# Netflix: Plataforma Big Data Para Análisis De Consumo Y Streaming En Tiempo Real

## 1. Descripción del caso

Netflix Big Data Solution es una plataforma diseñada para procesar y analizar el comportamiento de visualización de millones de suscriptores. 
La empresa necesita analizar datos históricos (15,000+ registros) y eventos en tiempo real para detectar tendencias de consumo, popularidad de géneros y actividad inusual en la plataforma.

## 2. Objetivo general

Construir una solución Big Data usando Apache Spark, Python, Kafka y MongoDB para procesar datos batch y streaming, generar indicadores estratégicos de visualización y asegurar la persistencia de resultados en una base de datos NoSQL.

## 3. Tecnologías usadas

- Docker
- Python
- Apache Spark / PySpark
- Spark SQL
- RDD
- DataFrames
- Kafka
- MongoDB
- CSV
- JSON
- Parquet
- Matplotlib

## 4. Resultados esperados

- Top películas más vistas
- Géneros con mayor demanda
- Usuarios más activos (Heavy Users)
- Distribución de planes de suscripción
- Tiempo total visualizado por usuario
- Alertas de actividad en tiempo real
- Reportes CSV
- Archivos Parquet
- Gráficos PNG

## 5. Estructura del proyecto

```text
netflix-bigdata-streaming/
├── data/
│   ├── raw/
│   ├── processed/
│   └── checkpoints/
├── docs/
├── notebooks/
├── output/
│   ├── visualizations/
│   ├── kpis/
│   └── streaming/
├── src/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md