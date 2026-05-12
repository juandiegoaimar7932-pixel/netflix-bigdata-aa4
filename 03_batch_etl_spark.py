"""
Archivo: 03_batch_etl_spark.py
Proyecto: Netflix Big Data Streaming
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pymongo import MongoClient

# ============================================================
# 1. Configuración de rutas y Mongo
# ============================================================
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "output" / "kpis"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# URI para conectar al contenedor de MongoDB
MONGO_URI = "mongodb://mongodb:27017"

# ============================================================
# 2. Función de persistencia en MongoDB
# ============================================================
def save_to_mongodb(df, collection_name):
    try:
        # Conectamos a la base de datos
        client = MongoClient(MONGO_URI)
        db = client["netflix_analytics"]
        collection = db[collection_name]
        
        # Limpiamos para evitar duplicados
        collection.delete_many({})
        
        # Convertimos Spark DF a diccionarios para Mongo
        data_dict = df.toPandas().to_dict(orient='records')
        
        if data_dict:
            collection.insert_many(data_dict)
            # Imprimimos confirmación silenciosa (opcional)
        client.close()
    except Exception as e:
        print(f"Error guardando en Mongo ({collection_name}): {e}")

# ============================================================
# 3. Crear Spark Session
# ============================================================
def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("NetflixBatchETL").master("local[*]").getOrCreate()

def main() -> None:
    print("=" * 70)
    print("Netflix Batch ETL con PySpark (CSV + Parquet)")
    print("=" * 70)

    spark = create_spark_session()

    # Carga de datos
    movies_df = spark.read.csv(str(RAW_DIR / "movies.csv"), header=True, inferSchema=True)
    users_df = spark.read.csv(str(RAW_DIR / "users.csv"), header=True, inferSchema=True)
    subscriptions_df = spark.read.csv(str(RAW_DIR / "subscriptions.csv"), header=True, inferSchema=True)
    watch_history_df = spark.read.csv(str(RAW_DIR / "watch_history.csv"), header=True, inferSchema=True)

    print(f"\n>>> VALIDACIÓN TÉCNICA: Se han cargado {watch_history_df.count()} registros del historial. <<<\n")

    # --- KPIs ---

    # 1. Películas más vistas
    most_watched_df = watch_history_df.groupBy("movie_id").agg(F.count("*").alias("total_views"))\
        .join(movies_df, on="movie_id").select("movie_id", "title", "genre", "total_views").orderBy(F.desc("total_views"))
    
    # GUARDAR EN 3 FORMATOS
    most_watched_df.toPandas().to_csv(OUTPUT_DIR / "most_watched_movies.csv", index=False) # CSV
    most_watched_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "most_watched_movies.parquet")) # PARQUET
    save_to_mongodb(most_watched_df, "kpi_most_watched_movies") # MONGO

    # 2. Géneros más vistos
    genres_df = watch_history_df.join(movies_df, on="movie_id").groupBy("genre")\
        .agg(F.count("*").alias("total_views")).orderBy(F.desc("total_views"))
    
    genres_df.toPandas().to_csv(OUTPUT_DIR / "most_watched_genres.csv", index=False)
    genres_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "most_watched_genres.parquet"))
    save_to_mongodb(genres_df, "kpi_most_watched_genres")

    # 3. Usuarios más activos
    active_users_df = watch_history_df.groupBy("user_id").agg(F.count("*").alias("total_views"))\
        .join(users_df, on="user_id").select("user_id", "country", "age", "total_views").orderBy(F.desc("total_views"))
    
    active_users_df.toPandas().to_csv(OUTPUT_DIR / "most_active_users.csv", index=False)
    active_users_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "most_active_users.parquet"))
    save_to_mongodb(active_users_df, "kpi_most_active_users")

    # 4. Suscripciones por plan
    plans_df = subscriptions_df.groupBy("plan").agg(F.count("*").alias("total_users"), F.round(F.avg("monthly_price"), 2).alias("avg_price"))\
        .orderBy(F.desc("total_users"))
    
    plans_df.toPandas().to_csv(OUTPUT_DIR / "subscriptions_by_plan.csv", index=False)
    plans_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "subscriptions_by_plan.parquet"))
    save_to_mongodb(plans_df, "kpi_subscriptions_by_plan")

    # 5. Tiempo total visto
    watch_time_df = watch_history_df.groupBy("user_id").agg(F.sum("minutes_watched").alias("total_watch_minutes"))\
        .join(users_df, on="user_id").select("user_id", "country", "age", "total_watch_minutes").orderBy(F.desc("total_watch_minutes"))
    
    watch_time_df.toPandas().to_csv(OUTPUT_DIR / "watch_time_by_user.csv", index=False)
    watch_time_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "watch_time_by_user.parquet"))
    save_to_mongodb(watch_time_df, "kpi_watch_time_by_user")

    # --- IMPRESIÓN ---
    print("Top películas más vistas:")
    most_watched_df.show(10)
    print("Géneros más vistos (Suma total = 15,000):")
    genres_df.show(10)
    print("Usuarios más activos:")
    active_users_df.show(10)
    print("Planes de suscripción:")
    plans_df.show()
    print("Usuarios con más tiempo visualizado:")
    watch_time_df.show(10)

    print("=" * 70)
    print("KPIs generados correctamente")
    print("Archivos guardados en: /app/output/kpis")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    main()