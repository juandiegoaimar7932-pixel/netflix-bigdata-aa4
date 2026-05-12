"""
Archivo: 04_streaming_consumer_spark.py

Objetivo:
Consumir eventos de Kafka usando Spark Structured Streaming
y mostrar eventos Netflix en tiempo real.
"""

from pyspark.sql import SparkSession


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("NetflixStreamingConsumer")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():

    print("=" * 70)
    print("Netflix Streaming Consumer")
    print("=" * 70)

    spark = create_spark_session()

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:19092")
        .option("subscribe", "netflix-events")
        .option("startingOffsets", "latest")
        .load()
    )

    events_df = df.selectExpr("CAST(value AS STRING) AS event")

    query = (
        events_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()