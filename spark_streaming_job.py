import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, trim, expr

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SparkStreamingJob")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "consumo_streaming")
OUTPUT_PATH             = os.getenv("OUTPUT_PATH", "/data/parquet_output")
CHECKPOINT_PATH         = os.getenv("CHECKPOINT_PATH", "/data/checkpoints/consumo_streaming")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ConsumoElectricoStreaming")
        .getOrCreate()
    )


def build_streaming_query(spark):
    # Leer mensajes del topic Kafka
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_lines = df_raw.selectExpr("CAST(value AS STRING) as linea")

    # Estructura del CSV (55 columnas):
    #   [0]      cups
    #   [1]      periodo (YYYYMM)
    #   [2]      tarifa
    #   [3]      provincia
    #   [4]      municipio
    #   [5..28]  h1..h24  — consumo activo por hora (Wh)
    #   [29]     separador
    #   [30..53] r1..r24  — consumo reactivo por hora (VARh)
    #   [54]     separador
    df_split = df_lines.select(split(col("linea"), ",").alias("c"))

    cols_activo   = [trim(col("c")[5  + i]).cast("double").alias(f"h{i+1}") for i in range(24)]
    cols_reactivo = [trim(col("c")[30 + i]).cast("double").alias(f"r{i+1}") for i in range(24)]

    df_parsed = df_split.select(
        trim(col("c")[0]).alias("cups"),
        trim(col("c")[1]).alias("periodo"),
        trim(col("c")[2]).alias("tarifa"),
        trim(col("c")[3]).alias("provincia"),
        trim(col("c")[4]).alias("municipio"),
        *cols_activo,
        *cols_reactivo,
    )

    suma_activo   = " + ".join([f"h{i+1}" for i in range(24)])
    suma_reactivo = " + ".join([f"r{i+1}" for i in range(24)])

    df_final = (
        df_parsed
        .withColumn("consumo_activo_total_wh",    expr(suma_activo))
        .withColumn("consumo_reactivo_total_varh", expr(suma_reactivo))
        .withColumn("anyo", col("periodo").substr(1, 4))
        .withColumn("mes",  col("periodo").substr(5, 2))
    )

    # Escribir en Parquet particionado por año y mes
    query = (
        df_final.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("anyo", "mes")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


if __name__ == "__main__":
    logger.info("Iniciando Spark Structured Streaming Job...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} -> topic '{KAFKA_TOPIC}'")
    logger.info(f"Salida Parquet: {OUTPUT_PATH}")

    query = build_streaming_query(spark)

    logger.info("Job activo. Procesando cada 10s... (Ctrl+C para parar)")
    query.awaitTermination()
