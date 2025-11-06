import os
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "pyspark-shell"
)

from pyspark.sql.functions import col, current_timestamp, from_json, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from s3_utils import get_spark_session

# Define os schemas para cada tópico
schemas = {
    "veiculos": StructType([
        StructField("prefixo_veiculo", StringType(), True),
        StructField("horario_ult_posicao_api", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("acessivel_deficiente", StringType(), True),
        StructField("letreiro_linha", StringType(), True),
        StructField("codigo_linha", StringType(), True),
        StructField("horario_captura_api", StringType(), True),
        StructField("horario_processamento", StringType(), True)
    ]),
    "linhas": StructType([
        StructField("codigo_linha", StringType(), True),
        StructField("nome_linha", StringType(), True),
        StructField("tipo_linha", StringType(), True)
    ]),
    "motoristas": StructType([
        StructField("id_motorista", StringType(), True),
        StructField("nome_motorista", StringType(), True),
        StructField("cpf", StringType(), True)
    ])
}

# Mapeia cada tópico para seu caminho S3
s3_paths = {
    "veiculos": "s3a://bronze/veiculos",
    "linhas": "s3a://bronze/linhas",
    "motoristas": "s3a://bronze/motoristas"
}

def read_from_kafka(spark, topic_name, schema):
    print(f"Lendo do tópico Kafka: {topic_name}")

    df_kafka = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_str = df_kafka.selectExpr("CAST(value AS STRING) AS json_value")

    df_final = df_str.withColumn("json_data", from_json(col("json_value"), schema)) \
                     .select("json_data.*")

    return df_final

def run_bronze_layer():
    spark = get_spark_session()

    for topic, schema in schemas.items():
        df_raw = read_from_kafka(spark, topic, schema)

        if df_raw.count() > 0:
            df_final = df_raw.withColumn("ingestion_timestamp_utc", current_timestamp())
            df_final = df_final.withColumn("data", date_format(col("ingestion_timestamp_utc"), "yyyy-MM-dd")) \
                               .withColumn("hora", date_format(col("ingestion_timestamp_utc"), "HH"))

            print(f"Dados do tópico {topic}:")
            df_final.show(truncate=False)

            df_final.write.mode("append").partitionBy("data", "hora").parquet(s3_paths[topic])
            print(f"Dados salvos no MinIO em: {s3_paths[topic]} particionados por data/hora")
        else:
            print(f"Nenhum dado no tópico {topic}.")

    spark.stop()

if __name__ == "__main__":
    run_bronze_layer()
