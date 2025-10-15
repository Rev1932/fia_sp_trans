from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import StringType
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import config
from .s3_utils import get_spark_session


def read_from_kafka(spark, topic_name):
    """
    Lê um batch de dados de um tópico Kafka, decodificando mensagens Avro
    """
    print(f"Lendo do tópico Kafka: {topic_name}")

    schema_registry_client = SchemaRegistryClient({'url': config.SCHEMA_REGISTRY_URL})
    schema_metadata = schema_registry_client.get_latest_version(f"{topic_name}-value")
    avro_schema = schema_metadata.schema.schema_str

    avro_deserializer = AvroDeserializer(schema_registry_client, avro_schema)

    jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.KAFKA_USERNAME}" password="{config.KAFKA_PASSWORD}";'

    df_kafka = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", config.KAFKA_PROTOCOL)
        .option("kafka.sasl.mechanism", config.KAFKA_SASL_MECHANISM)
        .option("kafka.sasl.jaas.config", jaas_config)
        .load()
    )

    def decode_avro(value):
        try:
            return str(avro_deserializer(value, None))
        except Exception as e:
            return f"Erro ao decodificar: {str(e)}"

    decode_avro_udf = udf(decode_avro, StringType())

    df_decoded = df_kafka.withColumn("json_value", decode_avro_udf(col("value")))

    df_final = spark.read.json(df_decoded.rdd.map(lambda r: r.json_value))

    return df_final


def run_bronze_layer():
    """
    Executa o processo da camada Bronze: lê do Kafka e salva no MinIO.
    """
    spark = get_spark_session()
    df_raw = read_from_kafka(spark, 'Teste_sabino')

    if df_raw.count() > 0:
        df_final = df_raw.withColumn("ingestion_timestamp_utc", current_timestamp())
        print(f"Dados lidos. Escrevendo na camada Bronze em: {config.BRONZE_PATH}")

        (
            df_final.write
            .format("parquet")
            .mode("append")
            .save(config.BRONZE_PATH)
        )
        print("Camada Bronze concluída.")
    else:
        print("Nenhum dado novo no tópico do Kafka.")

    spark.stop()


if __name__ == "__main__":
    run_bronze_layer()
