from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, row_number
from pyspark.sql.window import Window
from s3_utils import get_spark_session

# Define os tópicos da Silver e suas chaves primárias
silver_topics = {
    "veiculos": {"bronze_path": "s3a://bronze/veiculos", "id_col": "prefixo_veiculo"},
    "linhas": {"bronze_path": "s3a://bronze/linhas", "id_col": "codigo_linha"},
    "motoristas": {"bronze_path": "s3a://bronze/motoristas", "id_col": "id_motorista"}
}

silver_paths = {
    "veiculos": "s3a://silver/veiculos",
    "linhas": "s3a://silver/linhas",
    "motoristas": "s3a://silver/motoristas"
}

def run_silver_layer():
    spark = get_spark_session()

    for topic, config in silver_topics.items():
        bronze_path = config["bronze_path"]
        id_col = config["id_col"]

        # Lê a Bronze
        df = spark.read.parquet(bronze_path)

        # Cria new_id: id + ingestion_timestamp_utc
        df = df.withColumn("new_id", concat_ws("-", col(id_col), col("ingestion_timestamp_utc")))

        # Remove duplicados usando window function (mantendo o mais recente)
        window = Window.partitionBy("new_id").orderBy(col("ingestion_timestamp_utc").desc())
        df_clean = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")

        # Merge / upsert: no parquet não existe merge nativo, então simulamos
        silver_path = silver_paths[topic]
        try:
            df_existing = spark.read.parquet(silver_path)
            df_to_write = df_clean.unionByName(df_existing).dropDuplicates(["new_id"])
        except Exception:
            # Se não existe, apenas grava
            df_to_write = df_clean

        # Salva no MinIO particionando por data/hora
        df_to_write.write.mode("overwrite").partitionBy("data", "hora").parquet(silver_path)
        print(f"Silver layer atualizada para {topic} em: {silver_path}")

    spark.stop()


if __name__ == "__main__":
    run_silver_layer()
