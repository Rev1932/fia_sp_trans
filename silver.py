from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, to_timestamp, date_format,
    concat_ws, row_number
)
from pyspark.sql.window import Window
from s3_utils import get_spark_session


silver_topics = {
    "sptrans.previsao": {
        "bronze_path": "s3a://bronze/previsao",
        "id_col": "hr"
    },
    "sptrans.linhas": {
        "bronze_path": "s3a://bronze/linhas",
        "id_col": "cl"
    },
    "sptrans.posicao": {
        "bronze_path": "s3a://bronze/posicao",
        "id_col": "cl"
    },
}

silver_paths = {
    "sptrans.previsao": "s3a://silver/previsao",
    "sptrans.linhas": "s3a://silver/linhas",
    "sptrans.posicao": "s3a://silver/posicao"
}


def run_silver_layer():
    spark = get_spark_session()

    for topic, config in silver_topics.items():
        bronze_path = config["bronze_path"]
        id_col = config["id_col"]

        print(f"Processando tópico: {topic}")
        df = spark.read.parquet(bronze_path)

        df = df.withColumn("ts", to_timestamp(col("ingestion_timestamp_utc")))
        df = df.withColumn("data", date_format(col("ts"), "yyyy-MM-dd"))
        df = df.withColumn("hora", date_format(col("ts"), "HH"))

        if topic == "sptrans.posicao":
            df = (
                df.withColumn("linha", explode(col("l")))
                  .select(
                      col("linha.cl").alias("cl"),
                      col("linha.c").alias("codigo_linha"),
                      col("linha.lt0").alias("terminal_origem"),
                      col("linha.lt1").alias("terminal_destino"),
                      col("linha.sl").alias("sentido_linha"),
                      col("linha.qv").alias("quantidade_veiculos"),
                      col("linha.vs").alias("veiculos"),
                      col("ingestion_timestamp_utc"),
                      col("data"),
                      col("hora")
                  )
            )

        df = df.withColumn("new_id", concat_ws("-", col(id_col), col("ingestion_timestamp_utc")))

        window = Window.partitionBy("new_id").orderBy(col("ingestion_timestamp_utc").desc())
        df_clean = (
            df.withColumn("row_num", row_number().over(window))
              .filter(col("row_num") == 1)
              .drop("row_num")
        )

        silver_path = silver_paths[topic]
        try:
            df_existing = spark.read.parquet(silver_path)
            df_to_write = df_clean.unionByName(df_existing).dropDuplicates(["new_id"])
        except Exception:
            df_to_write = df_clean

        df_to_write.write.mode("overwrite").partitionBy("data", "hora").parquet(silver_path)
        print(f"Silver layer atualizada para {topic} em: {silver_path}")

    spark.stop()


if __name__ == "__main__":
    run_silver_layer()
