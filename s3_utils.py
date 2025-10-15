import duckdb
from pyspark.sql import SparkSession
from config import MINIO_ENDPOINT_URL, MINIO_USER, MINIO_PASSWORD


def get_spark_session():
    """Cria e configura uma sessão Spark para se conectar ao MinIO."""
    try:
        builder = (
            SparkSession.builder.master("local[*]")
            .appName("MedallionPipeline")
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

        spark = builder.getOrCreate()
        return spark
    except Exception as e:
        print(f'Erro ao iniciar secao spark: {e}')
        return False


def get_duckdb_connection():
    try:
        conn = duckdb.connect(database=':memory:', read_only=False)

        conn.execute("INSTALL httpfs; LOAD httpfs;")

        endpoint_without_protocol = MINIO_ENDPOINT_URL.split("//")[1]
        conn.execute(f"SET s3_endpoint = '{endpoint_without_protocol}';")
        conn.execute("SET s3_url_style = 'path';")
        conn.execute("SET s3_use_ssl = false;")
        conn.execute(f"SET s3_access_key_id = '{MINIO_USER}';")
        conn.execute(f"SET s3_secret_access_key = '{MINIO_PASSWORD}';")

        print("Conexão DuckDB com MinIO configurada.")
        return conn
    except Exception as e:
        print(f'Erro ao conectar com o duckdb: {e}')
        return False


if __name__ == '__main__':
    get_spark_session()
    get_duckdb_connection()