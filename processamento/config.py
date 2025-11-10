import os
from dotenv import load_dotenv


load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_USERNAME = os.environ.get('KAFKA_USERNAME')
KAFKA_PASSWORD = os.environ.get('KAFKA_PASSWORD')
KAFKA_PROTOCOL = os.environ.get('KAFKA_PROTOCOL', 'SASL_SSL')
KAFKA_SASL_MECHANISM = os.environ.get('KAFKA_SASL_MECHANISM', 'PLAIN')
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL')

CHAVE_PK = os.environ.get('CHAVE_PK', 'id')
CONFIG_NAME = os.environ.get('CONFIG_NAME', 'meu_config')
TABLE_NAME = os.environ.get('TABLE_NAME', 'minha_tabela')
TOPIC_NAME = f"{CONFIG_NAME}_{TABLE_NAME}"

MINIO_ENDPOINT_URL = os.environ.get('MINIO_ENDPOINT_URL')
MINIO_USER = os.environ.get('MINIO_USER')
MINIO_PASSWORD = os.environ.get('MINIO_PASSWORD')
MINIO_TESTE = os.environ.get('MINIO_TESTE', 'datalake')


BASE_S3_PATH = f"s3a://{MINIO_TESTE}"
BRONZE_PATH = f"{BASE_S3_PATH}/bronze/{TABLE_NAME}"
SILVER_PATH = f"{BASE_S3_PATH}/silver/{TABLE_NAME}"
GOLD_PATH = f"{BASE_S3_PATH}/gold/{TABLE_NAME}_agg"